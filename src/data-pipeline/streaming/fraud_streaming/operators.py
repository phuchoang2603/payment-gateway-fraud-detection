"""Adapt deterministic feature state to Flink clocks, timers, metrics and side outputs.

Live jobs advance with processing time; logical replay follows seed-clock progress.
Invalid records are rejected individually while state-invariant failures stop the job.
"""

import json
import logging
import time

from confluent_kafka import IsolationLevel, KafkaError, KafkaException, TopicPartition
from confluent_kafka.admin import AdminClient, OffsetSpec
from pyflink.common import Row, Types
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import KeyedProcessFunction, OutputTag
from pyflink.datastream.state import ValueStateDescriptor

from fraud_streaming.model import (
    DEDUP_RETENTION,
    LATENESS_BUCKETS,
    Entity,
    Packet,
    Progress,
    Rejected,
    Source,
    dedup_content,
    dedup_status,
    entity_key,
    lateness_bucket,
)
from fraud_streaming.replay import OffsetBarrier
from fraud_streaming.wire import decode, snapshot

log = logging.getLogger(__name__)
REJECT_CLASSES = ("undecodable", "invalid_event", "invalid_context", "conflicting_duplicate")
REJECT_TYPE = Types.ROW_NAMED(
    ["key", "payload", "headers"],
    [
        Types.PRIMITIVE_ARRAY(Types.BYTE()),
        Types.PRIMITIVE_ARRAY(Types.BYTE()),
        Types.MAP(Types.STRING(), Types.PRIMITIVE_ARRAY(Types.BYTE())),
    ],
)
REJECTS = OutputTag("feature-rejects-v1", REJECT_TYPE)


def micros():
    """Read wall-clock time at the feature contract microsecond resolution."""
    return time.time_ns() // 1000


def epoch_millis(timestamp):
    # PyFlink surfaces Kafka TIMESTAMP_LTZ metadata as Instant or as a local datetime.
    """Normalize the two timestamp representations returned by PyFlink."""
    if hasattr(timestamp, "to_epoch_milli"):
        return timestamp.to_epoch_milli()
    return int(timestamp.timestamp() * 1000)


def reject_counters(runtime_context):
    """Register independent per-class quarantine counters."""
    metrics = runtime_context.get_metrics_group()
    return {
        kind: metrics.add_group("reject_class", kind).counter("rejected") for kind in REJECT_CLASSES
    }


def reject(counters, error, source, event_id=None):
    """Count, log and build the dead-letter row for one per-record data error."""
    event_id = event_id or error.event_id
    message = str(error).encode("ascii", "replace")[:200]
    counters[error.kind].inc()
    log.warning(
        json.dumps(
            {
                "event": "rejected",
                "class": error.kind,
                "event_id": event_id,
                "topic": source.topic,
                "partition": source.partition,
                "offset": source.offset,
                "message": message.decode(),
            }
        )
    )
    headers = {
        "x-reject-class": error.kind.encode(),
        "x-reject-message": message,
        "x-source-topic": source.topic.encode(),
        "x-source-partition": str(source.partition).encode(),
        "x-source-offset": str(source.offset).encode(),
        "x-source-timestamp": str(source.timestamp_ms).encode(),
    }
    key = event_id.encode() if event_id else None
    return REJECTS, Row(key=key, payload=source.raw, headers=headers)


def timer_at(timestamp):
    """Round a microsecond deadline upward to Flink millisecond resolution."""
    return (timestamp + 999) // 1000


class Timeline:
    """Where an operator reads "now" and how it schedules timers. Live follows the wall clock with processing-time
    timers; a replay follows the clock the seed wrote with event-time timers. The operators are otherwise the same."""

    def __init__(self, logical):
        self.logical = logical
        # Every entity primed in a replay's lead-in day shares its coverage boundaries, so timers there would fire
        # millions at once; the entity's next row carries the coverage instead.
        self.coverage_timers = not logical

    def now(self, packet):
        """Select the logical replay clock or the live wall clock."""
        return packet.now if self.logical else micros()

    def register(self, ctx, timestamp):
        """Register a deadline in the clock domain selected for this job."""
        service = ctx.timer_service()
        if self.logical:
            service.register_event_time_timer(timer_at(timestamp))
        else:
            service.register_processing_time_timer(timer_at(timestamp))

    def delete(self, ctx, timestamp):
        """Remove a deadline from the same clock domain used to register it."""
        service = ctx.timer_service()
        if self.logical:
            service.delete_event_time_timer(timer_at(timestamp))
        else:
            service.delete_processing_time_timer(timer_at(timestamp))

    def fire_at(self, state, timestamp):
        """The instants an entity publishes at when a timer fires: each of its due boundaries in a replay, the
        wall clock live."""
        if not self.logical:
            yield micros()
            return
        while state.next_timer is not None and state.next_timer <= timestamp * 1000:
            yield state.next_timer


class Clock(TimestampAssigner):
    """Assign replay watermark timestamps from the validated logical clock."""

    def extract_timestamp(self, value, record_timestamp):
        """Convert packet clock microseconds to Flink milliseconds."""
        return value.now // 1000


class ObserveSource(KeyedProcessFunction):
    """Attach validated facts to checkpointed partition progress and bootstrap coverage."""

    def __init__(self, *, brokers, topic, partitions, logical=False, clock_topic=None):
        self.brokers = brokers
        self.topic = topic
        self.partitions = partitions
        self.logical = logical
        self.clock_topic = clock_topic

    def open(self, runtime_context):
        self.state = runtime_context.get_state(
            ValueStateDescriptor("source-observations-v1", Types.PICKLED_BYTE_ARRAY())
        )
        self.admin = AdminClient(
            {"bootstrap.servers": self.brokers, "allow.auto.create.topics": False}
        )
        metadata = self.admin.list_topics(self.topic, timeout=10)
        topic = metadata.topics.get(self.topic)
        if topic is None or topic.error or set(topic.partitions) != set(range(self.partitions)):
            raise ValueError("source partition layout changed; explicit bootstrap required")
        self.watermarks = {}
        self.last_poll = 0
        self.rejected = reject_counters(runtime_context)
        if self.logical:
            if self.partitions != 1 or not self.clock_topic:
                raise ValueError("logical replay requires one input partition and a clock topic")
            self.barrier = runtime_context.get_state(
                ValueStateDescriptor("replay-offset-barriers-v1", Types.PICKLED_BYTE_ARRAY())
            )

    def process_element(self, row, ctx):
        if not self.logical:
            yield from self.observe(row)
            return
        barrier = self.barrier.value() or OffsetBarrier(self.topic, self.clock_topic)
        control = json.loads(bytes(row[0])) if row[1] == self.clock_topic else None
        ready = barrier.accept(row, control)
        self.barrier.update(barrier)
        for item in ready:
            yield from self.observe(item)

    def observe(self, row):
        raw, topic, partition, offset, timestamp = row
        fact = rejected = None
        source = None
        if topic == self.clock_topic:
            if not self.logical:
                raise ValueError("clock controls are forbidden in live mode")
            observed = int(json.loads(bytes(raw))["clock_us"])
        elif topic == self.topic:
            source = Source(topic, partition, offset, epoch_millis(timestamp), bytes(raw))
            try:
                fact, recorded_at = decode(raw)
            except Rejected as error:
                rejected = error
                prior = self.state.value()
                if self.logical and prior is None:
                    yield reject(self.rejected, error, source)
                    return
                observed = prior["now"] if self.logical else micros()
            else:
                observed = recorded_at if self.logical else micros()
        else:
            raise ValueError("unexpected source topic")
        state = self.state.value() or {
            "bootstrap": observed,
            "now": observed,
            "offsets": {p: -1 for p in range(self.partitions)},
        }
        if self.logical and topic == self.clock_topic and observed < state["now"]:
            raise ValueError("replay control clock regressed")
        state["now"] = max(state["now"], observed)
        if topic == self.topic:
            if offset <= state["offsets"][partition]:
                raise ValueError("source offset regressed within checkpoint state")
            state["offsets"][partition] = offset
        if (
            not self.watermarks
            or time.monotonic() - self.last_poll >= 1
            or any(
                consumed >= self.watermarks[index] for index, consumed in state["offsets"].items()
            )
        ):
            self.watermarks = self.latest_offsets(state["offsets"])
            self.last_poll = time.monotonic()
            self.progress_observed = state["now"] if self.logical else micros()
        now = state["now"] if self.logical else max(state["now"], micros())
        if fact is not None:
            if fact.recorded_at > now or fact.started_at > now:
                raise ValueError("source fact follows observed availability")
        progress = tuple(
            Progress(self.topic, index, consumed, self.watermarks[index], self.progress_observed)
            for index, consumed in sorted(state["offsets"].items())
        )
        self.state.update(state)
        if rejected is not None:
            yield reject(self.rejected, rejected, source)
            return
        yield Packet(fact, now, state["bootstrap"], progress, source)

    def latest_offsets(self, consumed):
        for attempt in range(2):
            try:
                offsets = self.admin.list_offsets(
                    {
                        TopicPartition(self.topic, index): OffsetSpec.latest()
                        for index in range(self.partitions)
                    },
                    isolation_level=IsolationLevel.READ_UNCOMMITTED,
                    request_timeout=5,
                )
                observed = {
                    partition.partition: result.result().offset
                    for partition, result in offsets.items()
                }
                if set(observed) == set(consumed) and all(
                    observed[index] >= 0 and observed[index] > offset
                    for index, offset in consumed.items()
                ):
                    return observed
                if attempt:
                    raise RuntimeError("source offset observation trails consumed progress")
            except KafkaException as error:
                if attempt or error.args[0].code() != KafkaError.FENCED_LEADER_EPOCH:
                    raise
            self.admin.list_topics(self.topic, timeout=5)


class Deduplicate(KeyedProcessFunction):
    """Drop identical redeliveries of one identity and dead-letter conflicting content."""

    def __init__(self, timeline, scope):
        self.timeline = timeline
        self.scope = scope

    def open(self, runtime_context):
        self.state = runtime_context.get_state(
            ValueStateDescriptor(f"{self.scope}-identity-v3", Types.STRING())
        )
        self.expires = runtime_context.get_state(
            ValueStateDescriptor(f"{self.scope}-identity-expiry-v1", Types.LONG())
        )
        self.rejected = reject_counters(runtime_context)
        metrics = runtime_context.get_metrics_group()
        self.too_late = metrics.counter("too_late")
        self.duplicates = metrics.add_group("scope", self.scope).counter("duplicates_dropped")
        self.lateness = {
            name: metrics.add_group("lateness", name).counter("attempts")
            for name in (*(name for _, name in LATENESS_BUCKETS), "too_late")
        }

    def process_element(self, packet, ctx):
        fact = packet.fact
        if self.scope == "event" and packet.too_late:
            self.too_late.inc()
            self.lateness["too_late"].inc()
            log.warning(
                json.dumps(
                    {
                        "event": "too_late",
                        "event_id": fact.event_id,
                        "attempted_at_us": fact.started_at,
                        "processed_at_us": packet.now,
                    }
                )
            )
            return
        if self.expires.value() is not None and packet.now >= self.expires.value():
            self.state.clear()
            self.expires.clear()
        content = dedup_content(self.scope, fact)
        status = dedup_status(self.state.value(), content)
        if status == "duplicate":
            self.duplicates.inc()
            return
        if status == "conflict":
            error = Rejected("conflicting_duplicate", f"conflicting {self.scope} identity/content")
            yield reject(self.rejected, error, packet.source, fact.event_id)
            return
        if self.scope == "event":
            self.lateness[lateness_bucket(packet.now - fact.started_at)].inc()
        self.state.update(content)
        self.expires.update(fact.started_at + DEDUP_RETENTION)
        self.timeline.register(ctx, fact.started_at + DEDUP_RETENTION)
        yield packet

    def on_timer(self, timestamp, ctx):
        expiry = self.expires.value()
        if expiry is not None and timestamp * 1000 >= expiry:
            self.state.clear()
            self.expires.clear()
        return ()


class Aggregate(KeyedProcessFunction):
    """Checkpoint one entity state and publish revisions at contribution/expiry boundaries."""

    def __init__(self, *, view, epoch, timeline, max_entries):
        self.view = view
        self.epoch = epoch
        self.timeline = timeline
        self.max_entries = max_entries

    def open(self, runtime_context):
        self.state = runtime_context.get_state(
            ValueStateDescriptor(f"{self.view}-snapshot-v3", Types.PICKLED_BYTE_ARRAY())
        )

    def emit(self, state, now, ctx):
        previous_timer = state.next_timer
        values = state.advance(now)
        if previous_timer is not None and previous_timer != state.next_timer:
            self.timeline.delete(ctx, previous_timer)
        if state.next_timer is not None and state.next_timer != previous_timer:
            self.timeline.register(ctx, state.next_timer)
        self.state.update(state)
        if values is not None:
            yield Row(
                key=entity_key(self.epoch, self.view, state.integration, state.entity).encode(),
                payload=snapshot(state, values, epoch=self.epoch, now=now),
            )

    def process_element(self, packet, ctx):
        fact = packet.fact
        entity = fact.customer if self.view == "customer" else fact.method
        state = self.state.value() or Entity(
            self.view,
            fact.integration,
            entity,
            packet.bootstrap,
            max_entries=self.max_entries,
            coverage_timers=self.timeline.coverage_timers,
        )
        while state.next_timer is not None and state.next_timer <= packet.now:
            yield from self.emit(state, state.next_timer, ctx)
        state.add(packet)
        yield from self.emit(state, self.timeline.now(packet), ctx)

    def on_timer(self, timestamp, ctx):
        state = self.state.value()
        if state is not None:
            for instant in self.timeline.fire_at(state, timestamp):
                yield from self.emit(state, instant, ctx)
