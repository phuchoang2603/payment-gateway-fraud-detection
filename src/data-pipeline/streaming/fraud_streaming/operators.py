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
    Entity,
    Packet,
    Progress,
    Rejected,
    Source,
    entity_key,
)
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
    return time.time_ns() // 1000


def epoch_millis(timestamp):
    # PyFlink surfaces Kafka TIMESTAMP_LTZ metadata as Instant or as a local datetime.
    if hasattr(timestamp, "to_epoch_milli"):
        return timestamp.to_epoch_milli()
    return int(timestamp.timestamp() * 1000)


def reject_counters(runtime_context):
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
    return (timestamp + 999) // 1000


def register(ctx, timestamp, logical):
    service = ctx.timer_service()
    if logical:
        service.register_event_time_timer(timer_at(timestamp))
    else:
        service.register_processing_time_timer(timer_at(timestamp))


def delete_timer(ctx, timestamp, logical):
    service = ctx.timer_service()
    if logical:
        service.delete_event_time_timer(timer_at(timestamp))
    else:
        service.delete_processing_time_timer(timer_at(timestamp))


class Clock(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp):
        return value.now // 1000


class ObserveSource(KeyedProcessFunction):
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

    def process_element(self, row, ctx):
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

    def __init__(self, logical, scope):
        self.logical = logical
        self.scope = scope

    def open(self, runtime_context):
        self.state = runtime_context.get_state(
            ValueStateDescriptor(f"{self.scope}-identity-v3", Types.STRING())
        )
        self.rejected = reject_counters(runtime_context)
        self.too_late = runtime_context.get_metrics_group().counter("too_late")

    def process_element(self, packet, ctx):
        fact = packet.fact
        if self.scope == "event" and packet.too_late:
            self.too_late.inc()
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
        content = fact.digest if self.scope == "event" else f"{fact.event_id}:{fact.digest}"
        prior = self.state.value()
        if prior is not None:
            if prior != content:
                error = Rejected(
                    "conflicting_duplicate", f"conflicting {self.scope} identity/content"
                )
                yield reject(self.rejected, error, packet.source, fact.event_id)
            return
        self.state.update(content)
        register(ctx, fact.started_at + DEDUP_RETENTION, self.logical)
        yield packet

    def on_timer(self, timestamp, ctx):
        self.state.clear()
        return ()


class Aggregate(KeyedProcessFunction):
    def __init__(self, *, view, epoch, logical, max_entries):
        self.view = view
        self.epoch = epoch
        self.logical = logical
        self.max_entries = max_entries

    def open(self, runtime_context):
        self.state = runtime_context.get_state(
            ValueStateDescriptor(f"{self.view}-snapshot-v3", Types.PICKLED_BYTE_ARRAY())
        )

    def emit(self, state, now, ctx):
        previous_timer = state.next_timer
        values = state.advance(now)
        if previous_timer is not None and previous_timer != state.next_timer:
            delete_timer(ctx, previous_timer, self.logical)
        if state.next_timer is not None and state.next_timer != previous_timer:
            register(ctx, state.next_timer, self.logical)
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
        )
        state.add(packet)
        now = packet.now if self.logical else micros()
        yield from self.emit(state, now, ctx)

    def on_timer(self, timestamp, ctx):
        state = self.state.value()
        if state is not None:
            now = timestamp * 1000 if self.logical else micros()
            yield from self.emit(state, now, ctx)
