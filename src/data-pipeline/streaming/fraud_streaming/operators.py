import json
import logging
import time

from confluent_kafka import IsolationLevel, KafkaError, KafkaException, TopicPartition
from confluent_kafka.admin import AdminClient, OffsetSpec
from pyflink.common import Row, Types
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import KeyedProcessFunction
from pyflink.datastream.state import ValueStateDescriptor

from fraud_streaming.model import DEDUP_RETENTION, Entity, Packet, Progress, entity_key
from fraud_streaming.wire import decode, snapshot

log = logging.getLogger(__name__)


def micros():
    return time.time_ns() // 1000


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

    def process_element(self, row, ctx):
        raw, topic, partition, offset = row
        fact = None
        if topic == self.clock_topic:
            if not self.logical:
                raise ValueError("clock controls are forbidden in live mode")
            observed = int(json.loads(bytes(raw))["clock_us"])
        elif topic == self.topic:
            fact, recorded_at = decode(raw)
            if self.logical:
                observed = recorded_at
            else:
                observed = micros()
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
        yield Packet(fact, now, state["bootstrap"], progress)

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
    def __init__(self, logical):
        self.logical = logical

    def open(self, runtime_context):
        self.state = runtime_context.get_state(
            ValueStateDescriptor("event-identity-v3", Types.STRING())
        )
        self.too_late = runtime_context.get_metrics_group().counter("too_late")

    def process_element(self, packet, ctx):
        fact = packet.fact
        if packet.too_late:
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
        prior = self.state.value()
        if prior is not None:
            if prior != fact.digest:
                raise ValueError("conflicting event identity/content")
            return
        self.state.update(fact.digest)
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
