import json
import time

from confluent_kafka import IsolationLevel, TopicPartition
from confluent_kafka.admin import AdminClient, OffsetSpec
from pyflink.common import Row, Types
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import KeyedProcessFunction
from pyflink.datastream.state import ValueStateDescriptor

from fraud_streaming.model import RETENTION, Attempt, Entity, Packet, Progress, entity_key
from fraud_streaming.wire import decode, snapshot


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
            offsets = self.admin.list_offsets(
                {
                    TopicPartition(self.topic, index): OffsetSpec.latest()
                    for index in range(self.partitions)
                },
                isolation_level=IsolationLevel.READ_UNCOMMITTED,
                request_timeout=5,
            )
            self.watermarks = {
                partition.partition: result.result().offset for partition, result in offsets.items()
            }
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


class Deduplicate(KeyedProcessFunction):
    def __init__(self, logical):
        self.logical = logical

    def open(self, runtime_context):
        self.state = runtime_context.get_state(
            ValueStateDescriptor("event-identity-v2", Types.STRING())
        )

    def process_element(self, packet, ctx):
        prior = self.state.value()
        if prior is not None:
            if prior != packet.fact.digest:
                raise ValueError("conflicting event identity/content")
            return
        expires = packet.now + RETENTION
        self.state.update(packet.fact.digest)
        register(ctx, expires, self.logical)
        yield packet

    def on_timer(self, timestamp, ctx):
        self.state.clear()
        return ()


class MatchAttempts(KeyedProcessFunction):
    def __init__(self, logical):
        self.logical = logical

    def open(self, runtime_context):
        self.state = runtime_context.get_state(
            ValueStateDescriptor("attempt-lifecycle-v1", Types.PICKLED_BYTE_ARRAY())
        )

    def process_element(self, packet, ctx):
        state = self.state.value() or Attempt()
        previous_expiry = state.expires_at
        contribution = state.accept(packet)
        self.state.update(state)
        if state.expires_at != previous_expiry:
            if previous_expiry:
                delete_timer(ctx, previous_expiry, self.logical)
            register(ctx, state.expires_at, self.logical)
        if contribution is not None:
            yield contribution

    def on_timer(self, timestamp, ctx):
        state = self.state.value()
        if state is not None and timestamp * 1000 >= state.expires_at:
            state.expire()
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
            ValueStateDescriptor(f"{self.view}-snapshot-v2", Types.PICKLED_BYTE_ARRAY())
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

    def process_element(self, contribution, ctx):
        start = contribution.start
        entity = start.customer if self.view == "customer" else start.method
        state = self.state.value() or Entity(
            self.view,
            start.integration,
            entity,
            contribution.bootstrap,
            max_entries=self.max_entries,
        )
        state.add(contribution)
        now = contribution.available_at if self.logical else micros()
        yield from self.emit(state, now, ctx)

    def on_timer(self, timestamp, ctx):
        state = self.state.value()
        if state is not None:
            now = timestamp * 1000 if self.logical else micros()
            yield from self.emit(state, now, ctx)
