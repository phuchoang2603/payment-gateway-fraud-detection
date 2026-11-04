import base64
import json
import random
import sys
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from pathlib import Path

from features.reference_v2 import definition_identity, snapshot

from fraud_streaming.contracts import PaymentEvent, SnapshotUpdate

MINUTE = 60_000_000
HOUR = 60 * MINUTE
DAY = 24 * HOUR
DEFINITION_HASH = definition_identity()["sha256"]


@dataclass(frozen=True)
class Fact:
    event_id: str
    digest: str
    integration: str
    payment: str
    attempt: str
    started_at: int
    recorded_at: int
    customer: str
    method: str
    device: str | None

    @property
    def identity(self):
        return self.integration, self.payment, self.attempt


@dataclass(frozen=True)
class Progress:
    topic: str
    partition: int
    consumed: int
    high: int
    observed_at: int


@dataclass(frozen=True)
class Packet:
    fact: Fact
    now: int
    bootstrap: int
    progress: tuple

    @property
    def too_late(self):
        return self.fact.started_at < self.now - DAY


def instant(micros):
    return datetime(1970, 1, 1, tzinfo=UTC) + timedelta(microseconds=micros)


def event(fact):
    return {
        "event_id": fact.event_id,
        "integration_id": fact.integration,
        "payment_id": fact.payment,
        "attempt_started": {
            "attempt_id": fact.attempt,
            "context": {"customer": {"customer_id": fact.customer}},
            "signals": {
                "attempted_at": instant(fact.started_at),
                "payment_method": {"fingerprint": fact.method},
                "device_fingerprint": fact.device,
            },
        },
    }


class Oracle:
    def __init__(self, view, fact, base, *, live=False):
        self.view, self.fact, self.base = view, fact, base
        self.events, self.boundaries = [], set()
        if live:
            self.boundaries.update(base + delta for delta in (5 * MINUTE, HOUR, DAY))
        self.progress = ()
        self.available = self.revision = self.write_at = 0
        self.previous = None

    def add(self, packet):
        self.events.append(event(packet.fact))
        self.progress = packet.progress
        self.available = max(self.available, packet.now)
        self.boundaries.update(
            boundary
            for delta in (0, 5 * MINUTE, HOUR, DAY)
            if (boundary := packet.fact.started_at + delta + 1) > packet.now
        )

    def emit(self, now):
        self.boundaries.difference_update(t for t in tuple(self.boundaries) if t <= now)
        probe = event(replace(self.fact, payment="oracle-probe", started_at=now))
        values = snapshot(
            probe,
            self.events,
            scope=self.view,
            anchor=instant(now),
            cutoff=instant(now),
            coverage=dict.fromkeys(("5m", "1h", "24h"), "complete"),
            status="available",
        )["values"]
        counts = tuple(
            values[f"{self.view}_snapshot_attempt_count_{window}"] for window in ("5m", "1h", "24h")
        )
        complete = tuple(now - delta >= self.base for delta in (5 * MINUTE, HOUR, DAY))
        signature = complete, counts, tuple(p.consumed for p in self.progress)
        if signature == self.previous:
            return None
        self.previous = signature
        self.revision += 1
        self.write_at = max(now, self.write_at + 1)
        result = SnapshotUpdate(
            schema_version=1,
            epoch="jvm-fixture",
            view=1 if self.view == "customer" else 2,
            integration_id=self.fact.integration,
            entity_id=getattr(self.fact, self.view),
            revision=self.revision,
        )
        result.definition.version = "fraud-snapshot-v2"
        result.definition.sha256 = DEFINITION_HASH
        result.anchor.FromMicroseconds(now)
        result.source_available_at.FromMicroseconds(self.available)
        result.online_write_at.FromMicroseconds(self.write_at)
        for field, covered in zip(("five_minutes", "one_hour", "twenty_four_hours"), complete):
            setattr(result.coverage, field, 2 if covered else 1)
        result.values.SetInParent()
        for field, covered, count in zip(
            ("attempt_count_5m", "attempt_count_1h", "attempt_count_24h"), complete, counts
        ):
            if covered:
                setattr(result.values, field, count)
        if self.view == "customer" and complete[2]:
            result.values.distinct_devices_24h = values["customer_snapshot_distinct_devices_24h"]
        for progress in self.progress:
            item = result.source_progress.add(
                topic=progress.topic,
                partition=progress.partition,
                consumed_offset=progress.consumed,
                observed_high_watermark=progress.high,
            )
            item.observed_at.FromMicroseconds(progress.observed_at)
        result.snapshot_id = sha256(result.SerializeToString(deterministic=True)).hexdigest()
        return result.SerializeToString(deterministic=True)


def encoded(value):
    return base64.b64encode(value).decode()


def generate():
    rng = random.Random(17)
    base = 1_760_000_000_000_000
    facts = []
    for index in range(400):
        now = base + index * 9 * MINUTE + index % 997
        started = now - rng.choice([0, 1, MINUTE, HOUR, DAY, DAY + 1])
        facts.append(
            Fact(
                f"event-{index}",
                f"digest-{index}",
                "integration-é",
                f"payment-{index}",
                "attempt",
                started,
                now,
                f"customer-{index % 17}",
                f"method-{index % 5}",
                None if index % 7 == 0 else f"device-{index % 3}",
            )
        )
    facts.extend([replace(facts[-1], event_id="same-instant", payment="last")])
    packets, accepted, identities = [], [], {"event": {}, "attempt": {}}
    rejects = duplicates = late = 0
    for fact in facts:
        deliveries = [fact]
        if rng.random() < 0.15:
            deliveries.extend([fact, replace(fact, digest="conflict")])
        for delivery in deliveries:
            offset = len(packets)
            packet = Packet(
                delivery, fact.recorded_at, base, (Progress("fixture", 0, offset, 10000, base),)
            )
            packets.append(packet)
            if packet.too_late:
                late += 1
                continue
            keep = True
            for scope, identity, content in (
                ("event", delivery.event_id, delivery.digest),
                ("attempt", delivery.identity, f"{delivery.event_id}:{delivery.digest}"),
            ):
                prior = identities[scope].get(identity)
                if prior is not None and packet.now >= prior[1]:
                    prior = None
                if prior is not None:
                    if prior[0] == content:
                        duplicates += 1
                    else:
                        rejects += 1
                    keep = False
                    break
                identities[scope][identity] = (content, delivery.started_at + DAY + HOUR)
            if keep:
                accepted.append(packet)
    expected, states = [], {}

    def emit(state, now):
        value = state.emit(now)
        if value is not None:
            expected.append(encoded(value))

    for packet in accepted:
        for view in ("customer", "method"):
            key = view, getattr(packet.fact, view)
            state = states.setdefault(key, Oracle(view, packet.fact, base))
            while state.boundaries and min(state.boundaries) <= packet.now:
                emit(state, min(state.boundaries))
            state.add(packet)
            emit(state, packet.now)
    for state in states.values():
        while state.boundaries:
            emit(state, min(state.boundaries))
    documents = []
    for offset, packet in enumerate(packets):
        fact = packet.fact
        documents.append(
            {
                "fact": {
                    "eventId": fact.event_id,
                    "digest": fact.digest,
                    "integration": fact.integration,
                    "payment": fact.payment,
                    "attempt": fact.attempt,
                    "customer": fact.customer,
                    "method": fact.method,
                    "device": fact.device,
                    "startedAt": fact.started_at,
                    "recordedAt": fact.recorded_at,
                },
                "now": packet.now,
                "bootstrap": base,
                "progress": [
                    {
                        "topic": "fixture",
                        "partition": 0,
                        "consumed": offset,
                        "high": 10000,
                        "observedAt": base,
                    }
                ],
                "source": {
                    "raw": encoded(b"fixture"),
                    "topic": "fixture",
                    "partition": 0,
                    "offset": offset,
                    "timestamp": packet.now // 1000,
                },
            }
        )
    event = PaymentEvent(
        schema_version=3,
        event_id="wire-event",
        payment_id="payment",
        integration_id="integration-é",
        aggregate_version=1,
    )
    event.occurred_at.FromMicroseconds(base)
    event.recorded_at.FromMicroseconds(base)
    event.attempt_started.attempt_id = "attempt"
    event.attempt_started.context.customer.customer_id = "customer"
    event.attempt_started.signals.payment_method.fingerprint = "method"
    event.attempt_started.signals.attempted_at.FromMicroseconds(base - 1)
    event.attempt_started.signals.device_fingerprint = "device"
    raw = event.SerializeToString(deterministic=True)
    live_expected = []

    def live_emit(now):
        value = live.emit(now)
        if value is not None:
            live_expected.append(encoded(value))

    first = Fact(
        "live-one",
        "one",
        "integration",
        "payment-one",
        "attempt",
        base,
        base,
        "customer",
        "method",
        "device",
    )
    live = Oracle("customer", first, base, live=True)
    live.add(Packet(first, base, base, (Progress("fixture", 0, 0, 2, base),)))
    live_emit(base)
    live_emit(base + 1000)
    second = replace(
        first, event_id="live-two", digest="two", payment="payment-two", started_at=base - 1
    )
    live.add(Packet(second, base, base, (Progress("fixture", 0, 1, 2, base),)))
    live_emit(base + 1001)
    for delta in (5 * MINUTE + 1000, HOUR + 1000, DAY + 1000):
        live_emit(base + delta)
    return {
        "liveExpected": live_expected,
        "definition": DEFINITION_HASH,
        "packets": documents,
        "expected": expected,
        "rejects": rejects,
        "duplicates": duplicates,
        "late": late,
        "wire": encoded(raw),
        "digest": sha256(raw).hexdigest(),
    }


if __name__ == "__main__":
    Path(sys.argv[1]).write_text(json.dumps(generate()))
