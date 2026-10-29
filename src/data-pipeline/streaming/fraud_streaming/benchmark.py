"""Replay benchmark comparing late and duplicate handling with naive baselines.

A seeded workload with known late, too-late, duplicate and conflicting deliveries is replayed
in logical-clock order through the job's own model logic (Entity windows, dedup
classification and the 24-hour horizon). Each variant is compared with the v2 reference
calculation at hourly anchors. The Flink runtime is covered separately by the image test.

Run: cd src/data-pipeline/streaming && PYTHONPATH=.. uv run --locked python -m
fraud_streaming.benchmark
"""

import argparse
import json
import random
from collections import Counter
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta

from features.reference_v2 import snapshot as reference_snapshot

from fraud_streaming.model import (
    DAY,
    HOUR,
    MINUTE,
    SECOND,
    Entity,
    Fact,
    Packet,
    dedup_content,
    dedup_status,
    lateness_bucket,
)

START = 1_760_000_000_000_000
VARIANTS = {
    "handled": {"dedup": True, "event_time": True},
    "no_dedup": {"dedup": False, "event_time": True},
    "arrival_time": {"dedup": True, "event_time": False},
    "baseline": {"dedup": False, "event_time": False},
}


@dataclass(frozen=True)
class Workload:
    deliveries: list
    originals: list
    customers: tuple
    methods: tuple
    hours: int
    injected: dict


def workload(
    *,
    seed=2603,
    attempts=1200,
    customers=12,
    methods=6,
    hours=48,
    late_share=0.05,
    too_late_share=0.01,
    duplicate_share=0.015,
    conflict_share=0.005,
):
    """Generate attempts plus redelivered copies, sorted by arrival time."""
    rng = random.Random(seed)
    deliveries, originals, injected = [], [], Counter()
    for index in range(attempts):
        started = START + rng.randrange(hours * HOUR)
        roll = rng.random()
        if roll < too_late_share:
            kind, arrival = "too_late", started + DAY + rng.randrange(1, HOUR)
        elif roll < too_late_share + late_share:
            kind, arrival = "late", started + rng.randrange(5 * MINUTE, 15 * MINUTE)
        else:
            kind, arrival = "on_time", started + rng.randrange(2 * SECOND)
        injected[kind] += 1
        fact = Fact(
            f"event-{index}",
            f"digest-{index}",
            "integration",
            f"payment-{index}",
            f"attempt-{index}",
            started,
            arrival,
            f"customer-{rng.randrange(customers)}",
            f"method-{rng.randrange(methods)}",
            rng.choice((None, "device-1", "device-2", "device-3")),
        )
        originals.append(fact)
        deliveries.append((arrival, fact))
        copy = rng.random()
        if kind != "too_late" and copy < duplicate_share:
            injected["duplicate"] += 1
            deliveries.append((arrival + rng.randrange(60 * SECOND), fact))
        elif kind != "too_late" and copy < duplicate_share + conflict_share:
            injected["conflict"] += 1
            conflicting = replace(fact, digest=f"conflict-{index}", device="device-conflict")
            deliveries.append((arrival + rng.randrange(60 * SECOND), conflicting))
    deliveries.sort(key=lambda delivery: (delivery[0], delivery[1].event_id))
    return Workload(
        deliveries,
        originals,
        tuple(f"customer-{index}" for index in range(customers)),
        tuple(f"method-{index}" for index in range(methods)),
        hours,
        dict(injected),
    )


def anchors(load):
    first = START + DAY + HOUR
    return range(first, START + (load.hours + 26) * HOUR, HOUR)


def replay(load, *, dedup, event_time):
    """Return observed values per (anchor, view, entity) and the handling counters."""
    entities, last, seen = {}, {}, {"event": {}, "attempt": {}}
    stats = Counter()
    observed = {}
    deliveries = iter(enumerate(load.deliveries))
    upcoming = next(deliveries, None)
    for anchor in anchors(load):
        while upcoming is not None and upcoming[1][0] <= anchor:
            index, (arrival, fact) = upcoming
            upcoming = next(deliveries, None)
            if not event_time:
                fact = replace(fact, started_at=arrival)
            if not dedup:
                fact = replace(fact, payment=f"delivery-{index}", attempt=f"delivery-{index}")
            packet = Packet(fact, arrival, START, ())
            if packet.too_late:
                stats["too_late"] += 1
                stats["lateness_too_late"] += 1
                continue
            if dedup and not admit(seen, fact, stats):
                continue
            stats[f"lateness_{lateness_bucket(arrival - fact.started_at)}"] += 1
            for view in ("customer", "method"):
                entity = fact.customer if view == "customer" else fact.method
                state = entities.setdefault(
                    (view, entity),
                    Entity(view, "integration", entity, START, max_entries=1_000_000),
                )
                state.add(packet)
        for (view, entity), state in entities.items():
            result = state.advance(anchor)
            if result is not None:
                last[view, entity] = result[1]
        for view, names in (("customer", load.customers), ("method", load.methods)):
            for entity in names:
                observed[anchor, view, entity] = last.get((view, entity)) or zeros(view)
    return observed, stats


def admit(seen, fact, stats):
    for scope in ("event", "attempt"):
        key = fact.event_id if scope == "event" else fact.identity
        content = dedup_content(scope, fact)
        status = dedup_status(seen[scope].get(key), content)
        if status == "duplicate":
            stats[f"duplicates_dropped_{scope}"] += 1
            return False
        if status == "conflict":
            stats["rejected_conflicting_duplicate"] += 1
            return False
        seen[scope][key] = content
    return True


def zeros(view):
    values = {f"attempt_count_{name}": 0 for name in ("5m", "1h", "24h")}
    if view == "customer":
        values["distinct_devices_24h"] = 0
    return values


def instant(microseconds):
    return datetime(1970, 1, 1, tzinfo=UTC) + timedelta(microseconds=microseconds)


def reference_event(fact):
    return {
        "event_id": fact.event_id,
        "integration_id": fact.integration,
        "payment_id": fact.payment,
        "recorded_at": instant(fact.recorded_at),
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


def expected(load):
    events = [(fact.recorded_at, reference_event(fact)) for fact in load.originals]
    coverage = {window: "complete" for window in ("5m", "1h", "24h")}
    values = {}
    for anchor in anchors(load):
        available = [event for recorded, event in events if recorded <= anchor]
        for view, names in (("customer", load.customers), ("method", load.methods)):
            for entity in names:
                current = reference_event(
                    Fact(
                        "current",
                        "current",
                        "integration",
                        "current",
                        "current",
                        anchor,
                        anchor,
                        entity,
                        entity,
                    )
                )
                result = reference_snapshot(
                    current,
                    available,
                    scope=view,
                    anchor=instant(anchor),
                    cutoff=instant(anchor),
                    coverage=coverage,
                    status="available",
                )
                values[anchor, view, entity] = {
                    name.removeprefix(f"{view}_snapshot_"): value
                    for name, value in result["values"].items()
                }
    return values


def compare(observed, reference):
    result = Counter()
    for key, wanted in reference.items():
        for name, value in wanted.items():
            difference = observed[key][name] - value
            result["cells"] += 1
            result["mismatched_cells"] += difference != 0
            result["over_count"] += max(difference, 0)
            result["under_count"] += max(-difference, 0)
    return dict(result)


def run(**settings):
    load = workload(**settings)
    reference = expected(load)
    report = {"injected": load.injected, "deliveries": len(load.deliveries), "variants": {}}
    for name, options in VARIANTS.items():
        observed, stats = replay(load, **options)
        report["variants"][name] = {**compare(observed, reference), "counters": dict(stats)}
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--seed", type=int, default=2603)
    parser.add_argument("--attempts", type=int, default=1200)
    arguments = parser.parse_args()
    print(json.dumps(run(seed=arguments.seed, attempts=arguments.attempts), indent=2))


if __name__ == "__main__":
    main()
