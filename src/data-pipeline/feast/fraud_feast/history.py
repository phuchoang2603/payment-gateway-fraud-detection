import argparse
import hashlib
import json
import os
import time
import urllib.request
from collections import Counter
from datetime import UTC, date, datetime, timedelta

import pyarrow as pa
from confluent_kafka import Consumer, TopicPartition
from pyiceberg.expressions import And, EqualTo, GreaterThanOrEqual, LessThan
from pyroaring import BitMap

from fraud_feast.apply import ensure_tables, stream_schema
from fraud_feast.lake import catalog, read_json, write_json
from fraud_feast.push_job import decode
from fraud_feast.repo import KEYS, TABLES


def rest(path):
    with urllib.request.urlopen(os.environ["HISTORY_FLINK_URL"] + path, timeout=30) as response:
        return json.load(response)


def completed(consumer, seed):
    epoch = os.environ["FEATURE_EPOCH"]
    source = f"payments.events.{epoch}"
    clock = f"fraud.clock.{epoch}"
    offsets = consumer.committed([TopicPartition(source, 0), TopicPartition(clock, 0)], timeout=30)
    return offsets[0].offset >= seed["input_end"] and offsets[1].offset >= seed["clock_end"]


def checkpoints():
    jobs = rest("/jobs/overview")["jobs"]
    running = [job["jid"] for job in jobs if job["state"] == "RUNNING"]
    if len(running) != 1:
        raise ValueError("history must have exactly one running Flink job")
    job = running[0]
    result = rest(f"/jobs/{job}/checkpoints")
    latest = result.get("latest", {}).get("completed")
    return job, latest["id"] if latest else -1


def counters(job):
    result = Counter(duplicates=0, too_late=0, rejects=0)
    for vertex in rest(f"/jobs/{job}")["vertices"]:
        prefix = f"/jobs/{job}/vertices/{vertex['id']}/subtasks/metrics"
        available = rest(prefix)
        names = [
            item["id"]
            for item in available
            if item["id"].endswith(("duplicates_dropped", "too_late", "rejected"))
        ]
        if names:
            from urllib.parse import urlencode

            for metric in rest(prefix + "?" + urlencode({"get": ",".join(names), "agg": "sum"})):
                key = (
                    "duplicates"
                    if metric["id"].endswith("duplicates_dropped")
                    else "too_late"
                    if metric["id"].endswith("too_late")
                    else "rejects"
                )
                result[key] += int(float(metric.get("sum", 0)))
    return dict(result)


def staging_names():
    run = os.environ["RUN_ID"].replace("-", "_")
    if not run.replace("_", "").isalnum():
        raise ValueError("invalid history run id")
    return {scope: f"gold._history_{run}_{scope}" for scope in KEYS}


def run(start, end):
    epoch = os.environ["FEATURE_EPOCH"]
    if not epoch.startswith("history-") or date.fromisoformat(start) >= date.fromisoformat(end):
        raise ValueError("history requires a history epoch and a nonempty range")
    lake = catalog()
    ensure_tables(lake)
    names = staging_names()
    for scope, name in names.items():
        if lake.table_exists(name):
            lake.purge_table(name)
        source = lake.load_table(TABLES[scope])
        lake.create_table(name, schema=stream_schema(scope), partition_spec=source.spec())
    tables = {scope: lake.load_table(name) for scope, name in names.items()}
    topic = os.environ["FEATURE_UPDATE_TOPIC"]
    consumer = Consumer(
        {
            "bootstrap.servers": os.environ["FEATURE_KAFKA_BROKERS"],
            "group.id": f"fraud-features-{epoch}",
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "isolation.level": "read_committed",
            "auto.offset.reset": "error",
            "allow.auto.create.topics": False,
        }
    )
    consumer.assign([TopicPartition(topic, 0, 0)])
    observed = {scope: BitMap() for scope in KEYS}
    rows_written = Counter()
    first = datetime.combine(date.fromisoformat(start), datetime.min.time(), UTC)
    last = datetime.combine(date.fromisoformat(end), datetime.min.time(), UTC)
    seed = None
    checkpoint = None
    ends = None
    position = 0
    last_check = 0
    deadline = time.monotonic() + int(os.environ.get("HISTORY_TIMEOUT_SECONDS", "259200"))
    try:
        while ends is None or position < ends:
            if time.monotonic() > deadline:
                raise TimeoutError("history unit did not finish before its deadline")
            messages = consumer.consume(num_messages=1000, timeout=1)
            rows = {scope: [] for scope in KEYS}
            for message in messages:
                if message.error():
                    raise RuntimeError("history journal consumer failed")
                scope, row, update = decode(message, epoch)
                for progress in update.source_progress:
                    if (
                        progress.topic == f"payments.events.{epoch}"
                        and progress.partition == 0
                        and progress.consumed_offset >= 0
                    ):
                        observed[scope].add(progress.consumed_offset)
                if first <= row["event_timestamp"] < last:
                    rows[scope].append(row)
                position = message.offset() + 1
            for scope, values in rows.items():
                if values:
                    tables[scope].append(
                        pa.Table.from_pylist(values, schema=stream_schema(scope).as_arrow())
                    )
                    rows_written[scope] += len(values)
            if ends is None and time.monotonic() - last_check >= 5:
                seed = read_json(os.environ["HISTORY_SEED_REPORT"])
                if seed and seed.get("failed"):
                    raise RuntimeError("history seed failed")
                if seed and seed.get("done") and completed(consumer, seed):
                    job, latest = checkpoints()
                    if checkpoint is None:
                        checkpoint = latest
                    elif latest >= checkpoint + 2:
                        ends = consumer.get_watermark_offsets(TopicPartition(topic, 0), timeout=30)[
                            1
                        ]
                last_check = time.monotonic()
        if seed is None:
            raise ValueError("history seed report is missing")
        job, _ = checkpoints()
        report = {
            "start": start,
            "end": end,
            "epoch": epoch,
            "staging": names,
            "seed": seed,
            "counts": counters(job),
            "rows": dict(rows_written),
            "observed": {
                scope: {
                    "count": len(bitmap),
                    "sha256": hashlib.sha256(bitmap.serialize()).hexdigest(),
                }
                for scope, bitmap in observed.items()
            },
            "gate": False,
        }
        write_json(os.environ["RUN_REPORT"], report)
    finally:
        consumer.close()


def validate_gate(report):
    counts = report["counts"]
    seed = report["seed"]
    if counts["rejects"] or any(counts[k] != seed["counts"][k] for k in ("duplicates", "too_late")):
        raise ValueError("history rejection counts differ from Bronze")
    for scope in KEYS:
        if (
            report["observed"][scope]["count"] != seed["accepted"]
            or report["observed"][scope]["sha256"] != seed["accepted_sha256"]
        ):
            raise ValueError("history snapshots are missing seeded attempts")


def publish_day(target, source, epoch, day):
    start = datetime.combine(day, datetime.min.time(), UTC)
    end = start + timedelta(days=1)
    predicate = And(
        EqualTo("epoch", epoch),
        And(GreaterThanOrEqual("event_timestamp", start), LessThan("event_timestamp", end)),
    )
    with target.transaction() as transaction:
        transaction.delete(
            predicate, snapshot_properties={"history.epoch": epoch, "history.day": str(day)}
        )
        for batch in source.scan(row_filter=predicate).to_arrow_batch_reader():
            transaction.append(
                pa.Table.from_batches([batch]),
                snapshot_properties={"history.epoch": epoch, "history.day": str(day)},
            )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--cleanup", action="store_true")
    args = parser.parse_args()
    lake = catalog()
    if args.cleanup:
        for name in staging_names().values():
            if lake.table_exists(name):
                lake.purge_table(name)
        return
    uri = os.environ["RUN_REPORT"]
    report = read_json(uri)
    validate_gate(report)
    first, last = date.fromisoformat(report["start"]), date.fromisoformat(report["end"])
    for scope, name in report["staging"].items():
        target, source = lake.load_table(TABLES[scope]), lake.load_table(name)
        day = first
        while day < last:
            publish_day(target, source, report["epoch"], day)
            day += timedelta(days=1)
    report["gate"] = True
    write_json(uri, report)


if __name__ == "__main__":
    main()
