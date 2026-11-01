import argparse
import json
import os
import time
import urllib.request
from collections import Counter
from datetime import UTC, date, datetime

from confluent_kafka import Consumer, TopicPartition
from pyiceberg.expressions import And, EqualTo, GreaterThanOrEqual, LessThan

from fraud_feast.apply import ensure_tables
from fraud_feast.lake import catalog, read_json, write_json
from fraud_feast.repo import KEYS, TABLES
from fraud_feast.stream_writer import OfflineWriter, decode, properties


def rest(path):
    with urllib.request.urlopen(os.environ["HISTORY_FLINK_URL"] + path, timeout=30) as response:
        return json.load(response)


def consumed(consumer, seed):
    epoch = os.environ["FEATURE_EPOCH"]
    offsets = consumer.committed(
        [TopicPartition(f"payments.events.{epoch}", 0), TopicPartition(f"fraud.clock.{epoch}", 0)],
        timeout=30,
    )
    result = {"input": offsets[0].offset, "clock": offsets[1].offset}
    return (
        result
        if result["input"] >= seed["input_end"] and result["clock"] >= seed["clock_end"]
        else None
    )


def checkpoints():
    running = [job["jid"] for job in rest("/jobs/overview")["jobs"] if job["state"] == "RUNNING"]
    if len(running) != 1:
        raise ValueError("history must have exactly one running Flink job")
    job = running[0]
    latest = rest(f"/jobs/{job}/checkpoints").get("latest", {}).get("completed")
    return job, latest["id"] if latest else -1


def counters(job):
    result = Counter(duplicates=0, too_late=0, rejects=0, decode_rejects=0)
    for vertex in rest(f"/jobs/{job}")["vertices"]:
        prefix = f"/jobs/{job}/vertices/{vertex['id']}/subtasks/metrics"
        names = [
            item["id"]
            for item in rest(prefix)
            if item["id"].endswith(("duplicates_dropped", "too_late", "rejected"))
        ]
        if names:
            from urllib.parse import urlencode

            for metric in rest(prefix + "?" + urlencode({"get": ",".join(names), "agg": "sum"})):
                name = metric["id"]
                count = int(float(metric.get("sum", 0)))
                key = (
                    "duplicates"
                    if name.endswith("duplicates_dropped")
                    else "too_late"
                    if name.endswith("too_late")
                    else "rejects"
                )
                result[key] += count
                if name.endswith("rejected") and any(
                    f"reject_class.{kind}." in name
                    for kind in ("undecodable", "invalid_event", "invalid_context")
                ):
                    result["decode_rejects"] += count
    return dict(result)


def bounds(start, end):
    first = datetime.combine(date.fromisoformat(start), datetime.min.time(), UTC)
    last = datetime.combine(date.fromisoformat(end), datetime.min.time(), UTC)
    if not os.environ["FEATURE_EPOCH"].startswith("history-") or first >= last:
        raise ValueError("history requires a history epoch and a nonempty range")
    return first, last


def prepare(start, end):
    first, last = bounds(start, end)
    epoch = os.environ["FEATURE_EPOCH"]
    lake = catalog()
    ensure_tables(lake)
    predicate = And(
        EqualTo("epoch", epoch),
        And(GreaterThanOrEqual("event_timestamp", first), LessThan("event_timestamp", last)),
    )
    for name in TABLES.values():
        table = lake.load_table(name)
        with table.transaction() as transaction:
            transaction.delete(predicate)
            with transaction.update_snapshot(
                snapshot_properties=properties(epoch, {0: 0})
            ).fast_append():
                pass


def run(start, end):
    first, last = bounds(start, end)
    epoch = os.environ["FEATURE_EPOCH"]
    lake = catalog()
    writer = OfflineWriter({scope: lake.load_table(name) for scope, name in TABLES.items()}, epoch)
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
    start_offset = writer.resume([0])[0]
    consumer.assign([TopicPartition(topic, 0, start_offset)])
    rows_written = Counter()
    seed = progress = checkpoint = ends = None
    position = start_offset
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
                scope, row, _ = decode(message, epoch)
                if first <= row["event_timestamp"] < last:
                    rows[scope].append(row)
                position = message.offset() + 1
            if messages:
                writer.append(rows, {0: position})
                consumer.commit(offsets=[TopicPartition(topic, 0, position)], asynchronous=False)
                for scope, values in rows.items():
                    rows_written[scope] += len(values)
            if ends is None and time.monotonic() - last_check >= 5:
                seed = read_json(os.environ["HISTORY_SEED_REPORT"])
                if seed and seed.get("failed"):
                    raise RuntimeError("history seed failed")
                if seed and seed.get("done"):
                    progress = consumed(consumer, seed)
                    if progress:
                        job, latest = checkpoints()
                        if checkpoint is None:
                            checkpoint = latest
                        elif latest >= checkpoint + 2:
                            ends = consumer.get_watermark_offsets(
                                TopicPartition(topic, 0), timeout=30
                            )[1]
                last_check = time.monotonic()
        job, _ = checkpoints()
        report = {
            "start": start,
            "end": end,
            "epoch": epoch,
            "seed": seed,
            "counts": counters(job),
            "rows": dict(rows_written),
            "consumed": progress,
            "gate": False,
        }
        write_json(os.environ["RUN_REPORT"], report)
    finally:
        consumer.close()


def validate_gate(report):
    seed, progress = report["seed"], report["consumed"]
    if not seed or not seed.get("done") or report["counts"]["decode_rejects"]:
        raise ValueError("history seed is incomplete or replay has decode rejects")
    if (
        not progress
        or progress["input"] < seed["input_end"]
        or progress["clock"] < seed["clock_end"]
    ):
        raise ValueError("history has not consumed every seeded record")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=("prepare", "gate"))
    parser.add_argument("--start")
    parser.add_argument("--end")
    args = parser.parse_args()
    if args.action == "prepare":
        if not args.start or not args.end:
            parser.error("prepare requires start and end")
        prepare(args.start, args.end)
    else:
        uri = os.environ["RUN_REPORT"]
        report = read_json(uri)
        validate_gate(report)
        report["gate"] = True
        write_json(uri, report)


if __name__ == "__main__":
    main()
