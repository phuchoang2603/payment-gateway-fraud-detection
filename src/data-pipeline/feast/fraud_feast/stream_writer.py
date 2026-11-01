import argparse
import hashlib
import json
import os
import signal
import threading
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pandas as pd
import pyarrow as pa
from confluent_kafka import Consumer, KafkaError, TopicPartition
from feast.data_source import PushMode
from pyiceberg.exceptions import CommitFailedException
from pyiceberg.expressions import And, EqualTo, GreaterThanOrEqual, LessThan

from fraud_feast.apply import stream_schema
from fraud_feast.contracts.features.v2.features_pb2 import SnapshotUpdate
from fraud_feast.lake import catalog, write_json
from fraud_feast.repo import KEYS, STREAM_FIELDS, TABLES, epochs
from fraud_feast.runtime import store

DEFINITION_HASH = hashlib.sha256(
    Path(
        os.environ.get("FEATURE_DEFINITION")
        or Path(__file__).resolve().parents[4] / "contracts/features/fraud-snapshot-v2.yaml"
    ).read_bytes()
).hexdigest()


def decode(message, epoch):
    raw = message.value()
    if not raw or len(raw) > 128 * 1024:
        raise ValueError("invalid journal payload size")
    update = SnapshotUpdate.FromString(raw)
    identity = SnapshotUpdate()
    identity.CopyFrom(update)
    identity.ClearField("snapshot_id")
    if (
        update.schema_version != 1
        or update.definition.version != "fraud-snapshot-v2"
        or update.definition.sha256 != DEFINITION_HASH
        or update.epoch != epoch
        or update.view not in (1, 2)
        or not update.integration_id
        or not update.entity_id
        or not update.revision
        or not update.HasField("coverage")
        or not update.HasField("values")
        or update.snapshot_id
        != hashlib.sha256(identity.SerializeToString(deterministic=True)).hexdigest()
    ):
        raise ValueError("incompatible snapshot identity")
    scope = "customer" if update.view == 1 else "payment_method"
    digest = hashlib.sha256()
    for value in (
        epoch,
        "customer" if update.view == 1 else "method",
        update.integration_id,
        update.entity_id,
    ):
        encoded = value.encode()
        digest.update(str(len(encoded)).encode() + b":" + encoded)
    if message.key() != digest.hexdigest().encode():
        raise ValueError("journal key does not match snapshot")
    for field in ("anchor", "source_available_at", "online_write_at"):
        if not update.HasField(field) or getattr(update, field).nanos % 1000:
            raise ValueError("snapshot requires microsecond timestamps")
        getattr(update, field).ToDatetime(tzinfo=UTC)
    if update.online_write_at.ToMicroseconds() < update.anchor.ToMicroseconds():
        raise ValueError("write time precedes anchor")
    if update.source_available_at.ToMicroseconds() > update.online_write_at.ToMicroseconds():
        raise ValueError("source availability follows write")
    values = {}
    prefix = "customer" if update.view == 1 else "method"
    for window, coverage in (
        ("5m", update.coverage.five_minutes),
        ("1h", update.coverage.one_hour),
        ("24h", update.coverage.twenty_four_hours),
    ):
        if coverage not in (1, 2):
            raise ValueError("invalid window coverage")
        fields = [f"attempt_count_{window}"]
        if scope == "customer" and window == "24h":
            fields.append("distinct_devices_24h")
        for field in fields:
            present = update.values.HasField(field)
            if present != (coverage == 2) or (present and getattr(update.values, field) < 0):
                raise ValueError("snapshot value conflicts with coverage")
            name = f"{prefix}_snapshot_{field}"
            values[name] = getattr(update.values, field) if present else None
    if scope == "payment_method" and update.values.HasField("distinct_devices_24h"):
        raise ValueError("method snapshot contains customer values")
    if not update.source_progress:
        raise ValueError("missing source progress")
    seen = set()
    for progress in update.source_progress:
        key = progress.topic, progress.partition
        if (
            not progress.topic
            or progress.partition < 0
            or progress.consumed_offset < -1
            or progress.observed_high_watermark <= progress.consumed_offset
            or not progress.HasField("observed_at")
            or key in seen
        ):
            raise ValueError("invalid source progress")
        if progress.observed_at.nanos % 1000:
            raise ValueError("source observation requires microsecond timestamps")
        progress.observed_at.ToDatetime(tzinfo=UTC)
        seen.add(key)
    timestamp_type, timestamp = message.timestamp()
    if timestamp_type == 0 or timestamp < 0:
        raise ValueError("journal record has no timestamp")
    row = {
        "integration_id": update.integration_id,
        KEYS[scope]: update.entity_id,
        "event_timestamp": update.online_write_at.ToDatetime(tzinfo=UTC)
        + timedelta(microseconds=1),
        "created": datetime.fromtimestamp(timestamp / 1000, UTC),
        "epoch": epoch,
        "revision": update.revision,
        "_kafka_partition": message.partition(),
        "_kafka_offset": message.offset(),
        **values,
    }
    return scope, row, update


def frame(rows, fields):
    result = pd.DataFrame(rows)
    for name in fields:
        result[name] = pd.array(
            result[name], dtype="Float64" if name.startswith("decline_rate_") else "Int64"
        )
    return result


def write_online(feature_store, view, rows, fields):
    if rows:
        feature_store.push(f"{view}_push", frame(rows, fields), to=PushMode.ONLINE)


def watermarks(table, epoch):
    return {
        int(k): int(v)
        for k, v in json.loads(table.properties.get(f"kafka.offsets.{epoch}", "{}")).items()
    }


def commit_with_retry(table, write):
    for attempt in range(4):
        table.refresh()
        try:
            with table.transaction() as transaction:
                write(transaction)
            return
        except CommitFailedException:
            if attempt == 3:
                raise
            time.sleep(0.1 * 2**attempt)


class IcebergWriter:
    def __init__(self, tables, epoch):
        self.tables = tables
        self.epoch = epoch

    def resume(self, partitions):
        marks = [watermarks(table.refresh(), self.epoch) for table in self.tables.values()]
        return {p: min(mark.get(p, 0) for mark in marks) for p in partitions}

    def append(self, rows, offsets):
        for scope, table in self.tables.items():

            def append_batch(transaction):
                prior = watermarks(table, self.epoch)
                selected = [
                    row
                    for row in rows[scope]
                    if row["_kafka_offset"] >= prior.get(row["_kafka_partition"], 0)
                ]
                ends = {**prior, **{p: max(offsets[p], prior.get(p, 0)) for p in offsets}}
                if ends == prior:
                    return
                transaction.append(
                    pa.Table.from_pylist(selected, schema=stream_schema(scope).as_arrow())
                )
                transaction.set_properties(
                    {f"kafka.offsets.{self.epoch}": json.dumps(ends, sort_keys=True)}
                )

            commit_with_retry(table, append_batch)


class HistoryWriter:
    def __init__(self, tables, epoch, first, last):
        self.tables = tables
        predicate = And(
            EqualTo("epoch", epoch),
            And(GreaterThanOrEqual("event_timestamp", first), LessThan("event_timestamp", last)),
        )
        for table in tables.values():
            commit_with_retry(table, lambda transaction: transaction.delete(predicate))

    def append(self, rows):
        for scope, table in self.tables.items():
            if rows[scope]:
                data = pa.Table.from_pylist(rows[scope], schema=stream_schema(scope).as_arrow())
                commit_with_retry(table, lambda transaction: transaction.append(data))


class StreamWriter:
    def __init__(
        self, offline=None, feature_store=None, *, offline_interval=300, offline_rows=100000
    ):
        if offline_interval <= 0 or offline_rows <= 0:
            raise ValueError("offline flush bounds must be positive")
        self.offline = offline
        self.feature_store = feature_store
        self.offline_interval = offline_interval
        self.offline_rows = offline_rows
        self.pending = {scope: [] for scope in KEYS}
        self.offsets = None
        self.last_flush = time.monotonic()

    def write(self, rows, offsets):
        if self.feature_store is not None:
            for scope in KEYS:
                write_online(
                    self.feature_store, f"{scope}_stream", rows[scope], STREAM_FIELDS[scope]
                )
        if self.offline is None:
            return False
        for scope in KEYS:
            self.pending[scope].extend(rows[scope])
        self.offsets = dict(offsets)
        if (
            sum(map(len, self.pending.values())) >= self.offline_rows
            or time.monotonic() - self.last_flush >= self.offline_interval
        ):
            return self.flush()
        return False

    def flush(self):
        if self.offline is None or self.offsets is None:
            return False
        self.offline.append(self.pending, self.offsets)
        self.pending = {scope: [] for scope in KEYS}
        self.offsets = None
        self.last_flush = time.monotonic()
        return True


def resolve_range(consumer, topic, value, partitions):
    if value.startswith("{"):
        offsets = {int(k): int(v) for k, v in json.loads(value).items()}
        if set(offsets) != set(partitions) or any(v < 0 for v in offsets.values()):
            raise ValueError("range must name every partition with a nonnegative offset")
        return offsets
    instant = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if instant.tzinfo is None:
        raise ValueError("replay timestamps must include UTC offset")
    resolved = consumer.offsets_for_times(
        [TopicPartition(topic, p, int(instant.timestamp() * 1000)) for p in partitions],
        timeout=30,
    )
    return {
        item.partition: item.offset
        if item.offset >= 0
        else consumer.get_watermark_offsets(item, timeout=30)[1]
        for item in resolved
    }


def retained(consumer, topic, starts, ends=None):
    for partition, start in starts.items():
        low, high = consumer.get_watermark_offsets(TopicPartition(topic, partition), timeout=30)
        if start < low or start > high or (ends and not start <= ends[partition] <= high):
            raise ValueError("journal range is no longer retained or exceeds its end")


def batches(consumer, topic, starts, ends, stopped, batch_size=1000, history=None):
    consumer.assign([TopicPartition(topic, p, start) for p, start in starts.items()])
    positions = dict(starts)
    finished = set()
    eof = set()
    draining = False
    pending = []
    touched = time.monotonic()
    while not stopped.is_set() and len(finished) < len(starts):
        if history is not None and not draining and history.ends():
            draining = True
            eof.clear()
            for partition, offset in positions.items():
                consumer.seek(TopicPartition(topic, partition, offset))
        if draining:
            finished.update(eof)
        if ends is not None:
            finished.update(p for p in starts if positions[p] >= ends[p])
        if len(finished) == len(starts):
            break
        message = consumer.poll(1)
        if message is not None:
            partition = message.partition()
            if message.error():
                if message.error().code() != KafkaError._PARTITION_EOF:
                    raise RuntimeError("journal consumer failed")
                positions[partition] = max(positions[partition], message.offset())
                eof.add(partition)
                if ends is not None:
                    finished.add(partition)
                    consumer.pause([TopicPartition(topic, partition)])
            else:
                eof.discard(partition)
                if ends is None or message.offset() < ends[partition]:
                    pending.append(message)
                    positions[partition] = message.offset() + 1
                else:
                    positions[partition] = ends[partition]
                    finished.add(partition)
                    consumer.pause([TopicPartition(topic, partition)])
        if len(pending) >= batch_size or time.monotonic() - touched >= 5:
            yield pending, dict(positions)
            pending = []
            touched = time.monotonic()
    if pending:
        yield pending, dict(positions)
    if stopped.is_set() and (ends is not None or history is not None):
        raise RuntimeError("bounded replay interrupted")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--from", dest="start")
    parser.add_argument("--until")
    parser.add_argument("--history", action="store_true")
    parser.add_argument("--history-start")
    parser.add_argument("--history-end")
    parser.add_argument("--offline-interval", type=float, default=300)
    parser.add_argument("--offline-rows", type=int, default=100000)
    args = parser.parse_args()
    if bool(args.start) != bool(args.until):
        parser.error("--from and --until are required together")
    if (
        args.history != bool(args.history_start and args.history_end)
        or (not args.history and (args.history_start or args.history_end))
        or (args.history and args.start)
    ):
        parser.error("--history requires both history days and no replay range")
    epoch = os.environ["FEATURE_EPOCH"]
    if epoch not in epochs() or args.history != epoch.startswith("history-"):
        raise ValueError("writer mode must match a configured live or history epoch")
    topic = os.environ["FEATURE_UPDATE_TOPIC"]
    replay = args.start is not None
    history = None
    if args.history:
        from fraud_feast.history import HistoryReplay

        history = HistoryReplay(args.history_start, args.history_end)
    consumer = Consumer(
        {
            "bootstrap.servers": os.environ["FEATURE_KAFKA_BROKERS"],
            "group.id": f"feature-writer-replay-{os.environ.get('RUN_ID', 'bounded')}"
            if replay
            else f"feature-writer-{epoch}"
            if history
            else "feature-writer",
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "auto.offset.reset": "error",
            "isolation.level": "read_committed",
            "enable.partition.eof": True,
            "allow.auto.create.topics": False,
            "max.poll.interval.ms": 3600000,
        }
    )
    stopped = threading.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: stopped.set())
    try:
        partitions = sorted(consumer.list_topics(topic, timeout=30).topics[topic].partitions)
        offline = None
        if not replay:
            lake = catalog()
            tables = {scope: lake.load_table(name) for scope, name in TABLES.items()}
            offline = (
                HistoryWriter(tables, epoch, history.first, history.last)
                if history
                else IcebergWriter(tables, epoch)
            )
        writer = (
            None
            if history
            else StreamWriter(
                offline,
                store(online=True),
                offline_interval=args.offline_interval,
                offline_rows=args.offline_rows,
            )
        )
        if replay:
            starts = resolve_range(consumer, topic, args.start, partitions)
            ends = resolve_range(consumer, topic, args.until, partitions)
        elif history:
            starts, ends = dict.fromkeys(partitions, 0), None
        else:
            starts, ends = offline.resume(partitions), None
        retained(consumer, topic, starts, ends)
        written = 0
        offsets = starts
        for messages, offsets in batches(consumer, topic, starts, ends, stopped, history=history):
            rows = {scope: [] for scope in KEYS}
            for message in messages:
                scope, row, _ = decode(message, epoch)
                if history is None or history.first <= row["event_timestamp"] < history.last:
                    rows[scope].append(row)
            if history:
                offline.append(rows)
            elif writer.write(rows, offsets):
                consumer.commit(
                    offsets=[TopicPartition(topic, p, offset) for p, offset in offsets.items()],
                    asynchronous=False,
                )
            written += sum(map(len, rows.values()))
        if stopped.is_set() and history:
            raise RuntimeError("history replay interrupted")
        if writer is not None and writer.flush():
            consumer.commit(
                offsets=[TopicPartition(topic, p, offset) for p, offset in offsets.items()],
                asynchronous=False,
            )
        if history:
            history.report(written)
        elif os.environ.get("RUN_REPORT"):
            write_json(
                os.environ["RUN_REPORT"],
                {
                    "mode": "replay" if replay else "live",
                    "from": starts,
                    "until": ends,
                    "rows": written,
                },
            )
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
