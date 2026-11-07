import argparse
import hashlib
import json
import os
import random
import signal
import threading
import time
from contextlib import ExitStack
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
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
    ):
        raise ValueError("incompatible snapshot identity")
    scope = "customer" if update.view == 1 else "payment_method"
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


def commit_with_retry(table, write, attempts=8):
    for attempt in range(attempts):
        table.refresh()
        try:
            with table.transaction() as transaction:
                write(transaction)
            return
        except CommitFailedException:
            if attempt == attempts - 1:
                raise
            # Concurrent history units commit on the same tables; jitter keeps their retries apart.
            time.sleep(min(0.1 * 2**attempt, 5) * (0.5 + random.random()))


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
        self.epoch = epoch
        self.first, self.last = first, last
        self.files = {scope: [] for scope in tables}
        self.writers = {}
        self.streams = ExitStack()

    def append(self, rows, offsets):
        for scope, table in self.tables.items():
            if not rows[scope]:
                continue
            # Iceberg add_files assigns IDs through name mapping; its input must omit field IDs.
            schema = pa.schema([field.remove_metadata() for field in table.schema().as_arrow()])
            batch = pa.Table.from_pylist(rows[scope], schema=schema)
            if (
                pc.any(pc.not_equal(batch["epoch"], self.epoch)).as_py()
                or pc.any(pc.less(batch["event_timestamp"], self.first)).as_py()
                or pc.any(pc.greater_equal(batch["event_timestamp"], self.last)).as_py()
            ):
                raise ValueError("staged history row is outside its replay range")
            days = batch["event_timestamp"].cast(pa.timestamp("us")).cast(pa.date32())
            for day in pc.unique(days):
                key = scope, day.as_py()
                if key not in self.writers:
                    path = table.location_provider().new_data_location(f"history-{uuid4()}.parquet")
                    output = self.streams.enter_context(table.io.new_output(path).create())
                    parquet = self.streams.enter_context(
                        pq.ParquetWriter(output, batch.schema, compression="zstd")
                    )
                    self.writers[key] = parquet, output
                    self.files[scope].append(path)
                parquet, output = self.writers[key]
                parquet.write_table(batch.filter(pc.equal(days, day)))
                if output.tell() >= int(
                    table.properties.get("write.target-file-size-bytes", 268435456)
                ):
                    parquet.close()
                    output.close()
                    del self.writers[key]

    def close(self):
        self.streams.close()

    def publish(self):
        self.close()
        predicate = And(
            EqualTo("epoch", self.epoch),
            And(
                GreaterThanOrEqual("event_timestamp", self.first),
                LessThan("event_timestamp", self.last),
            ),
        )
        for scope, table in self.tables.items():

            def replace_range(transaction):
                transaction.delete(predicate)
                if self.files[scope]:
                    # UUID paths belong to this attempt; failed optimistic commits can reuse them.
                    transaction.add_files(self.files[scope], check_duplicate_files=False)

            commit_with_retry(table, replace_range)


class StreamWriter:
    def __init__(self, offline, feature_store=None, *, offline_interval=300, offline_rows=100000):
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
        if self.offsets is None:
            return False
        self.offline.append(self.pending, self.offsets)
        self.pending = {scope: [] for scope in KEYS}
        self.offsets = None
        self.last_flush = time.monotonic()
        return True


def batches(consumer, topic, starts, stopped, batch_size=1000, *, history=False):
    consumer.assign([TopicPartition(topic, p, start) for p, start in starts.items()])
    positions = dict(starts)
    finished = set()
    pending = []
    touched = time.monotonic()
    while not stopped.is_set() and len(finished) < len(starts):
        message = consumer.poll(1)
        if message is not None:
            partition = message.partition()
            if message.error():
                if message.error().code() != KafkaError._PARTITION_EOF:
                    raise RuntimeError("journal consumer failed")
                positions[partition] = max(positions[partition], message.offset())
                if history:
                    finished.add(partition)
                    consumer.pause([TopicPartition(topic, partition)])
            else:
                pending.append(message)
                positions[partition] = message.offset() + 1
        if len(pending) >= batch_size or time.monotonic() - touched >= 5:
            yield pending, dict(positions)
            pending = []
            touched = time.monotonic()
    if pending:
        yield pending, dict(positions)
    if stopped.is_set() and history:
        raise RuntimeError("bounded replay interrupted")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--history", action="store_true")
    parser.add_argument("--history-start")
    parser.add_argument("--history-end")
    parser.add_argument("--offline-interval", type=float, default=300)
    parser.add_argument("--offline-rows", type=int, default=100000)
    args = parser.parse_args()
    if args.history != bool(args.history_start and args.history_end) or (
        not args.history and (args.history_start or args.history_end)
    ):
        parser.error("--history requires both history days")
    epoch = os.environ["FEATURE_EPOCH"]
    if epoch not in epochs() or args.history != epoch.startswith("history-"):
        raise ValueError("writer mode must match a configured live or history epoch")
    topic = os.environ["FEATURE_UPDATE_TOPIC"]
    history = None
    if args.history:
        from fraud_feast.history import HistoryReplay

        history = HistoryReplay(args.history_start, args.history_end)
    consumer = Consumer(
        {
            "bootstrap.servers": os.environ["FEATURE_KAFKA_BROKERS"],
            "group.id": f"feature-writer-{topic}" if history else "feature-writer",
            "enable.auto.commit": False,
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
    offline = None
    try:
        partitions = sorted(consumer.list_topics(topic, timeout=30).topics[topic].partitions)
        lake = catalog()
        tables = {scope: lake.load_table(name) for scope, name in TABLES.items()}
        if history:
            history.check()
            offline = HistoryWriter(tables, epoch, history.first, history.last)
        else:
            offline = IcebergWriter(tables, epoch)
        writer = StreamWriter(
            offline,
            None if history else store(online=True),
            offline_interval=args.offline_interval,
            offline_rows=args.offline_rows,
        )
        starts = dict.fromkeys(partitions, 0) if history else offline.resume(partitions)
        written = 0
        started = time.monotonic()
        offsets = starts
        for messages, offsets in batches(consumer, topic, starts, stopped, history=args.history):
            rows = {scope: [] for scope in KEYS}
            for message in messages:
                scope, row, _ = decode(message, epoch)
                if history is None or history.first <= row["event_timestamp"] < history.last:
                    rows[scope].append(row)
            written += sum(map(len, rows.values()))
            flushed = writer.write(rows, offsets)
            if history and flushed:
                print(
                    json.dumps(
                        {
                            "stage": "staging",
                            "rows": written,
                            "seconds": round(time.monotonic() - started, 1),
                        }
                    ),
                    flush=True,
                )
        if stopped.is_set() and history:
            raise RuntimeError("history replay interrupted")
        writer.flush()
        if history:
            print(
                json.dumps(
                    {
                        "stage": "publishing",
                        "rows": written,
                        "files": {scope: len(files) for scope, files in offline.files.items()},
                    }
                ),
                flush=True,
            )
            offline.publish()
            history.report(written)
            print(
                json.dumps(
                    {
                        "stage": "published",
                        "rows": written,
                        "seconds": round(time.monotonic() - started, 1),
                    }
                ),
                flush=True,
            )
        elif os.environ.get("RUN_REPORT"):
            write_json(
                os.environ["RUN_REPORT"],
                {
                    "mode": "live",
                    "from": starts,
                    "rows": written,
                },
            )
    finally:
        if isinstance(offline, HistoryWriter):
            offline.close()
        consumer.close()


if __name__ == "__main__":
    main()
