import argparse
import hashlib
import json
import os
import signal
import tempfile
import threading
import time
from datetime import UTC, datetime
from functools import reduce
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from confluent_kafka import Consumer, KafkaError, TopicPartition
from feast.data_source import PushMode
from pyiceberg.expressions import And, EqualTo, GreaterThanOrEqual, LessThan, Or

from fraud_feast.apply import stream_schema
from fraud_feast.contracts.features.v2.features_pb2 import SnapshotUpdate
from fraud_feast.lake import catalog, write_json
from fraud_feast.repo import KEYS, STREAM_FIELDS, TABLES, epochs
from fraud_feast.runtime import store

DEFINITION_HASH = "958f9b0fd7e50ba33a22526271ff2e62eafcf827c9f9e0bce1c35c4a016f85a3"


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
        "event_timestamp": update.online_write_at.ToDatetime(tzinfo=UTC),
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


def write_online(feature_store, view, rows, fields, *, push=False):
    if not rows:
        return
    data = frame(rows, fields)
    missing = data[fields].isna().all(axis=1)
    present = data.loc[~missing]
    if not present.empty:
        if push:
            feature_store.push(f"{view}_push", present, to=PushMode.ONLINE)
        else:
            feature_store.write_to_online_store(view, present)
    if missing.any():
        # Feast 0.67's public write skips all-null rows, including coverage and deletion tombstones.
        feature_store._get_provider().ingest_df(
            feature_store.get_feature_view(view), data.loc[missing]
        )


def watermarks(table, epoch):
    for snapshot in reversed(table.metadata.snapshots):
        summary = snapshot.summary
        if summary and summary.get("kafka.epoch") == epoch and summary.get("kafka.offsets"):
            return {int(k): int(v) for k, v in json.loads(summary["kafka.offsets"]).items()}
    return {}


def properties(epoch, offsets):
    return {"kafka.epoch": epoch, "kafka.offsets": json.dumps(offsets, sort_keys=True)}


def offset_filter(epoch, starts, ends):
    partitions = [
        And(
            EqualTo("_kafka_partition", partition),
            And(
                GreaterThanOrEqual("_kafka_offset", start),
                LessThan("_kafka_offset", ends[partition]),
            ),
        )
        for partition, start in starts.items()
        if start < ends[partition]
    ]
    if not partitions:
        raise ValueError("empty replay range")
    return And(EqualTo("epoch", epoch), reduce(Or, partitions))


class OfflineWriter:
    def __init__(self, tables, epoch):
        self.tables = tables
        self.epoch = epoch

    def resume(self, partitions):
        marks = [watermarks(table, self.epoch) for table in self.tables.values()]
        return {p: min(mark.get(p, 0) for mark in marks) for p in partitions}

    def append(self, rows, offsets):
        for scope, table in self.tables.items():
            table.refresh()
            prior = watermarks(table, self.epoch)
            selected = [
                row
                for row in rows[scope]
                if row["_kafka_offset"] >= prior.get(row["_kafka_partition"], 0)
            ]
            ends = {p: max(offsets[p], prior.get(p, 0)) for p in offsets}
            if ends == prior:
                continue
            data = pa.Table.from_pylist(selected, schema=stream_schema(scope).as_arrow())
            with table.transaction() as transaction:
                if selected:
                    transaction.append(data, snapshot_properties=properties(self.epoch, ends))
                else:
                    with transaction.update_snapshot(
                        snapshot_properties=properties(self.epoch, ends)
                    ).fast_append():
                        pass

    def validate_replay(self, starts, ends):
        for table in self.tables.values():
            table.refresh()
            mark = watermarks(table, self.epoch)
            if any(ends[p] > mark.get(p, 0) for p in starts):
                raise ValueError("offline replay extends past the committed watermark")

    def replay(self, files, starts, ends):
        self.validate_replay(starts, ends)
        predicate = offset_filter(self.epoch, starts, ends)
        for scope, table in self.tables.items():
            prior = watermarks(table, self.epoch)
            with table.transaction() as transaction:
                transaction.delete(predicate, snapshot_properties=properties(self.epoch, prior))
                for path in files[scope]:
                    transaction.append(
                        pq.read_table(path), snapshot_properties=properties(self.epoch, prior)
                    )
                if not files[scope]:
                    with transaction.update_snapshot(
                        snapshot_properties=properties(self.epoch, prior)
                    ).fast_append():
                        pass


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


def batches(consumer, topic, starts, ends, stopped, batch_size=1000):
    consumer.assign([TopicPartition(topic, p, start) for p, start in starts.items()])
    positions = dict(starts)
    pending = []
    touched = time.monotonic()
    while not stopped.is_set() and (ends is None or any(positions[p] < ends[p] for p in positions)):
        message = consumer.poll(1)
        if message is not None:
            if message.error():
                if message.error().code() != KafkaError._PARTITION_EOF:
                    raise RuntimeError("journal consumer failed")
                partition = message.partition()
                positions[partition] = max(positions[partition], message.offset())
                if ends and positions[partition] < ends[partition]:
                    raise ValueError("journal ended before requested offset")
            elif ends is None or message.offset() < ends[message.partition()]:
                pending.append(message)
                positions[message.partition()] = message.offset() + 1
            else:
                positions[message.partition()] = ends[message.partition()]
                consumer.pause([TopicPartition(topic, message.partition())])
        if pending and (
            len(pending) >= batch_size
            or time.monotonic() - touched >= 5
            or (ends and all(positions[p] >= ends[p] for p in positions))
        ):
            yield pending, dict(positions)
            pending = []
            touched = time.monotonic()
    if pending:
        yield pending, dict(positions)
    if stopped.is_set() and ends is not None:
        raise RuntimeError("bounded replay interrupted")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--to", choices=("online", "offline"), required=True)
    parser.add_argument("--from", dest="start")
    parser.add_argument("--until")
    parser.add_argument("--history-start")
    parser.add_argument("--history-end")
    args = parser.parse_args()
    if bool(args.start) != bool(args.until):
        parser.error("--from and --until are required together")
    if bool(args.history_start) != bool(args.history_end):
        parser.error("--history-start and --history-end are required together")
    epoch = os.environ["FEATURE_EPOCH"]
    if epoch not in epochs():
        raise ValueError("push epoch is not selected by the repository")
    if args.history_start:
        if args.to != "offline" or args.start or not args.history_end or epoch.startswith("live"):
            parser.error(
                "history requires offline, a history epoch and both days, without a replay range"
            )
        from fraud_feast.history import run

        return run(args.history_start, args.history_end)
    topic = os.environ["FEATURE_UPDATE_TOPIC"]
    replay = args.start is not None
    consumer = Consumer(
        {
            "bootstrap.servers": os.environ["FEATURE_KAFKA_BROKERS"],
            "group.id": f"feast-replay-{args.to}-{os.environ.get('RUN_ID', 'bounded')}"
            if replay
            else f"feast-push-{args.to}",
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
    partitions = sorted(consumer.list_topics(topic, timeout=30).topics[topic].partitions)
    writer = None
    feature_store = None
    if args.to == "offline":
        lake = catalog()
        writer = OfflineWriter(
            {scope: lake.load_table(name) for scope, name in TABLES.items()}, epoch
        )
    else:
        feature_store = store(online=True)
    if replay:
        starts = resolve_range(consumer, topic, args.start, partitions)
        ends = resolve_range(consumer, topic, args.until, partitions)
        if writer:
            writer.validate_replay(starts, ends)
    elif writer:
        starts, ends = writer.resume(partitions), None
    else:
        committed = consumer.committed([TopicPartition(topic, p) for p in partitions], timeout=30)
        starts = {
            p.partition: p.offset
            if p.offset >= 0
            else consumer.get_watermark_offsets(p, timeout=30)[0]
            for p in committed
        }
        ends = None
    retained(consumer, topic, starts, ends)
    files = {scope: [] for scope in KEYS}
    written = 0
    try:
        with tempfile.TemporaryDirectory() as directory:
            for messages, offsets in batches(consumer, topic, starts, ends, stopped):
                rows = {scope: [] for scope in KEYS}
                for message in messages:
                    scope, row, _ = decode(message, epoch)
                    rows[scope].append(row)
                if writer and replay:
                    for scope, values in rows.items():
                        if values:
                            path = Path(directory) / f"{scope}-{len(files[scope])}.parquet"
                            pq.write_table(
                                pa.Table.from_pylist(
                                    values, schema=stream_schema(scope).as_arrow()
                                ),
                                path,
                            )
                            files[scope].append(path)
                elif writer:
                    writer.append(rows, offsets)
                else:
                    for scope in KEYS:
                        write_online(
                            feature_store,
                            f"{scope}_stream",
                            rows[scope],
                            STREAM_FIELDS[scope],
                            push=True,
                        )
                if not replay:
                    consumer.commit(
                        offsets=[TopicPartition(topic, p, offset) for p, offset in offsets.items()],
                        asynchronous=False,
                    )
                written += len(messages)
            if writer and replay:
                retained(consumer, topic, starts, ends)
                writer.replay(files, starts, ends)
        if os.environ.get("RUN_REPORT"):
            write_json(
                os.environ["RUN_REPORT"],
                {"to": args.to, "from": starts, "until": ends, "rows": written},
            )
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
