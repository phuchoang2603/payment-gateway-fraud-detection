from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pyarrow as pa
import pytest

from fraud_feast.contracts.features.v2.features_pb2 import SnapshotUpdate
from fraud_feast.repo import KEYS, STREAM_FIELDS, TABLES
from fraud_feast.stream_writer import (
    DEFINITION_HASH,
    IcebergWriter,
    StreamWriter,
    batches,
    decode,
    write_online,
)

DAY = datetime(2025, 9, 1, tzinfo=UTC)


@pytest.mark.parametrize("scope,view", [("customer", 1), ("payment_method", 2)])
def test_shared_row_mapping_delays_visibility_until_after_online_write(scope, view):
    update = SnapshotUpdate(
        schema_version=1,
        definition={"version": "fraud-snapshot-v2", "sha256": DEFINITION_HASH},
        epoch="live-ci",
        view=view,
        integration_id="i",
        entity_id="entity",
        revision=1,
        coverage={"five_minutes": 1, "one_hour": 1, "twenty_four_hours": 1},
        values={},
    )
    for name in ("anchor", "source_available_at", "online_write_at"):
        getattr(update, name).FromDatetime(DAY)
    message = SimpleNamespace(
        value=lambda: update.SerializeToString(deterministic=True),
        timestamp=lambda: (1, int(DAY.timestamp() * 1000)),
        partition=lambda: 0,
        offset=lambda: 0,
    )
    mapped_scope, mapped, original = decode(message, "live-ci")
    assert mapped_scope == scope
    assert mapped["event_timestamp"] == DAY + timedelta(microseconds=1)
    assert mapped["created"] == DAY
    assert original.online_write_at.ToDatetime(tzinfo=UTC) == DAY
    assert all(mapped[field] is None for field in STREAM_FIELDS[scope])


def row(scope, offset=0, *, at=DAY, value=1, epoch="live-ci"):
    return {
        "integration_id": "i",
        KEYS[scope]: "entity",
        "event_timestamp": at,
        "created": DAY,
        "epoch": epoch,
        "revision": offset + 1,
        "_kafka_partition": 0,
        "_kafka_offset": offset,
        **dict.fromkeys(STREAM_FIELDS[scope], value),
    }


def writer(lake):
    return IcebergWriter(
        {scope: lake.load_table(name) for scope, name in TABLES.items()}, "live-ci"
    )


def test_live_push_buffers_and_restarts_from_partial_iceberg_commit(
    lake, feature_store, monkeypatch
):
    from fraud_feast import stream_writer

    clock = [0]
    monkeypatch.setattr(stream_writer.time, "monotonic", lambda: clock[0])
    offline = writer(lake)
    live = StreamWriter(offline, feature_store)
    rows = {scope: [row(scope)] for scope in KEYS}
    assert not live.write(rows, {0: 1})
    assert all(table.current_snapshot() is None for table in offline.tables.values())
    result = feature_store.get_online_features(
        features=["customer_stream:customer_snapshot_attempt_count_5m"],
        entity_rows=[{"integration_id": "i", "customer_id": "entity"}],
    ).to_dict()
    assert result["customer_snapshot_attempt_count_5m"] == [1]
    table = offline.tables["payment_method"]
    transaction = table.transaction

    def failed_commit():
        raise RuntimeError("offline commit failed")

    monkeypatch.setattr(table, "transaction", failed_commit)
    clock[0] = 300
    with pytest.raises(RuntimeError, match="offline commit"):
        live.write({scope: [] for scope in KEYS}, {0: 1})
    monkeypatch.setattr(table, "transaction", transaction)
    resumed = writer(lake)
    assert resumed.resume([0]) == {0: 0}
    live = StreamWriter(resumed, feature_store, offline_rows=2)
    assert live.write(rows, {0: 1})
    assert resumed.resume([0]) == {0: 1}
    for table in resumed.tables.values():
        assert table.scan().to_arrow().num_rows == 1
    assert live.write(rows, {0: 1})
    assert all(table.scan().to_arrow().num_rows == 1 for table in resumed.tables.values())


def test_empty_view_advances_watermark(lake):
    job = writer(lake)
    job.append({"customer": [row("customer")], "payment_method": []}, {0: 1})
    assert job.resume([0]) == {0: 1}


def test_older_online_revision_is_ignored_and_new_epoch_clears_old_values(feature_store):
    scope = "customer"
    write_online(
        feature_store,
        "customer_stream",
        [row(scope, at=DAY + timedelta(minutes=1), value=2)],
        STREAM_FIELDS[scope],
    )
    write_online(feature_store, "customer_stream", [row(scope, value=1)], STREAM_FIELDS[scope])
    entities = [{"integration_id": "i", "customer_id": "entity"}]
    features = [f"customer_stream:{field}" for field in STREAM_FIELDS[scope]]
    result = feature_store.get_online_features(features=features, entity_rows=entities).to_dict()
    assert result[STREAM_FIELDS[scope][0]] == [2]
    write_online(
        feature_store,
        "customer_stream",
        [row(scope, at=DAY + timedelta(minutes=2), value=None, epoch="live-next")],
        STREAM_FIELDS[scope],
    )
    result = feature_store.get_online_features(features=features, entity_rows=entities).to_dict()
    assert result[STREAM_FIELDS[scope][0]] == [None]


def test_invalid_journal_stops_live_writer_without_iceberg_commit(lake, monkeypatch):
    from unittest.mock import Mock

    from fraud_feast import stream_writer

    consumer = Mock()
    consumer.list_topics.return_value.topics = {"journal": SimpleNamespace(partitions={0: None})}
    message = Mock()
    message.error.return_value = None
    message.partition.return_value = 0
    message.offset.return_value = 0
    message.value.return_value = b""
    consumer.poll.return_value = message
    monkeypatch.setattr(stream_writer, "Consumer", lambda _: consumer)
    monkeypatch.setattr(stream_writer, "catalog", lambda: lake)
    monkeypatch.setattr(stream_writer.signal, "signal", lambda *_: None)
    monkeypatch.setenv("FEATURE_EPOCH", "live-ci")
    monkeypatch.setenv("FEAST_STREAM_EPOCHS", "live-ci")
    monkeypatch.setenv("FEATURE_UPDATE_TOPIC", "journal")
    monkeypatch.setenv("FEATURE_KAFKA_BROKERS", "unused")
    monkeypatch.setattr(stream_writer, "store", lambda **_: None)
    monkeypatch.setattr("sys.argv", ["stream_writer"])
    with pytest.raises(ValueError, match="payload"):
        stream_writer.main()
    consumer.close.assert_called_once()
    assert all(lake.load_table(name).current_snapshot() is None for name in TABLES.values())


def test_live_resume_survives_expiring_offset_snapshot(lake):
    job = writer(lake)
    job.append({scope: [row(scope)] for scope in KEYS}, {0: 1})
    table = job.tables["customer"]
    snapshot_id = table.current_snapshot().snapshot_id
    table.append(
        pa.Table.from_pylist([row("customer", offset=1)], schema=table.schema().as_arrow())
    )
    table.maintenance.expire_snapshots().by_id(snapshot_id).commit()
    assert all(snapshot.snapshot_id != snapshot_id for snapshot in table.snapshots())
    assert writer(lake).resume([0]) == {0: 1}


def test_live_conflict_refreshes_offsets_and_deduplicates(lake, monkeypatch):
    from pyiceberg.exceptions import CommitFailedException
    from pyiceberg.table import Transaction

    job = writer(lake)
    commit = Transaction.commit_transaction
    conflicted = [False]

    def competing_commit(transaction):
        if not conflicted[0]:
            conflicted[0] = True
            competitor = writer(lake)
            competitor.append({scope: [row(scope)] for scope in KEYS}, {0: 1})
            raise CommitFailedException("concurrent append")
        return commit(transaction)

    monkeypatch.setattr(Transaction, "commit_transaction", competing_commit)
    job.append({scope: [row(scope), row(scope, 1)] for scope in KEYS}, {0: 2})
    assert job.resume([0]) == {0: 2}
    assert all(table.scan().to_arrow().num_rows == 2 for table in job.tables.values())


def test_history_reads_every_partition_to_committed_eof():
    import threading
    from unittest.mock import Mock

    from confluent_kafka import KafkaError

    consumer = Mock()
    record, first_eof, second_eof = Mock(), Mock(), Mock()
    record.error.return_value = None
    record.partition.return_value = 0
    record.offset.return_value = 0
    for partition, message in enumerate((first_eof, second_eof)):
        message.error.return_value.code.return_value = KafkaError._PARTITION_EOF
        message.partition.return_value = partition
        message.offset.return_value = 2
    consumer.poll.side_effect = [record, first_eof, second_eof]
    result = list(batches(consumer, "journal", {0: 0, 1: 0}, threading.Event(), history=True))
    assert result == [([record], {0: 2, 1: 2})]


def test_live_ignores_eof_until_stopped():
    import threading
    from unittest.mock import Mock

    from confluent_kafka import KafkaError

    consumer = Mock()
    stopped = threading.Event()
    eof = Mock()
    eof.error.return_value.code.return_value = KafkaError._PARTITION_EOF
    eof.partition.return_value = 0
    eof.offset.return_value = 0

    def poll(_):
        if consumer.poll.call_count > 1:
            stopped.set()
        return eof

    consumer.poll.side_effect = poll
    assert list(batches(consumer, "journal", {0: 0}, stopped)) == []
    assert consumer.poll.call_count == 2
    consumer.pause.assert_not_called()


def snapshot(view=1, **changes):
    update = SnapshotUpdate(
        schema_version=1,
        definition={"version": "fraud-snapshot-v2", "sha256": DEFINITION_HASH},
        epoch="live-ci",
        view=view,
        integration_id="i",
        entity_id="entity",
        revision=1,
        coverage={"five_minutes": 2, "one_hour": 2, "twenty_four_hours": 2},
        values={"attempt_count_5m": 1, "attempt_count_1h": 1, "attempt_count_24h": 1}
        | ({"distinct_devices_24h": 1} if view == 1 else {}),
    )
    for name in ("anchor", "source_available_at", "online_write_at"):
        getattr(update, name).FromDatetime(DAY)
    for change in changes.pop("apply", ()):
        change(update)
    return update


def message(update, timestamp=(1, 0)):
    raw = (
        update.SerializeToString(deterministic=True)
        if isinstance(update, SnapshotUpdate)
        else update
    )
    return SimpleNamespace(
        value=lambda: raw, timestamp=lambda: timestamp, partition=lambda: 0, offset=lambda: 0
    )


def later(field, seconds):
    return lambda update: getattr(update, field).FromDatetime(DAY + timedelta(seconds=seconds))


# Every rejection the writer guards, one partition each; a valid customer and method snapshot decode.
@pytest.mark.parametrize(
    ("record", "error"),
    [
        (b"", "payload size"),
        (b"x" * (128 * 1024 + 1), "payload size"),
        (snapshot(apply=[lambda u: setattr(u, "schema_version", 2)]), "identity"),
        (snapshot(apply=[lambda u: setattr(u, "epoch", "live-other")]), "identity"),
        (snapshot(apply=[lambda u: setattr(u, "view", 3)]), "identity"),
        (
            snapshot(apply=[lambda u: u.anchor.FromNanoseconds(u.anchor.ToNanoseconds() + 1)]),
            "microsecond",
        ),
        (snapshot(apply=[later("anchor", 1)]), "precedes anchor"),
        (snapshot(apply=[later("source_available_at", 1)]), "availability follows"),
        (snapshot(apply=[lambda u: setattr(u.coverage, "one_hour", 3)]), "coverage"),
        (snapshot(apply=[lambda u: setattr(u.coverage, "one_hour", 1)]), "conflicts with coverage"),
        (
            snapshot(view=2, apply=[lambda u: setattr(u.values, "distinct_devices_24h", 1)]),
            "customer values",
        ),
    ],
)
def test_decode_rejects_invalid_snapshots(record, error):
    with pytest.raises(ValueError, match=error):
        decode(message(record), "live-ci")


@pytest.mark.parametrize(
    ("timestamp", "valid"), [((1, 0), True), ((0, 0), False), ((1, -1), False)]
)
def test_decode_requires_a_record_timestamp(timestamp, valid):
    if valid:
        assert decode(message(snapshot(view=2), timestamp), "live-ci")[0] == "payment_method"
    else:
        with pytest.raises(ValueError, match="timestamp"):
            decode(message(snapshot(), timestamp), "live-ci")


def run_main(lake, monkeypatch, argv, epoch, messages, env=None):
    import threading
    from unittest.mock import Mock

    from fraud_feast import stream_writer

    consumer = Mock()
    consumer.list_topics.return_value.topics = {"journal": SimpleNamespace(partitions={0: None})}
    monkeypatch.setattr(stream_writer, "Consumer", lambda config: consumer)
    monkeypatch.setattr(stream_writer, "catalog", lambda: lake)
    monkeypatch.setattr(stream_writer.signal, "signal", lambda *_: None)
    monkeypatch.setattr(stream_writer, "store", lambda **_: None)
    monkeypatch.setattr(stream_writer, "write_online", lambda *args: None)
    monkeypatch.setattr(
        stream_writer,
        "batches",
        lambda consumer, topic, starts, stopped, history: iter([(messages, {0: len(messages)})]),
    )
    for name, value in {
        "FEATURE_EPOCH": epoch,
        "FEAST_STREAM_EPOCHS": "history-ci,live-ci",
        "FEATURE_UPDATE_TOPIC": "journal",
        "FEATURE_KAFKA_BROKERS": "kafka",
        **(env or {}),
    }.items():
        monkeypatch.setenv(name, value)
    monkeypatch.setattr("sys.argv", ["stream_writer", *argv])
    stream_writer.main()
    consumer.close.assert_called_once()
    return threading


def test_live_main_writes_and_reports(lake, monkeypatch, tmp_path):
    import json

    report = tmp_path / "live.json"
    record = snapshot()
    record.online_write_at.FromDatetime(DAY + timedelta(hours=1))
    run_main(lake, monkeypatch, [], "live-ci", [message(record)], {"RUN_REPORT": str(report)})
    assert json.loads(report.read_text()) == {"mode": "live", "from": {"0": 0}, "rows": 1}
    assert lake.load_table(TABLES["customer"]).scan().to_arrow().num_rows == 1


# Partitions of the writer's mode flags.
@pytest.mark.parametrize(
    ("argv", "epoch", "failure"),
    [
        (["--history"], "history-ci", SystemExit),
        (["--history-start", "2026-08-01"], "live-ci", SystemExit),
        (
            ["--history", "--history-start", "2025-09-01", "--history-end", "2025-09-02"],
            "live-ci",
            ValueError,
        ),
        ([], "history-ci", ValueError),
        ([], "unknown-ci", ValueError),
    ],
)
def test_main_rejects_mismatched_modes(lake, monkeypatch, argv, epoch, failure):
    with pytest.raises(failure):
        run_main(lake, monkeypatch, argv, epoch, [])


def test_history_main_publishes_staged_files_before_reporting(lake, monkeypatch, tmp_path):
    import json

    from fraud_feast.history import HistoryReplay

    def check(replay):
        replay.seed, replay.counts = {"done": True}, {"decode_rejects": 0}

    monkeypatch.setattr(HistoryReplay, "check", check)
    record = snapshot(apply=[lambda u: setattr(u, "epoch", "history-ci")])
    report = tmp_path / "history.json"
    run_main(
        lake,
        monkeypatch,
        ["--history", "--history-start", "2025-09-01", "--history-end", "2025-09-02"],
        "history-ci",
        [message(record)],
        {"RUN_REPORT": str(report)},
    )
    assert json.loads(report.read_text())["rows"] == 1
    table = lake.load_table(TABLES["customer"])
    assert table.scan().to_arrow().num_rows == 1
    assert len(table.snapshots()) == 1
    assert lake.load_table(TABLES["payment_method"]).current_snapshot() is None
