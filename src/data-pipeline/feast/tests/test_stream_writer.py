from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from fraud_feast.contracts.features.v2.features_pb2 import SnapshotUpdate
from fraud_feast.repo import KEYS, STREAM_FIELDS, TABLES
from fraud_feast.stream_writer import (
    DEFINITION_HASH,
    IcebergWriter,
    StreamWriter,
    decode,
    write_online,
)

DAY = datetime(2025, 9, 1, tzinfo=UTC)


@pytest.mark.parametrize("scope,view", [("customer", 1), ("payment_method", 2)])
def test_shared_row_mapping_delays_visibility_until_after_online_write(scope, view):
    import hashlib

    from fraud_streaming.model import entity_key

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
    progress = update.source_progress.add(
        topic="payments", partition=0, consumed_offset=0, observed_high_watermark=1
    )
    progress.observed_at.FromDatetime(DAY)
    update.snapshot_id = hashlib.sha256(update.SerializeToString(deterministic=True)).hexdigest()
    message = SimpleNamespace(
        value=lambda: update.SerializeToString(deterministic=True),
        key=lambda: entity_key(
            "live-ci", "customer" if view == 1 else "method", "i", "entity"
        ).encode(),
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


def test_empty_view_advances_watermark_and_invalid_record_has_no_commit(lake):
    job = writer(lake)
    job.append({"customer": [row("customer")], "payment_method": []}, {0: 1})
    assert job.resume([0]) == {0: 1}
    before = {scope: table.current_snapshot().snapshot_id for scope, table in job.tables.items()}
    message = SimpleNamespace(value=lambda: b"", key=lambda: b"bad")
    with pytest.raises(ValueError, match="payload"):
        scope, value, _ = decode(message, "live-ci")
        job.append({scope: [value]}, {0: 2})
    assert before == {
        scope: table.current_snapshot().snapshot_id for scope, table in job.tables.items()
    }


def test_older_online_revision_does_not_replace_newer_and_all_null_is_skipped(feature_store):
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
        [row(scope, at=DAY + timedelta(minutes=2), value=None)],
        STREAM_FIELDS[scope],
    )
    result = feature_store.get_online_features(features=features, entity_rows=entities).to_dict()
    assert result[STREAM_FIELDS[scope][0]] == [2]


def test_invalid_journal_stops_continuous_job_before_kafka_commit(lake, monkeypatch):
    from unittest.mock import Mock

    from fraud_feast import stream_writer

    consumer = Mock()
    consumer.list_topics.return_value.topics = {"journal": SimpleNamespace(partitions={0: None})}
    consumer.get_watermark_offsets.return_value = (0, 1)
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
    consumer.commit.assert_not_called()
    consumer.close.assert_called_once()
    assert all(lake.load_table(name).current_snapshot() is None for name in TABLES.values())
