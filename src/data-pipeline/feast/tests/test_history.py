from datetime import timedelta

import pyarrow as pa
import pytest
from test_stream_writer import DAY, row
from test_stream_writer import writer as live_writer

from fraud_feast.apply import stream_schema
from fraud_feast.repo import KEYS, TABLES
from fraud_feast.stream_writer import HistoryWriter, StreamWriter, watermarks


def history_writer(lake, *, first=DAY, last=DAY + timedelta(days=30), **buffer):
    tables = {scope: lake.load_table(name) for scope, name in TABLES.items()}
    offline = HistoryWriter(tables, "history-ci", first, last)
    return StreamWriter(offline, **buffer)


def seed_tables(lake):
    for scope, name in TABLES.items():
        lake.load_table(name).append(
            pa.Table.from_pylist(
                [
                    row(scope),
                    row(scope, value=99, epoch="history-ci"),
                    row(scope, at=DAY - timedelta(days=1), epoch="history-ci"),
                ],
                schema=stream_schema(scope).as_arrow(),
            )
        )


def month_rows(table):
    return sorted(
        (record["_kafka_offset"], record["revision"])
        for record in table.scan().to_arrow().to_pylist()
        if record["epoch"] == "history-ci" and record["event_timestamp"] >= DAY
    )


def batch(offset=0, at=DAY):
    return {scope: [row(scope, offset, at=at, epoch="history-ci")] for scope in KEYS}


def test_history_stages_without_changing_visible_rows_and_reuses_daily_files(lake):
    seed_tables(lake)
    writer = history_writer(lake, offline_rows=1)
    before = {s: t.current_snapshot().snapshot_id for s, t in writer.offline.tables.items()}
    for offset in range(3):
        writer.write(batch(offset), {0: offset + 1})
    for scope, table in writer.offline.tables.items():
        assert table.refresh().current_snapshot().snapshot_id == before[scope]
        assert month_rows(table) == [(0, 1)]
        assert len(writer.offline.files[scope]) == 1
    writer.offline.publish()
    for scope, table in writer.offline.tables.items():
        assert month_rows(table) == [(0, 1), (1, 2), (2, 3)]
        assert table.scan().to_arrow().num_rows == 5
        assert watermarks(table, "history-ci") == {}


def test_conflicting_publish_reuses_files_and_preserves_concurrent_live_rows(lake, monkeypatch):
    from pyiceberg.table import Transaction

    from fraud_feast import stream_writer

    seed_tables(lake)
    writer = history_writer(lake, offline_rows=1)
    writer.write(batch(), {0: 1})
    paths = {scope: list(files) for scope, files in writer.offline.files.items()}
    commit = Transaction.commit_transaction
    competing = [False]
    delays = []
    monkeypatch.setattr(stream_writer.time, "sleep", delays.append)

    def interleave(transaction):
        if not competing[0]:
            competing[0] = True
            live_writer(lake).append({scope: [row(scope, offset=1)] for scope in KEYS}, {0: 2})
        return commit(transaction)

    monkeypatch.setattr(Transaction, "commit_transaction", interleave)
    writer.offline.publish()
    assert len(delays) == 1
    assert paths == writer.offline.files
    for table in writer.offline.tables.values():
        records = table.scan().to_arrow().to_pylist()
        assert month_rows(table) == [(0, 1)]
        assert sorted(r["_kafka_offset"] for r in records if r["epoch"] == "live-ci") == [0, 1]
        assert watermarks(table, "live-ci") == {0: 2}


def test_disjoint_history_units_publish_during_conflicting_commit(lake, monkeypatch):
    from pyiceberg.table import Transaction

    from fraud_feast import stream_writer

    monkeypatch.setattr(stream_writer.time, "sleep", lambda _: None)
    first = history_writer(lake, last=DAY + timedelta(days=1), offline_rows=1)
    second = history_writer(lake, first=DAY + timedelta(days=1), offline_rows=1)
    first.write(batch(), {0: 1})
    second.write(batch(1, DAY + timedelta(days=1)), {0: 2})
    commit = Transaction.commit_transaction
    competing = [False]

    def interleave(transaction):
        if not competing[0]:
            competing[0] = True
            second.offline.publish()
        return commit(transaction)

    monkeypatch.setattr(Transaction, "commit_transaction", interleave)
    first.offline.publish()
    for table in first.offline.tables.values():
        assert month_rows(table) == [(0, 1), (1, 2)]
        assert len(table.inspect.data_files()) == 2


def test_history_rerun_repairs_partial_two_table_publication_without_duplicates(lake, monkeypatch):
    from pyiceberg.exceptions import CommitFailedException

    from fraud_feast import stream_writer

    seed_tables(lake)
    monkeypatch.setattr(stream_writer.time, "sleep", lambda _: None)
    writer = history_writer(lake, offline_rows=1)
    writer.write(batch(), {0: 1})
    writer.write(batch(1), {0: 2})
    attempts = []

    def failed_commit():
        attempts.append(True)
        raise CommitFailedException("maintenance conflict")

    monkeypatch.setattr(writer.offline.tables["payment_method"], "transaction", failed_commit)
    with pytest.raises(CommitFailedException, match="maintenance conflict"):
        writer.offline.publish()
    assert len(attempts) == 8
    assert month_rows(lake.load_table(TABLES["customer"])) == [(0, 1), (1, 2)]
    assert month_rows(lake.load_table(TABLES["payment_method"])) == [(0, 1)]
    rerun = history_writer(lake, offline_rows=1)
    rerun.write(batch(), {0: 1})
    rerun.write(batch(1), {0: 2})
    rerun.offline.publish()
    for table in rerun.offline.tables.values():
        assert month_rows(table) == [(0, 1), (1, 2)]
        assert table.scan().to_arrow().num_rows == 4


@pytest.mark.parametrize("bound", ["rows", "interval"])
def test_history_buffers_and_stages_remainder_without_batch_commits(lake, monkeypatch, bound):
    from fraud_feast import stream_writer

    clock = [0]
    monkeypatch.setattr(stream_writer.time, "monotonic", lambda: clock[0])
    writer = history_writer(lake, offline_rows=4 if bound == "rows" else 100000)
    for offset in range(3):
        if offset == 1 and bound == "interval":
            clock[0] = 300
        assert writer.write(batch(offset), {0: offset + 1}) == (offset == 1)
        assert all(table.current_snapshot() is None for table in writer.offline.tables.values())
    assert writer.flush()
    assert not writer.flush()
    writer.offline.publish()
    for table in writer.offline.tables.values():
        assert len(table.snapshots()) == 1
        assert month_rows(table) == [(0, 1), (1, 2), (2, 3)]


def test_empty_history_unit_atomically_removes_old_range(lake):
    seed_tables(lake)
    writer = history_writer(lake)
    writer.offline.publish()
    for table in writer.offline.tables.values():
        assert month_rows(table) == []
        assert table.scan().to_arrow().num_rows == 2


def test_history_files_roll_at_target_size_and_keep_day_partitions(lake):
    writer = history_writer(lake, offline_rows=1)
    for table in writer.offline.tables.values():
        with table.transaction() as transaction:
            transaction.set_properties({"write.target-file-size-bytes": "1"})
    writer.write(batch(), {0: 1})
    writer.write(batch(1), {0: 2})
    writer.write(batch(2, DAY + timedelta(days=1)), {0: 3})
    writer.offline.publish()
    for scope, table in writer.offline.tables.items():
        assert len(writer.offline.files[scope]) == 3
        assert month_rows(table) == [(0, 1), (1, 2), (2, 3)]
        assert len(table.inspect.partitions()) == 2


@pytest.mark.parametrize("at", [DAY - timedelta(microseconds=1), DAY + timedelta(days=30)])
def test_staging_rejects_rows_outside_owned_range(lake, at):
    writer = history_writer(lake, offline_rows=1)
    with pytest.raises(ValueError, match="outside its replay range"):
        writer.write(batch(at=at), {0: 1})
    writer.offline.close()
    assert all(table.current_snapshot() is None for table in writer.offline.tables.values())
