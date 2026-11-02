from datetime import timedelta

import pyarrow as pa
import pytest
from test_stream_writer import DAY, row
from test_stream_writer import writer as live_writer

from fraud_feast.apply import stream_schema
from fraud_feast.repo import KEYS, TABLES
from fraud_feast.stream_writer import IcebergWriter, StreamWriter, watermarks


def history_writer(tables, *, offline_rows=2, offline_interval=300):
    offline = IcebergWriter(tables, "history-ci", history=True)
    offline.delete_range(DAY, DAY + timedelta(days=30))
    return StreamWriter(offline, offline_rows=offline_rows, offline_interval=offline_interval)


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


def test_history_short_commits_retry_conflicts_and_allow_live_writes_between_batches(
    lake, monkeypatch
):
    from pyiceberg.table import Transaction

    from fraud_feast import stream_writer

    seed_tables(lake)
    commit = Transaction.commit_transaction
    stage = ["delete"]
    live_offset = [1]
    delays = []
    monkeypatch.setattr(stream_writer.time, "sleep", delays.append)

    def interleave(transaction):
        if stage[0]:
            stage[0] = None
            offset = live_offset[0]
            live_writer(lake).append(
                {scope: [row(scope, offset)] for scope in KEYS}, {0: offset + 1}
            )
            live_offset[0] += 1
        return commit(transaction)

    monkeypatch.setattr(Transaction, "commit_transaction", interleave)
    tables = {scope: lake.load_table(name) for scope, name in TABLES.items()}
    writer = history_writer(tables)
    stage[0] = "append"
    writer.write({scope: [row(scope, epoch="history-ci")] for scope in KEYS}, {0: 1})
    live_writer(lake).append({scope: [row(scope, offset=3)] for scope in KEYS}, {0: 4})
    writer.write({scope: [row(scope, offset=1, epoch="history-ci")] for scope in KEYS}, {0: 2})
    assert len(delays) == 2 and all(0.05 <= delay <= 0.15 for delay in delays)
    for table in tables.values():
        table.refresh()
        assert month_rows(table) == [(0, 1), (1, 2)]
        records = table.scan().to_arrow().to_pylist()
        assert sorted(
            record["_kafka_offset"] for record in records if record["epoch"] == "live-ci"
        ) == [0, 1, 2, 3]
        assert watermarks(table, "live-ci") == {0: 4}
        assert watermarks(table, "history-ci") == {}


def test_history_failed_unit_rerun_replaces_partial_month_without_duplicates(lake, monkeypatch):
    from pyiceberg.exceptions import CommitFailedException

    from fraud_feast import stream_writer

    seed_tables(lake)
    monkeypatch.setattr(stream_writer.time, "sleep", lambda _: None)
    tables = {scope: lake.load_table(name) for scope, name in TABLES.items()}
    writer = history_writer(tables)
    first = {scope: [row(scope, epoch="history-ci")] for scope in KEYS}
    second = {scope: [row(scope, offset=1, epoch="history-ci")] for scope in KEYS}
    writer.write(first, {0: 1})
    attempts = []

    def failed_commit():
        attempts.append(True)
        raise CommitFailedException("maintenance conflict")

    monkeypatch.setattr(tables["payment_method"], "transaction", failed_commit)
    with pytest.raises(CommitFailedException, match="maintenance conflict"):
        writer.write(second, {0: 2})
    assert len(attempts) == 8
    assert month_rows(lake.load_table(TABLES["customer"])) == [(0, 1), (1, 2)]
    assert month_rows(lake.load_table(TABLES["payment_method"])) == [(0, 1)]

    tables = {scope: lake.load_table(name) for scope, name in TABLES.items()}
    rerun = history_writer(tables)
    rerun.write(first, {0: 1})
    rerun.write(second, {0: 2})
    for table in tables.values():
        assert month_rows(table) == [(0, 1), (1, 2)]
        records = table.scan().to_arrow().to_pylist()
        assert len(records) == 4
        assert sum(record["epoch"] == "live-ci" for record in records) == 1
        assert sum(record["event_timestamp"] < DAY for record in records) == 1
        assert watermarks(table, "history-ci") == {}


@pytest.mark.parametrize("bound", ["rows", "interval"])
def test_history_buffers_kafka_batches_and_flushes_remainder(lake, monkeypatch, bound):
    from fraud_feast import stream_writer

    clock = [0]
    monkeypatch.setattr(stream_writer.time, "monotonic", lambda: clock[0])
    tables = {scope: lake.load_table(name) for scope, name in TABLES.items()}
    writer = history_writer(tables, offline_rows=4 if bound == "rows" else 100000)
    for offset in range(3):
        if offset == 1 and bound == "interval":
            clock[0] = 300
        flushed = writer.write(
            {scope: [row(scope, offset, epoch="history-ci")] for scope in KEYS},
            {0: offset + 1},
        )
        assert flushed == (offset == 1)
        for table in tables.values():
            assert len(table.snapshots()) == (0 if offset == 0 else 1)
    assert writer.flush()
    assert not writer.flush()
    for table in tables.values():
        assert len(table.snapshots()) == 2
        assert month_rows(table) == [(0, 1), (1, 2), (2, 3)]
        assert watermarks(table, "history-ci") == {}
