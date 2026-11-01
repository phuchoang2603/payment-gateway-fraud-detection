from copy import deepcopy
from datetime import timedelta

import pyarrow as pa
import pytest
from test_stream_writer import DAY, row
from test_stream_writer import writer as live_writer

from fraud_feast import history
from fraud_feast.apply import stream_schema
from fraud_feast.repo import KEYS, TABLES
from fraud_feast.stream_writer import HistoryWriter, watermarks


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
    history_writer = HistoryWriter(tables, "history-ci", DAY, DAY + timedelta(days=30))
    stage[0] = "append"
    history_writer.append({scope: [row(scope, epoch="history-ci")] for scope in KEYS})
    live_writer(lake).append({scope: [row(scope, offset=3)] for scope in KEYS}, {0: 4})
    history_writer.append({scope: [row(scope, offset=1, epoch="history-ci")] for scope in KEYS})
    assert delays == [0.1, 0.1]
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
    history_writer = HistoryWriter(tables, "history-ci", DAY, DAY + timedelta(days=30))
    first = {scope: [row(scope, epoch="history-ci")] for scope in KEYS}
    second = {scope: [row(scope, offset=1, epoch="history-ci")] for scope in KEYS}
    history_writer.append(first)
    attempts = []

    def failed_commit():
        attempts.append(True)
        raise CommitFailedException("maintenance conflict")

    monkeypatch.setattr(tables["payment_method"], "transaction", failed_commit)
    with pytest.raises(CommitFailedException, match="maintenance conflict"):
        history_writer.append(second)
    assert len(attempts) == 4
    assert month_rows(lake.load_table(TABLES["customer"])) == [(0, 1), (1, 2)]
    assert month_rows(lake.load_table(TABLES["payment_method"])) == [(0, 1)]

    tables = {scope: lake.load_table(name) for scope, name in TABLES.items()}
    rerun = HistoryWriter(tables, "history-ci", DAY, DAY + timedelta(days=30))
    rerun.append(first)
    rerun.append(second)
    for table in tables.values():
        assert month_rows(table) == [(0, 1), (1, 2)]
        records = table.scan().to_arrow().to_pylist()
        assert len(records) == 4
        assert sum(record["epoch"] == "live-ci" for record in records) == 1
        assert sum(record["event_timestamp"] < DAY for record in records) == 1
        assert watermarks(table, "history-ci") == {}


def test_gate_requires_decode_success_and_all_input_not_reject_count_equality():
    report = {
        "seed": {"done": True, "input_end": 10, "clock_end": 5},
        "consumed": {"input": 10, "clock": 5},
        "counts": {"decode_rejects": 0, "duplicates": 12, "too_late": 8, "rejects": 20},
    }
    history.validate_gate(report)
    for broken in ("decode", "input", "clock", "seed"):
        changed = deepcopy(report)
        if broken == "decode":
            changed["counts"]["decode_rejects"] = 1
        elif broken == "seed":
            changed["seed"]["done"] = False
        else:
            changed["consumed"][broken] -= 1
        with pytest.raises(ValueError):
            history.validate_gate(changed)
