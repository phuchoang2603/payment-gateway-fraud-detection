from copy import deepcopy
from datetime import timedelta

import pyarrow as pa
import pytest
from test_stream_writer import DAY, row

from fraud_feast import history
from fraud_feast.apply import stream_schema
from fraud_feast.repo import KEYS, TABLES
from fraud_feast.stream_writer import HistoryWriter, watermarks


def test_history_replaces_only_its_unit_atomically_and_retries_from_zero(lake):
    for scope, name in TABLES.items():
        table = lake.load_table(name)
        table.append(
            pa.Table.from_pylist(
                [
                    row(scope),
                    row(scope, epoch="history-ci"),
                    row(scope, at=DAY - timedelta(days=1), epoch="history-ci"),
                ],
                schema=stream_schema(scope).as_arrow(),
            )
        )
    for _ in range(2):
        tables = {scope: lake.load_table(name) for scope, name in TABLES.items()}
        before = {scope: table.scan().to_arrow() for scope, table in tables.items()}
        writer = HistoryWriter(tables, "history-ci", DAY, DAY + timedelta(days=30))
        writer.append({scope: [row(scope, value=2, epoch="history-ci")] for scope in KEYS})
        writer.append(
            {scope: [row(scope, offset=1, value=3, epoch="history-ci")] for scope in KEYS}
        )
        for scope, name in TABLES.items():
            assert lake.load_table(name).scan().to_arrow().equals(before[scope])
        writer.commit()
        for scope, table in tables.items():
            assert table.scan().to_arrow().num_rows == 4
            assert watermarks(table, "history-ci") == {}
    abandoned = HistoryWriter(tables, "history-ci", DAY, DAY + timedelta(days=30))
    abandoned.append({scope: [row(scope, epoch="history-ci")] for scope in KEYS})
    assert all(lake.load_table(name).scan().to_arrow().num_rows == 4 for name in TABLES.values())


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
