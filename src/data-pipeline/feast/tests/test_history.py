from copy import deepcopy
from datetime import timedelta

import pyarrow as pa
import pytest
from test_stream_writer import DAY, row

from fraud_feast import history
from fraud_feast.apply import stream_schema
from fraud_feast.repo import KEYS, TABLES
from fraud_feast.stream_writer import IcebergWriter, watermarks


def test_month_prepare_replaces_only_its_epoch_and_resets_journal_progress(lake, monkeypatch):
    monkeypatch.setattr(history, "catalog", lambda: lake)
    monkeypatch.setenv("FEATURE_EPOCH", "history-ci")
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
        history.prepare("2025-09-01", "2025-10-01")
        writer = IcebergWriter({s: lake.load_table(n) for s, n in TABLES.items()}, "history-ci")
        assert writer.resume([0]) == {0: 0}
        writer.append({s: [row(s, epoch="history-ci")] for s in KEYS}, {0: 1})
        for table in writer.tables.values():
            assert table.scan().to_arrow().num_rows == 3
            assert watermarks(table, "history-ci") == {0: 1}


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
