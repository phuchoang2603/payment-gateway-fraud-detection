import pyarrow.fs as fs
import pytest

from simulator import storage
from simulator.object_store import ObjectStore
from simulator.run_state import RunState
from tests.conftest import small_config


def test_cross_midnight_histories_and_labels_validate_with_controlled_connection(tmp_path):
    config = small_config(
        duration_days=2,
        **{
            "data_problems.schema_cutover_date": "2026-07-02",
            "behavior.unknown_outcome_rate": 1,
            "behavior.issuer_decline_rate": 0,
            "behavior.insufficient_funds_rate": 0,
            "behavior.unknown_resolution_minutes": 1500,
            "execution.duckdb_memory_limit_mb": 256,
            "execution.duckdb_threads": 2,
            "execution.duckdb_spill_bytes": 1024**3,
        },
    )
    manifest = storage.generate(config, fs.LocalFileSystem(), str(tmp_path))
    assert manifest["history_end"][:10] > "2026-07-02"
    assert max(manifest["partitions"]["fraud_labels"]) > "2026-07-02"
    assert manifest["validation"] == {"payment-events": "passed", "fraud-labels": "passed"}


@pytest.mark.parametrize("defect,contract", [("lifecycle", "payment-events"), ("linkage", "fraud-labels")])
def test_native_checks_use_caller_resources_and_reject_full_history_defects(dataset, tmp_path, defect, contract):
    root, manifest = dataset
    config = small_config(
        **{
            "execution.duckdb_memory_limit_mb": 256,
            "execution.duckdb_threads": 2,
            "execution.duckdb_spill_bytes": 12345678,
        }
    )
    source, reports = ObjectStore(str(root)), ObjectStore(str(tmp_path / "reports"))
    run = RunState(reports, "test", "profiles")
    with storage._connection(config, tmp_path, source) as connection:
        storage._views(connection, manifest["inventory"], source, tmp_path)
        if defect == "lifecycle":
            connection.execute(
                "CREATE TEMP TABLE invalid_events AS SELECT * FROM payment_events WHERE risk_decision_recorded IS NULL"
            )
            connection.execute("CREATE OR REPLACE VIEW payment_events AS SELECT * FROM invalid_events")
        else:
            connection.execute(
                "CREATE TEMP TABLE invalid_labels AS SELECT * REPLACE ('missing-attempt' AS attempt_id) FROM fraud_labels"
            )
            connection.execute("CREATE OR REPLACE VIEW fraud_labels AS SELECT * FROM invalid_labels")
        settings = connection.execute(
            "SELECT current_setting('memory_limit'), current_setting('threads'), "
            "current_setting('max_temp_directory_size'), current_setting('temp_directory')"
        ).fetchone()
        assert settings[1] == 2 and settings[2] == "11.7 MiB" and settings[3] == str(tmp_path / "spill")
        with pytest.raises(storage.ContractViolation, match=contract):
            storage._validate(connection, reports, run)
        assert (
            connection.execute(
                "SELECT current_setting('memory_limit'), current_setting('threads'), "
                "current_setting('max_temp_directory_size'), current_setting('temp_directory')"
            ).fetchone()
            == settings
        )
        assert connection.execute("SELECT 1").fetchone() == (1,)
        assert reports.json(f"validation/{contract}.json")["result"] != "passed"
    run.release()
