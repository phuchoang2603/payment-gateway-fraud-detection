import json
from datetime import date, datetime, timezone
from pathlib import Path

import duckdb
import pyarrow.fs as fs
import pyarrow.parquet as pq
import pytest
import yaml

import simulator.storage as storage
from simulator.arrow import LABEL_SCHEMA, event_schema
from simulator.generation import GeneratedHistory
from simulator.profiles import build_profiles, schedule_for_task, tasks_for_day
from tests.conftest import small_config

duckdb.execute("SET TimeZone = 'UTC'")


def _read(root: Path, table: str):
    return duckdb.sql(f"SELECT * FROM read_parquet('{root}/{table}/*/*.parquet', union_by_name = true)")


def _odcs_fields(properties: list[dict]) -> dict:
    fields = {}
    for prop in properties:
        children = prop.get("properties") or prop.get("items", {}).get("properties")
        fields[prop["name"]] = (prop.get("required", False), _odcs_fields(children) if children else None)
    return fields


def _arrow_fields(fields) -> dict:
    result = {}
    for field in fields:
        kind = field.type.value_type if hasattr(field.type, "value_type") else field.type
        nested = _arrow_fields(kind) if hasattr(kind, "num_fields") and kind.num_fields else None
        result[field.name] = (not field.nullable, nested)
    return result


def _strip_required(fields: dict | None) -> dict | None:
    return None if fields is None else {name: _strip_required(nested) for name, (_, nested) in fields.items()}


@pytest.mark.parametrize(("contract", "schema"), [("payment-events", event_schema(2)), ("fraud-labels", LABEL_SCHEMA)])
def test_arrow_schema_matches_contract(contract, schema):
    declared = _odcs_fields(
        yaml.safe_load((storage.PRODUCT_DIR / f"{contract}.odcs.yaml").read_text())["schema"][0]["properties"]
    )
    actual = _arrow_fields(schema)
    assert _strip_required(actual) == _strip_required(declared)
    assert {name: required for name, (required, _) in actual.items()} == {
        name: required for name, (required, _) in declared.items()
    }


def test_published_dataset_passes_both_contracts(dataset):
    root, manifest = dataset
    assert manifest["validation"] == {"payment-events": "passed", "fraud-labels": "passed"}
    assert (root / "manifest.json").exists()
    assert {path.name for path in (root / "validation").iterdir()} == {"payment-events.json", "fraud-labels.json"}


def test_rebuilt_profiles_match_the_manifest_digest(dataset):
    _, manifest = dataset
    assert build_profiles(small_config()).digest() == manifest["profile_sha256"]


def test_regeneration_is_byte_identical(dataset, tmp_path):
    root, manifest = dataset
    storage.generate(small_config(), fs.LocalFileSystem(), str(tmp_path))
    again = tmp_path / "historical" / "smoke"
    files = sorted(
        path.relative_to(root)
        for name in ("payment_events", "fraud_labels")
        for path in (root / name).rglob("*.parquet")
    )
    assert files == sorted(
        path.relative_to(again)
        for name in ("payment_events", "fraud_labels")
        for path in (again / name).rglob("*.parquet")
    )
    assert all((root / path).read_bytes() == (again / path).read_bytes() for path in files)


def test_existing_dataset_version_is_refused(dataset):
    root, _ = dataset
    with pytest.raises(FileExistsError, match="already published"):
        storage.generate(small_config(), fs.LocalFileSystem(), str(root.parents[1]))


def test_completed_publication_recovery_reuses_output(dataset, monkeypatch):
    root, expected = dataset
    original = (root / "manifest.json").read_bytes()

    def unexpected(*args):
        raise AssertionError("completed publication must not regenerate")

    monkeypatch.setattr(storage, "build_profiles", unexpected)
    actual = storage.generate(small_config(), fs.LocalFileSystem(), str(root.parents[1]), resume=True)
    assert actual == expected
    assert (root / "manifest.json").read_bytes() == original


def test_schema_cutover_is_physical(dataset):
    root, _ = dataset
    cutover = small_config().data_problems.schema_cutover_date
    for path in (root / "payment_events").rglob("*.parquet"):
        day = date.fromisoformat(path.parent.name.split("=")[1])
        signals = pq.ParquetFile(path).schema_arrow.field("attempt_started").type.field("signals").type
        assert (signals.get_field_index("user_agent_family") >= 0) == (day >= cutover)
    nulls = (
        _read(root, "payment_events")
        .filter("attempt_started IS NOT NULL")
        .aggregate(
            "min(occurred_at) FILTER (attempt_started.signals.user_agent_family IS NOT NULL) AS first_present, "
            "max(occurred_at) FILTER (attempt_started.signals.user_agent_family IS NULL) AS last_missing"
        )
        .fetchone()
    )
    assert nulls[1] < datetime.combine(cutover, datetime.min.time(), timezone.utc) <= nulls[0]


def test_duplicates_repeat_identical_events_beside_their_originals(dataset):
    root, manifest = dataset
    rows, distinct, conflicting = duckdb.sql(
        f"SELECT count(*), count(DISTINCT event_id), count(DISTINCT to_json(e)) "
        f"FROM read_parquet('{root}/payment_events/*/*.parquet', union_by_name = true) e"
    ).fetchone()
    assert distinct == conflicting
    assert (rows - distinct) / rows == pytest.approx(0.05, abs=0.02)
    assert manifest["observed"]["duplicate_events"] == rows - distinct


def test_partitions_are_sorted_single_files_with_time_pruning(dataset):
    root, _ = dataset
    for path in (root / "payment_events").rglob("*.parquet"):
        assert path.name == "part-0.parquet"
        times = pq.read_table(path, columns=["occurred_at"]).column(0).to_pylist()
        assert times == sorted(times)
        statistics = pq.ParquetFile(path).metadata.row_group(0).column(4).statistics
        assert statistics.has_min_max
    window = "occurred_at BETWEEN TIMESTAMPTZ '2026-07-02 06:00:00+00' AND TIMESTAMPTZ '2026-07-02 07:00:00+00'"
    pruned = duckdb.sql(
        f"SELECT event_id FROM read_parquet('{root}/payment_events/*/*.parquet', union_by_name = true) "
        f"WHERE {window} ORDER BY ALL"
    ).fetchall()
    full = [row for row in _read(root, "payment_events").order("event_id").fetchall()]
    assert pruned and len(pruned) == sum(
        datetime(2026, 7, 2, 6, tzinfo=timezone.utc) <= row[4] <= datetime(2026, 7, 2, 7, tzinfo=timezone.utc)
        for row in full
    )


def test_each_day_publishes_its_scheduled_payments(dataset):
    root, _ = dataset
    config = small_config()
    profiles = build_profiles(config)
    created = dict(
        _read(root, "payment_events")
        .filter("payment_created IS NOT NULL")
        .aggregate("CAST(occurred_at AS DATE) AS day, count(DISTINCT payment_id)")
        .fetchall()
    )
    for day, count in created.items():
        assert count == sum(
            len(schedule_for_task(config, task, profiles))
            for task in tasks_for_day(config, (day - config.start_date).days)
        )


def test_labels_are_separate_from_events(dataset):
    root, _ = dataset
    events = duckdb.sql(
        f"SELECT string_agg(to_json(e)::VARCHAR, '') "
        f"FROM read_parquet('{root}/payment_events/*/*.parquet', union_by_name = true) e"
    ).fetchone()[0]
    for scenario in ("stolen_card_new_device", "account_takeover", "card_testing", "friendly_fraud"):
        assert scenario not in events
    declined_fraud = duckdb.sql(
        f"""
        WITH labels AS (SELECT * FROM read_parquet('{root}/fraud_labels/*/*.parquet')),
        truth AS (SELECT * FROM labels WHERE source = 'LABEL_SOURCE_SIMULATION_TRUTH' AND is_fraud),
        succeeded AS (
            SELECT DISTINCT payment_succeeded.attempt_id AS attempt_id
            FROM read_parquet('{root}/payment_events/*/*.parquet', union_by_name = true)
            WHERE payment_succeeded IS NOT NULL
        )
        SELECT count(*), count(*) FILTER (EXISTS (
            SELECT 1 FROM labels c WHERE c.source = 'LABEL_SOURCE_CHARGEBACK' AND c.attempt_id = truth.attempt_id
        ))
        FROM truth WHERE attempt_id NOT IN (SELECT attempt_id FROM succeeded)
        """
    ).fetchone()
    assert declined_fraud[0] > 0 and declined_fraud[1] == 0


def test_contract_violation_blocks_publication(tmp_path, monkeypatch):
    build = storage.build_history

    def skip_risk_decision(*args, **kwargs):
        history = build(*args, **kwargs)
        events = tuple(event for event in history.events if event.get("risk_decision_recorded") is None)
        return GeneratedHistory(events, history.labels, history.truth)

    monkeypatch.setattr(storage, "build_history", skip_risk_decision)
    with pytest.raises(storage.ContractViolation, match="payment-events"):
        storage.generate(
            small_config(duration_days=2, **{"data_problems.schema_cutover_date": "2026-07-02"}),
            fs.LocalFileSystem(),
            str(tmp_path),
        )
    assert not (tmp_path / "historical" / "smoke" / "manifest.json").exists()
    assert (tmp_path / "historical" / "smoke" / "_run" / "prepared.json").exists()


def test_row_groups_respect_the_configured_size(tmp_path):
    config = small_config(
        duration_days=2,
        **{
            "data_problems.schema_cutover_date": "2026-07-02",
            "storage.row_group_rows": 2048,
            "counts.customers": 3000,
        },
    )
    manifest = storage.generate(config, fs.LocalFileSystem(), str(tmp_path))
    paths = list((tmp_path / "historical" / "smoke" / "payment_events").rglob("*.parquet"))
    groups = [
        pq.ParquetFile(path).metadata.row_group(index)
        for path in paths
        for index in range(pq.ParquetFile(path).metadata.num_row_groups)
    ]
    assert len(groups) > len(paths)
    assert max(group.num_rows for group in groups) <= 2048
    assert manifest["storage"]["payment_events"]["row_groups"] == len(groups)


def test_empty_schedule_publishes_an_empty_valid_dataset(tmp_path):
    config = small_config(
        **{
            "behavior.purchases_per_customer_day": 0.0001,
            "counts.customers": 1,
            "scenarios.obvious_high_value.rate": 0,
            "scenarios.stolen_card_new_device.rate": 0,
            "scenarios.account_takeover.rate": 0,
            "scenarios.card_testing.rate": 0,
        }
    )
    manifest = storage.generate(config, fs.LocalFileSystem(), str(tmp_path), allow_small_dataset=True)
    assert manifest["observed"]["payments"] == 0
    assert manifest["validation"] == {"payment-events": "passed", "fraud-labels": "passed"}
    assert manifest["storage"]["payment_events"]["files"] == 0
    assert (tmp_path / "historical" / "smoke" / "manifest.json").exists()


def test_configured_dp1_gate_publishes_with_a_recorded_reason(tmp_path, monkeypatch):
    config = small_config(dataset_version="unvalidated", accepted_version=True, validation_gate="dp1-spark-gx")

    def unexpected(*args):
        raise AssertionError("DP1 is the configured contract gate")

    monkeypatch.setattr(storage, "_validate", unexpected)
    manifest = storage.generate(config, fs.LocalFileSystem(), str(tmp_path))
    assert manifest["validation"] == {
        "result": "skipped",
        "gate": "dp1-spark-gx",
        "reason": "validation_gate: dp1-spark-gx; the downstream pipeline is the contract gate",
    }
    assert manifest["observed"]["payments"] > 0
    assert (tmp_path / "historical" / "unvalidated" / "manifest.json").exists()


def test_task_fragments_are_one_file_per_table_and_schema_version(dataset):
    root, _ = dataset
    for task in (root / "_run" / "fragments").iterdir():
        names = sorted(path.name for path in task.iterdir())
        assert names == [name for name in names if name.startswith(("payment_events-v", "fraud_labels-v"))]
        assert len(names) == len(set(names))
        assert len(names) <= 3


def test_publication_receipt_records_skipped_cleanup(dataset):
    root, _ = dataset
    publication = json.loads((root / "_run" / "publication.json").read_text())
    assert publication["cleanup"] == "skipped"
    assert publication["tagging"] == "skipped"
    assert (root / "_run" / "fragments").is_dir()


def test_cleanup_removes_intermediates_after_publication(tmp_path):
    config = small_config(dataset_version="cleanup")
    root = tmp_path / "output"
    storage.generate(config, fs.LocalFileSystem(), str(root), cleanup_intermediates=True)
    prefix = root / "historical" / "cleanup"
    publication = json.loads((prefix / "_run" / "publication.json").read_text())
    assert publication["cleanup"] == "removed"
    assert publication["objects"] > 0
    assert publication["bytes"] > 0
    assert (prefix / "manifest.json").exists()
    assert {path.name for path in (prefix / "_run").iterdir()} == {"locks", "publication.json"}


def test_publication_refuses_probe_scale_without_opt_in(tmp_path):
    config = small_config(dataset_version="guard", **{"counts.customers": 10})
    with pytest.raises(ValueError, match="publication refused"):
        storage.generate(config, fs.LocalFileSystem(), str(tmp_path))
    prefix = tmp_path / "historical" / "guard"
    assert not (prefix / "manifest.json").exists()
    assert (prefix / "_run" / "prepared.json").exists()


def test_publication_receipt_records_accepted_version_flag(tmp_path):
    config = small_config(dataset_version="unprotected", accepted_version=True)
    storage.generate(config, fs.LocalFileSystem(), str(tmp_path))
    publication = json.loads((tmp_path / "historical" / "unprotected" / "_run" / "publication.json").read_text())
    assert publication["accepted_version"] is True
    assert publication["tagging"] == "skipped"
