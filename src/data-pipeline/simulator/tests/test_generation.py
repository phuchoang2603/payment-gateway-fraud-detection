import json
import shutil
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path

import pyarrow.parquet as pq
import pytest
import yaml
from pydantic import ValidationError

import simulator.historical as historical
from simulator.historical import (
    ConfigurationChanged,
    GenerationError,
    GeneratorConfig,
    SourceTooSmall,
    VersionPublished,
    generate,
    produce,
    publish,
)

SMALL_GENERATOR = Path(__file__).with_name("small-generator.yaml")
CONTRACTS = Path(__file__).resolve().parents[4] / "contracts/data-products/historical-payments"


def config_for(output: Path, **overrides) -> GeneratorConfig:
    data = yaml.safe_load(SMALL_GENERATOR.read_text())
    data["output"] = str(output)
    for path, value in overrides.items():
        target = data
        *parents, leaf = path.split(".")
        for parent in parents:
            target = target.setdefault(parent, {})
        target[leaf] = value
    return GeneratorConfig.model_validate(data)


def run(config: GeneratorConfig) -> dict:
    return generate(config, CONTRACTS)


def dataset_of(root: Path) -> Path:
    return root / "historical" / "smoke"


def files_under(directory: Path) -> dict[str, bytes]:
    return {
        path.relative_to(directory).as_posix(): path.read_bytes()
        for path in sorted(directory.rglob("*"))
        if path.is_file()
    }


def read_rows(dataset: Path, table: str) -> list[dict]:
    rows: list[dict] = []
    for path in sorted((dataset / table).rglob("*.parquet")):
        rows.extend(pq.read_table(path).to_pylist())
    return rows


def records(dataset: Path) -> dict[str, list[str]]:
    result: dict[str, list[str]] = defaultdict(list)
    for table in ("payment_events", "fraud_labels"):
        for row in read_rows(dataset, table):
            result[table].append(json.dumps(row, sort_keys=True, default=str))
    return {table: sorted(rows) for table, rows in result.items()}


def test_worker_count_and_interrupted_day_do_not_change_the_dataset(tmp_path):
    single = run(config_for(tmp_path / "single"))

    parallel_root = tmp_path / "parallel"
    generate(config_for(parallel_root), CONTRACTS, workers=2)
    dataset = dataset_of(parallel_root)
    (dataset / "manifest.json").unlink()
    (dataset / "date=2026-07-03" / "_day.json").unlink()
    (dataset / "payment_events" / "date=2026-07-03" / "part-00000.v2.parquet").unlink()

    assert generate(config_for(parallel_root), CONTRACTS, workers=1) == single


def test_records_do_not_depend_on_the_split_or_worker_count(tmp_path, monkeypatch):
    default_root = tmp_path / "default"
    run(config_for(default_root))
    default = records(dataset_of(default_root))

    monkeypatch.setattr(historical, "PART_CUSTOMERS", 100)
    split_root = tmp_path / "split"
    run(config_for(split_root))
    monkeypatch.undo()

    pooled_root = tmp_path / "pooled"
    generate(config_for(pooled_root), CONTRACTS, workers=2)

    assert records(dataset_of(split_root)) == default
    assert records(dataset_of(pooled_root)) == default


def test_changed_configuration_is_refused_without_modifying_the_version(tmp_path):
    root = tmp_path / "out"
    with pytest.raises(SourceTooSmall):
        run(config_for(root, minimum_source_bytes=10**12))
    before = files_under(dataset_of(root))

    with pytest.raises(ConfigurationChanged):
        run(config_for(root, **{"world.seed": 8}))

    assert files_under(dataset_of(root)) == before


def test_changed_contract_is_refused_without_modifying_the_version(tmp_path):
    root = tmp_path / "out"
    with pytest.raises(SourceTooSmall):
        run(config_for(root, minimum_source_bytes=10**12))
    before = files_under(dataset_of(root))
    contracts = tmp_path / "contracts"
    shutil.copytree(CONTRACTS, contracts)
    product = contracts / "product.odps.yaml"
    product.write_text(product.read_text() + "\n# revised\n")

    with pytest.raises(ConfigurationChanged):
        publish(config_for(root), contracts)

    assert files_under(dataset_of(root)) == before


def test_published_version_is_refused_without_modifying_it(tmp_path):
    root = tmp_path / "out"
    run(config_for(root))
    before = files_under(dataset_of(root))

    with pytest.raises(VersionPublished):
        run(config_for(root))

    assert files_under(dataset_of(root)) == before


def test_undersized_source_reports_observed_and_required_bytes_without_manifest(tmp_path):
    root = tmp_path / "out"
    with pytest.raises(SourceTooSmall, match=r"physical bytes \d+ are below minimum_source_bytes 1000000000000"):
        run(config_for(root, minimum_source_bytes=10**12))

    assert not (dataset_of(root) / "manifest.json").exists()
    assert len(list(dataset_of(root).glob("date=*/_day.json"))) == 6


def test_schema_change_splits_versions_by_occurrence_time(tmp_path):
    root = tmp_path / "out"
    run(config_for(root))
    events = read_rows(dataset_of(root), "payment_events")
    change = datetime(2026, 7, 4, tzinfo=UTC)

    assert {row["schema_version"] for row in events} == {2, 3}
    for row in events:
        expected = 3 if row["occurred_at"] >= change else 2
        assert row["schema_version"] == expected
        risk = row["risk_decision_recorded"]
        if risk is not None:
            has_assessment_id = risk["assessment"].get("assessment_id") is not None
            assert has_assessment_id == (expected == 3)


def test_each_schema_version_has_a_parquet_schema_that_declares_its_fields(tmp_path):
    root = tmp_path / "out"
    manifest = run(config_for(root))
    events = [part for part in manifest["files"] if part["table"] == "payment_events"]

    def fields_of(version: int) -> tuple[set[str], set[str]]:
        part = next(p for p in events if p["schema_version"] == version and p["rows"] > 0)
        schema = pq.read_schema(dataset_of(root) / part["path"])
        assessment = {f.name for f in schema.field("risk_decision_recorded").type["assessment"].type}
        signals = {f.name for f in schema.field("attempt_started").type["signals"].type}
        return assessment, signals

    v2_assessment, v2_signals = fields_of(2)
    v3_assessment, v3_signals = fields_of(3)
    assert {"assessment_id", "evidence_sha256"}.isdisjoint(v2_assessment)
    assert {"assessment_id", "evidence_sha256"} <= v3_assessment
    assert "user_agent_family" not in v2_signals
    assert "user_agent_family" in v3_signals


def test_duplicate_exports_are_republished_with_a_later_recorded_at(tmp_path):
    root = tmp_path / "out"
    run(config_for(root))
    groups: dict[str, list[dict]] = defaultdict(list)
    for row in read_rows(dataset_of(root), "payment_events"):
        groups[row["event_id"]].append(row)

    duplicated = [group for group in groups.values() if len(group) > 1]
    assert duplicated
    for group in duplicated:
        original, *republished = sorted(group, key=lambda row: row["recorded_at"])
        content = {key: value for key, value in original.items() if key != "recorded_at"}
        for copy in republished:
            assert {key: value for key, value in copy.items() if key != "recorded_at"} == content
            assert copy["recorded_at"] > original["recorded_at"]


def test_parts_are_never_empty_and_rows_sit_on_their_side_of_the_change(tmp_path):
    root = tmp_path / "out"
    manifest = run(config_for(root))
    change = datetime(2026, 7, 4, tzinfo=UTC)

    assert all(part["rows"] > 0 for part in manifest["files"])
    for part in manifest["files"]:
        if part["table"] != "payment_events":
            continue
        rows = pq.read_table(dataset_of(root) / part["path"]).to_pylist()
        if part["path"].endswith(".v3.parquet"):
            assert all(row["occurred_at"] >= change for row in rows)
        else:
            assert all(row["occurred_at"] < change for row in rows)
    assert any(part["path"].endswith(".v3.parquet") for part in manifest["files"])


@pytest.mark.parametrize(
    "changes",
    [
        [
            {"effective_at": "2026-07-04T00:00:00Z", "version": 3},
            {"effective_at": "2026-07-03T00:00:00Z", "version": 4},
        ],
        [{"effective_at": "2026-07-04T00:00:00Z", "version": 2}],
    ],
)
def test_invalid_schema_schedule_is_rejected_before_any_output(tmp_path, changes):
    with pytest.raises(ValidationError):
        config_for(tmp_path / "out", schema_changes=changes)
    assert not (tmp_path / "out").exists()


def test_schema_version_the_contract_does_not_declare_is_refused_before_output(tmp_path):
    root = tmp_path / "out"
    config = config_for(root, schema_changes=[{"effective_at": "2026-07-04T00:00:00Z", "version": 4}])

    with pytest.raises(GenerationError, match="not declared by the payment-events contract"):
        run(config)
    assert not root.exists()


def test_manifest_records_identities_totals_and_the_file_list(tmp_path):
    root = tmp_path / "out"
    manifest = run(config_for(root))
    stored = json.loads((dataset_of(root) / "manifest.json").read_text())

    assert stored == manifest
    assert set(manifest) == {
        "dataset_version",
        "generator_digest",
        "configuration_sha256",
        "configuration",
        "world_digest",
        "contracts",
        "rows",
        "physical_bytes",
        "files",
    }
    assert manifest["physical_bytes"] == sum(part["bytes"] for part in manifest["files"])
    assert manifest["rows"]["payment_events"] == sum(
        part["rows"] for part in manifest["files"] if part["table"] == "payment_events"
    )


def test_disjoint_shards_then_publish_equal_a_single_run(tmp_path):
    single = run(config_for(tmp_path / "single"))
    sharded = tmp_path / "sharded"
    produce(config_for(sharded), CONTRACTS, shard=(0, 2))
    produce(config_for(sharded), CONTRACTS, shard=(1, 2))

    assert publish(config_for(sharded), CONTRACTS) == single


def test_publish_refuses_while_any_shard_is_missing(tmp_path):
    root = tmp_path / "out"
    produce(config_for(root), CONTRACTS, shard=(0, 2))

    with pytest.raises(GenerationError):
        publish(config_for(root), CONTRACTS)
    assert not (dataset_of(root) / "manifest.json").exists()
