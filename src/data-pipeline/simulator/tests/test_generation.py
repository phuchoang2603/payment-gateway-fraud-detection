import json
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path

import pyarrow.parquet as pq
import pytest
import yaml
from pydantic import ValidationError

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
REVISION = "0123456789abcdef0123456789abcdef01234567"


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
    return generate(config, REVISION)


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


def test_worker_count_and_interrupted_day_do_not_change_the_dataset(tmp_path):
    single = run(config_for(tmp_path / "single"))

    parallel_root = tmp_path / "parallel"
    run(config_for(parallel_root, workers=2))
    dataset = dataset_of(parallel_root)
    (dataset / "manifest.json").unlink()
    (dataset / "date=2026-07-03" / "_day.json").unlink()
    (dataset / "payment_events" / "date=2026-07-03" / "part-00001.parquet").unlink()

    assert run(config_for(parallel_root, workers=1)) == single


def test_changed_configuration_is_refused_without_modifying_the_version(tmp_path):
    root = tmp_path / "out"
    with pytest.raises(SourceTooSmall):
        run(config_for(root, minimum_source_bytes=10**12))
    before = files_under(dataset_of(root))

    with pytest.raises(ConfigurationChanged):
        run(config_for(root, **{"world.seed": 8}))

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


def test_schema_change_splits_physical_versions_by_occurrence_time(tmp_path):
    root = tmp_path / "out"
    run(config_for(root))
    events = read_rows(dataset_of(root), "payment_events")
    change = datetime(2026, 7, 4, tzinfo=UTC)

    assert {row["physical_schema_version"] for row in events} == {3, 4}
    for row in events:
        expected = 4 if row["occurred_at"] >= change else 3
        assert row["physical_schema_version"] == expected
        risk = row["risk_decision_recorded"]
        if risk is not None:
            has_assessment_id = risk["assessment"]["assessment_id"] is not None
            assert has_assessment_id == (expected == 4)


def test_duplicate_exports_repeat_original_rows(tmp_path):
    root = tmp_path / "out"
    run(config_for(root))
    groups: dict[str, list[dict]] = defaultdict(list)
    for row in read_rows(dataset_of(root), "payment_events"):
        groups[row["event_id"]].append(row)

    duplicated = [group for group in groups.values() if len(group) > 1]
    assert duplicated
    assert all(all(copy == group[0] for copy in group) for group in duplicated)


@pytest.mark.parametrize(
    "changes",
    [
        [
            {"effective_at": "2026-07-04T00:00:00Z", "version": 4},
            {"effective_at": "2026-07-03T00:00:00Z", "version": 4},
        ],
        [{"effective_at": "2026-07-04T00:00:00Z", "version": 5}],
    ],
)
def test_invalid_schema_schedule_is_rejected_before_any_output(tmp_path, changes):
    with pytest.raises(ValidationError):
        config_for(tmp_path / "out", schema_changes=changes)
    assert not (tmp_path / "out").exists()


def test_generation_requires_a_git_commit_revision(tmp_path):
    with pytest.raises(GenerationError):
        generate(config_for(tmp_path / "out"), "dev")
    assert not (tmp_path / "out").exists()


def test_manifest_records_both_physical_versions_and_identities(tmp_path):
    root = tmp_path / "out"
    manifest = run(config_for(root))
    stored = json.loads((dataset_of(root) / "manifest.json").read_text())

    assert stored == manifest
    assert manifest["generator_revision"] == REVISION
    assert manifest["physical_schema_versions"] == [3, 4]
    assert manifest["physical_bytes"] == sum(part["bytes"] for part in manifest["files"])
    assert manifest["rows"]["payment_events"] == sum(
        part["rows"] for part in manifest["files"] if part["table"] == "payment_events"
    )


def test_disjoint_shards_then_publish_equal_a_single_run(tmp_path):
    single = run(config_for(tmp_path / "single"))
    sharded = tmp_path / "sharded"
    produce(config_for(sharded), REVISION, shard=(0, 2))
    produce(config_for(sharded), REVISION, shard=(1, 2))

    assert publish(config_for(sharded), REVISION) == single


def test_publish_refuses_while_any_shard_is_missing(tmp_path):
    root = tmp_path / "out"
    produce(config_for(root), REVISION, shard=(0, 2))

    with pytest.raises(GenerationError):
        publish(config_for(root), REVISION)
    assert not (dataset_of(root) / "manifest.json").exists()
