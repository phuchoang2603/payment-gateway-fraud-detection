import pytest
from pydantic import ValidationError

from simulator.cli import destination
from tests.conftest import small_config


def test_unknown_scenario_is_rejected():
    with pytest.raises(ValidationError, match="device_farm"):
        small_config(**{"scenarios.device_farm": {"rate": 0.1}})


def test_credential_fields_are_rejected():
    with pytest.raises(ValidationError, match="aws_secret_access_key"):
        small_config(aws_secret_access_key="secret")


def test_cutover_must_fall_inside_the_dataset():
    with pytest.raises(ValidationError, match="schema_cutover_date"):
        small_config(**{"data_problems.schema_cutover_date": "2027-01-01"})


def test_output_url_must_not_carry_credentials():
    with pytest.raises(ValueError, match="without credentials"):
        destination("s3://key:secret@synthetic-source")


@pytest.mark.parametrize(
    "overrides",
    [
        {"execution.workers": 0},
        {"execution.workers": 2, "execution.max_in_flight": 1},
        {"execution.duckdb_spill_bytes": 10**12},
        {"execution.transfer_concurrency": 2},
        {"execution.aws_secret_access_key": "secret"},
        {"minimum_source_parquet_bytes": -1},
    ],
)
def test_invalid_execution_budgets_and_credentials_are_rejected(overrides):
    with pytest.raises(ValidationError):
        small_config(**overrides)


def test_generation_identity_separates_logical_inputs_from_execution():
    base = small_config().generation_identity({"contract": "v1"}, "revision")
    changed = small_config(
        output="/another/location",
        chunk_purchases=13,
        accepted_version=True,
        validation_gate="dp1-spark-gx",
        **{
            "execution.workers": 2,
            "execution.max_in_flight": 2,
            "execution.duckdb_memory_limit_mb": 1024,
            "acceptance_bands": {"duplicate_rate": {"minimum": 0.01, "maximum": 0.03}},
        },
    )
    assert changed.generation_identity({"contract": "v1"}, "revision") == base
    assert small_config(seed=8).generation_identity({"contract": "v1"}, "revision") != base
    assert small_config().generation_identity({"contract": "v2"}, "revision") != base
    assert small_config().generation_identity({"contract": "v1"}, "another revision") != base


def test_scale_pilot_uses_same_profiles_algorithm_and_layout_without_changing_v7():
    from pathlib import Path

    from simulator.config import load_config

    directory = Path(__file__).resolve().parents[1] / "configs"
    scale, pilot, legacy = (
        load_config(directory / name) for name in ("historical-scale.yaml", "historical-pilot.yaml", "historical.yaml")
    )
    assert legacy.dataset_version == "v7"
    assert pilot.duration_days >= 6
    assert pilot.counts == scale.counts and pilot.storage == scale.storage
    assert pilot.scenarios == scale.scenarios and pilot.label_delays == scale.label_delays
    assert pilot.customer_block_size == scale.customer_block_size
    assert pilot.dataset_version != scale.dataset_version != legacy.dataset_version
    assert scale.minimum_source_parquet_bytes == 100000000000
    assert pilot.minimum_source_parquet_bytes == 0
    assert scale.validation_gate == "dp1-spark-gx"
    assert pilot.validation_gate == legacy.validation_gate == "native"


def test_unknown_validation_gate_is_rejected():
    with pytest.raises(ValidationError, match="validation_gate"):
        small_config(validation_gate="none")


def test_acceptance_bands_are_validated_without_forming_logical_identity():
    from simulator.storage import _gate_observed

    band = {"minimum": 0.015, "maximum": 0.025}
    config = small_config(acceptance_bands={"duplicate_rate": band})
    _gate_observed(config, {"duplicate_rate": 0.02})
    with pytest.raises(ValueError, match="observed acceptance gate"):
        _gate_observed(config, {"duplicate_rate": 0.1})
    with pytest.raises(ValidationError, match="unknown observed"):
        small_config(acceptance_bands={"mistyped": band})
    assert config.generation_identity({}, "revision") == small_config().generation_identity({}, "revision")


def test_full_run_band_covers_compromise_windows_after_warmup():
    from datetime import timedelta
    from pathlib import Path

    import numpy as np

    from simulator.config import load_config
    from simulator.generation import build_history
    from simulator.profiles import (
        HISTORY_STAGE,
        STOLEN_CARD_NEW_DEVICE,
        build_profiles,
        schedule_for_task,
        stage_rng,
        tasks_for_day,
    )

    config = load_config(Path(__file__).resolve().parents[1] / "configs/historical-scale.yaml")
    profiles = build_profiles(config)
    totals = {}
    for offset in range(30, 36):
        task = next(tasks_for_day(config, offset))
        schedule = schedule_for_task(config, task, profiles)
        for kind in np.unique(schedule.scenarios):
            indices = np.flatnonzero(schedule.scenarios == kind)
            sample = indices[:250]
            attempts = 0
            for index in sample:
                index = int(index)
                history = build_history(
                    config,
                    profiles,
                    schedule,
                    index,
                    config.start_date + timedelta(days=offset),
                    task.ordinal(index),
                    stage_rng(config.seed, HISTORY_STAGE, task.offset, task.kind, task.block, index),
                )
                attempts += sum("attempt_started" in event for event in history.events)
            totals[int(kind)] = totals.get(int(kind), 0) + len(indices) * attempts / len(sample)
    observed = totals[STOLEN_CARD_NEW_DEVICE] / sum(totals.values())
    band = config.acceptance_bands["scenario_attempt_shares.stolen_card_new_device"]
    assert band.minimum <= observed <= band.maximum
