from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from simulator.arrow import ASSESSMENTS
from simulator.runtime import destination
from tests.conftest import small_config


def test_output_url_must_not_carry_credentials():
    with pytest.raises(ValueError, match="without credentials"):
        destination("s3://key:secret@platform-dev")


@pytest.mark.parametrize(
    "overrides",
    [
        {"execution.workers": 2, "execution.max_in_flight": 1},
        {"execution.task_scratch_bytes": 10**12},
        {"execution.transfer_concurrency": 2},
    ],
)
def test_inconsistent_execution_budgets_are_rejected(overrides):
    with pytest.raises(ValidationError):
        small_config(**overrides)


def test_generation_identity_separates_logical_inputs_from_execution():
    base = small_config().generation_identity({"contract": "v1"}, "revision")
    changed = small_config(
        output="/another/location",
        chunk_purchases=13,
        **{
            "execution.workers": 2,
            "execution.max_in_flight": 2,
            "execution.transfer_concurrency": 2,
        },
    )
    assert changed.generation_identity({"contract": "v1"}, "revision") == base
    assert small_config(seed=8).generation_identity({"contract": "v1"}, "revision") != base
    assert small_config().generation_identity({"contract": "v2"}, "revision") != base
    assert small_config().generation_identity({"contract": "v1"}, "another revision") != base


def test_schema_schedule_selects_by_occurrence_and_rejects_ambiguous_or_unknown_versions():
    boundary = "2026-07-03T00:00:00Z"
    schedule = small_config(
        schema_schedule={"initial_version": 3, "changes": [{"effective_at": boundary, "version": 4}]}
    ).schema_schedule
    assert schedule.version_at(datetime(2026, 7, 2, 23, 59, 59, tzinfo=UTC)) == 3
    assert schedule.version_at(datetime(2026, 7, 3, tzinfo=UTC)) == 4
    for changes in (
        [{"effective_at": boundary, "version": 5}],
        [{"effective_at": boundary, "version": 4}, {"effective_at": boundary, "version": 3}],
        [{"effective_at": "2026-07-03T00:00:00", "version": 4}],
    ):
        with pytest.raises(ValidationError):
            small_config(schema_schedule={"initial_version": 3, "changes": changes})


def test_schema_schedule_skips_an_intermediate_declared_version(monkeypatch):
    monkeypatch.setitem(ASSESSMENTS, 5, ASSESSMENTS[4])
    schedule = small_config(
        schema_schedule={
            "initial_version": 3,
            "changes": [{"effective_at": "2026-07-03T00:00:00Z", "version": 5}],
        }
    ).schema_schedule
    assert schedule.version_at(datetime(2026, 7, 2, tzinfo=UTC)) == 3
    assert schedule.version_at(datetime(2026, 7, 3, tzinfo=UTC)) == 5
