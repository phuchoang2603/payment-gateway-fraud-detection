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


def test_output_url_must_not_carry_credentials():
    with pytest.raises(ValueError, match="without credentials"):
        destination("s3://key:secret@synthetic-source")


@pytest.mark.parametrize(
    "overrides",
    [
        {"execution.workers": 0},
        {"execution.workers": 2, "execution.max_in_flight": 1},
        {"execution.task_scratch_bytes": 10**12},
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
