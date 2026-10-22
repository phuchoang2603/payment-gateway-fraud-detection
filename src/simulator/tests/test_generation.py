from datetime import date

from simulator.generation import build_history
from simulator.profiles import (
    CARD_TESTING,
    HISTORY_STAGE,
    STOLEN_CARD_NEW_DEVICE,
    build_profiles,
    compromised_on,
    schedule_for_task,
    stage_rng,
    tasks_for_day,
)
from tests.conftest import small_config


def _histories(config, day):
    profiles = build_profiles(config)
    schedules, histories = [], []
    for task in tasks_for_day(config, (day - config.start_date).days):
        schedule = schedule_for_task(config, task, profiles)
        schedules.append(schedule)
        for index in range(len(schedule)):
            random = stage_rng(config.seed, HISTORY_STAGE, task.offset, task.kind, task.block, index)
            histories.append(build_history(config, profiles, schedule, index, day, task.ordinal(index), random))
    import numpy as np

    from simulator.profiles import Schedule

    return Schedule(
        *(
            np.concatenate([getattr(s, name) for s in schedules])
            for name in ("customers", "merchants", "seconds", "scenarios", "groups")
        )
    ), histories


def test_profiles_are_reproducible_from_the_seed():
    assert build_profiles(small_config()).digest() == build_profiles(small_config()).digest()
    assert build_profiles(small_config()).digest() != build_profiles(small_config(seed=8)).digest()


def test_compromise_windows_started_earlier_continue_on_a_rebuilt_day():
    config = small_config(**{"scenarios.stolen_card_new_device.rate": 0.2})
    profiles = build_profiles(config)
    started = set(compromised_on(config, STOLEN_CARD_NEW_DEVICE, 0).tolist())
    task = next(tasks_for_day(config, 2))
    schedule = schedule_for_task(config, task, profiles)
    fraud = schedule.scenarios == STOLEN_CARD_NEW_DEVICE
    from_day_zero = set(schedule.customers[fraud & (schedule.groups == 0)].tolist())
    assert from_day_zero and from_day_zero <= started
    assert not (schedule.groups[fraud] < 0).any()


def test_card_testing_bursts_share_a_device_across_many_low_amount_methods():
    config = small_config(**{"scenarios.card_testing.rate": 3})
    schedule, histories = _histories(config, date(2026, 7, 2))
    bursts = {}
    for index, history in enumerate(histories):
        if schedule.scenarios[index] != CARD_TESTING:
            continue
        created = history.events[0]["payment_created"]["snapshot"]
        signals = history.events[1]["attempt_started"]["signals"]
        assert created["total"]["minor_units"] <= 500
        bursts.setdefault(int(schedule.groups[index]), []).append(signals)
    assert bursts
    for signals in bursts.values():
        assert len({signal["device_fingerprint"] for signal in signals}) == 1
        assert len({signal["payment_method"]["fingerprint"] for signal in signals}) == len(signals)
        span = max(signal["attempted_at"] for signal in signals) - min(signal["attempted_at"] for signal in signals)
        assert span.total_seconds() <= config.scenarios.card_testing.burst_minutes * 60 + 5


def test_disabled_scenarios_never_appear_in_labels():
    config = small_config(
        **{
            "scenarios.card_testing.rate": 0,
            "scenarios.account_takeover.rate": 0,
            "scenarios.friendly_fraud.rate": 0,
        }
    )
    _, histories = _histories(config, date(2026, 7, 3))
    scenarios = {label["scenario"] for history in histories for label in history.labels}
    assert not scenarios & {"card_testing", "account_takeover", "friendly_fraud"}


def test_memory_mapped_profiles_are_read_only_distinct_and_rebuild_in_another_process(tmp_path):
    import subprocess
    import sys

    import numpy as np

    config = small_config()
    profiles = build_profiles(config, tmp_path)
    assert isinstance(profiles.familiar_merchants, np.memmap)
    assert not profiles.familiar_merchants.flags.writeable
    assert all(len(set(row)) == len(row) for row in profiles.familiar_merchants)
    assert build_profiles(config, tmp_path, reopen=True).digest() == profiles.digest()
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "from simulator.profiles import build_profiles; "
            "from tests.conftest import small_config; print(build_profiles(small_config()).digest())",
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    assert result.stdout.strip() == profiles.digest()


def test_continuation_refuses_unsupported_algorithm_and_profile_mismatch():
    import pytest

    from simulator.config import ALGORITHM_VERSION
    from simulator.profiles import profiles_from_manifest

    manifest = {
        "algorithm_version": ALGORITHM_VERSION,
        "configuration": small_config().model_dump(mode="json"),
        "profile_sha256": build_profiles(small_config()).digest(),
    }
    assert profiles_from_manifest(manifest).digest() == manifest["profile_sha256"]
    with pytest.raises(ValueError, match="unsupported"):
        profiles_from_manifest({**manifest, "algorithm_version": "old"})
    with pytest.raises(ValueError, match="digest mismatch"):
        profiles_from_manifest({**manifest, "profile_sha256": "changed"})
