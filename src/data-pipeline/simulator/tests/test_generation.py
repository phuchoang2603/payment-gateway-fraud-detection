from datetime import UTC, date, datetime
from hashlib import sha256
from uuid import UUID, uuid5

import pytest

from simulator.generation import build_history, synthetic_assessment_reference
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


def test_synthetic_assessment_reference_uses_declared_canonical_bytes():
    signals = {
        "attempted_at": datetime(2025, 7, 1, 12, 0, tzinfo=UTC),
        "payment_method": {"type": "card", "fingerprint": "safe"},
        "device_fingerprint": "device",
        "ip_country": "US",
        "ip_network_fingerprint": "network",
        "user_agent_family": "é",
    }
    assessment = {
        "outcome": "RISK_OUTCOME_APPROVE",
        "reason_codes": [],
        "policy_version": "rules-v1",
        "model_version": None,
        "feature_set_version": None,
        "risk_score": None,
    }
    reference, digest = synthetic_assessment_reference("integration", "payment", "attempt", signals, assessment)
    expected = (
        '{"assessment":{"feature_set_version":null,"model_version":null,"outcome":"RISK_OUTCOME_APPROVE",'
        '"policy_version":"rules-v1","reason_codes":[],"risk_score":null},"attempt_id":"attempt",'
        '"format":"synthetic-assessment-v1","integration_id":"integration","payment_id":"payment",'
        '"signals":{"attempted_at":"2025-07-01T12:00:00.000000+00:00","device_fingerprint":"device",'
        '"ip_country":"US","ip_network_fingerprint":"network","payment_method":{"fingerprint":"safe",'
        '"type":"card"},"user_agent_family":"é"}}'
    ).encode("utf-8")
    assert digest == sha256(expected).hexdigest()
    assert reference == str(
        uuid5(UUID("71088b85-53a3-5ed5-bf27-8ea372bf796d"), "synthetic-assessment-v1:integration:payment:attempt")
    )


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


@pytest.mark.parametrize("reuse", [0, 1])
def test_card_testing_bursts_configure_context_reuse_across_many_low_amount_methods(reuse):
    config = small_config(**{"scenarios.card_testing.rate": 3, "behavior.familiar_context_probability": reuse})
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
        assert (len({signal["device_fingerprint"] for signal in signals}) == 1) == bool(reuse)
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


def test_entity_births_remain_fixed_across_purchases_and_days():
    from datetime import timedelta

    births = {}
    config = small_config(**{"behavior.purchases_per_customer_day": 2})
    for offset in (0, 2):
        _, histories = _histories(config, config.start_date + timedelta(days=offset))
        for history in histories:
            created = history.events[0]["payment_created"]
            for entity, field in (
                ("customer", "customer_account_created_at"),
                ("merchant", "merchant_account_created_at"),
            ):
                key = entity, created[entity][f"{entity}_id"]
                birth = created["snapshot"][field]
                assert births.setdefault(key, birth) == birth
    assert births


def test_us_dst_uses_real_instants_without_inventing_spring_hour():
    from datetime import UTC, datetime

    from simulator.calibration import hourly_weights, zone

    before = datetime(2026, 3, 8, 6, 30, tzinfo=UTC)
    after = datetime(2026, 3, 8, 7, 30, tzinfo=UTC)
    assert (before.astimezone(zone(0)).hour, after.astimezone(zone(0)).hour) == (1, 3)
    assert before.astimezone(zone(4)).utcoffset() == after.astimezone(zone(4)).utcoffset()
    weights, _ = hourly_weights(before.date(), 0)
    assert weights[7] < weights[6]


def test_historical_risk_matches_gateway_rule_boundaries():
    from datetime import UTC, datetime, timedelta

    from simulator.generation import risk_reasons

    now = datetime(2026, 7, 1, tzinfo=UTC)
    created = now - timedelta(days=1)
    assert risk_reasons(200000, created, now, "US", None) == []
    assert risk_reasons(200001, created, now, "US", None) == ["HIGH_AMOUNT"]
    assert risk_reasons(200001, created, now, "US", "CA") == ["HIGH_AMOUNT", "NEW_ACCOUNT_GEO_MISMATCH"]
    assert risk_reasons(20001, now - timedelta(days=7), now, "US", "CA") == []
    assert risk_reasons(20000, created, now, "US", "CA") == []


def test_unreported_success_retains_oracle_truth_without_observed_negative():
    config = small_config(**{"label_delays.reporting_probability": 0, "scenarios.friendly_fraud.rate": 1})
    _, histories = _histories(config, config.start_date)
    assert any(history.diagnostics["unreported_eligible_dispute_payments"] for history in histories)
    for history in histories:
        assert all(label["source"] == "LABEL_SOURCE_SIMULATION_TRUTH" for label in history.labels)
        assert all(label["occurred_at"] <= label["label_available_at"] for label in history.labels)


def test_fraud_and_legitimate_contexts_use_the_same_pools_and_overlap():
    config = small_config(**{"scenarios.stolen_card_new_device.rate": 0.5})
    profiles = build_profiles(config)
    _, histories = _histories(config, config.start_date)
    device_pool, network_pool = set(profiles.device_pool), set(profiles.network_pool)
    amounts, contexts = {}, {}
    for history in histories:
        signals = history.events[1]["attempt_started"]["signals"]
        assert signals["device_fingerprint"] is None or signals["device_fingerprint"] in device_pool
        assert signals["ip_network_fingerprint"] is None or signals["ip_network_fingerprint"] in network_pool
        assert signals["ip_country"] is None
        amount = history.events[0]["payment_created"]["snapshot"]["total"]["minor_units"]
        amounts.setdefault(history.truth, []).append(amount)
        for key in history.diagnostics:
            if key.startswith("context."):
                contexts[key] = contexts.get(key, 0) + 1
    legitimate, stolen = amounts["legitimate"], amounts["stolen_card_new_device"]
    assert max(min(legitimate), min(stolen)) < min(max(legitimate), max(stolen))
    assert contexts["context.legitimate.new"] and contexts["context.stolen_card_new_device.familiar"]
