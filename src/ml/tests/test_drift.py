"""Tests for drift: the reference profile, the population stability index, the drift state and the retrain rule."""

from datetime import datetime, timedelta, timezone

import numpy as np
from hypothesis import given
from hypothesis import strategies as st

from risk_model.data import Split
from risk_model.drift import (
    Policy,
    assess,
    decide,
    edges_of,
    index,
    load_policy,
    profile,
    weighted_edges,
)

NOW = datetime(2026, 11, 10, 12, tzinfo=timezone.utc)
SMALL = Policy(samples=100)


def make(seed, rows=400, shift=0.0, missing=0.1):
    """Returns a weighted two-feature Split, the second feature shifted by `shift` and partly missing."""
    generator = np.random.default_rng(seed)
    x = generator.normal(size=(rows, 2)).astype(np.float32)
    x[:, 1] += shift
    x[generator.random(rows) < missing, 1] = np.nan
    weights = generator.choice([1.0, 10.0], size=rows).astype(np.float32)
    keys = np.array([f"k{n}" for n in range(rows)], dtype=object)
    return Split(x, np.zeros(rows, dtype=np.float32), weights, keys, ["a", "b"])


def counts(reference, split):
    """Returns the bin counts of a Split under a profile, the way the fraud service counts them."""
    result = {}
    for index_, name in enumerate(split.features):
        edges = reference[name]["edges"]
        values = np.zeros(len(edges) + 2)
        for value in split.x[:, index_]:
            values[
                len(edges) + 1 if np.isnan(value) else np.searchsorted(edges, value, side="right")
            ] += 1
        result[name] = values.tolist()
    return result


@given(st.integers(0, 10_000), st.integers(50, 500), st.floats(0, 0.9))
def test_a_profile_has_sorted_distinct_edges_and_shares_that_sum_to_one(seed, rows, missing):
    """A profile has sorted distinct edges and shares that sum to one."""
    reference = profile(make(seed, rows, missing=missing))
    for entry in reference.values():
        assert entry["edges"] == sorted(set(entry["edges"]))
        assert len(entry["share"]) == len(entry["edges"]) + 2
        assert abs(sum(entry["share"]) - 1.0) < 1e-9


@given(st.integers(0, 10_000))
def test_a_sample_scored_against_its_own_profile_shows_no_drift(seed):
    """A sample scored against its own profile shows an index near zero for every feature."""
    split = make(seed, 600)
    reference = profile(split)
    result = assess(reference, counts(reference, split), SMALL)
    assert max(result["psi"].values()) < 0.2 and result["state"] == "ok"


@given(st.lists(st.integers(0, 50), min_size=2, max_size=12))
def test_the_index_is_never_negative(values):
    """The index is never negative, whatever the counts."""
    share = [1 / len(values)] * len(values)
    result = index(share, values)
    assert result is None or result >= 0


@given(st.integers(0, 10_000), st.floats(0.05, 0.5))
def test_raising_the_threshold_never_adds_a_breaching_feature(seed, step):
    """Raising the index threshold never adds a breaching feature."""
    reference = profile(make(seed))
    observed = counts(reference, make(seed + 1, shift=1.5))
    low = assess(reference, observed, Policy(samples=100, psi=0.1))
    high = assess(reference, observed, Policy(samples=100, psi=0.1 + step))
    assert set(high["over"]) <= set(low["over"])


def test_edges_are_distinct_values_of_the_feature_and_no_values_give_no_edges():
    """The edges are sorted, distinct values taken from the feature, and an empty feature has none."""
    values = np.array([1, 2, 3, 4, 5], dtype=np.float32)
    edges = weighted_edges(values, np.ones(5, dtype=np.float32))
    assert list(edges) == sorted(set(edges)) and set(edges) <= set(values.astype(np.float64))
    assert len(weighted_edges(np.array([], dtype=np.float32), np.array([], dtype=np.float32))) == 0


def test_a_constant_feature_has_one_edge_and_edges_of_lists_them_by_feature():
    """A constant feature has a single edge, and `edges_of` keeps only the edges by feature."""
    split = Split(
        np.ones((20, 1), dtype=np.float32),
        np.zeros(20, dtype=np.float32),
        np.ones(20, dtype=np.float32),
        np.array([str(n) for n in range(20)], dtype=object),
        ["only"],
    )
    reference = profile(split, {"only": 3.0})
    assert edges_of(reference) == {"only": [1.0]} and reference["only"]["gain"] == 3.0


def test_too_few_requests_are_insufficient_and_a_shift_is_drifting():
    """Too few requests are insufficient, and a clear shift is drifting with the shifted feature named."""
    reference = profile(make(1), {"a": 1.0, "b": 5.0})
    shifted = counts(reference, make(2, shift=3.0))
    assert assess(reference, shifted, Policy(samples=10_000))["state"] == "insufficient"
    result = assess(reference, shifted, SMALL)
    assert result["state"] == "drifting" and "b" in result["over"]
    assert (
        result["missing"]["b"] == shifted["b"][-1] / sum(shifted["b"]) - reference["b"]["share"][-1]
    )


def test_a_missing_share_jump_is_a_shift_in_the_missing_bin():
    """A jump in the share of missing values shows in the index of that feature."""
    reference = profile(make(3, missing=0.05))
    observed = counts(reference, make(4, missing=0.7))
    assert assess(reference, observed, SMALL)["psi"]["b"] > 0.2


def assessed(state):
    """Returns the part of an assessment `decide` reads."""
    return {"state": state}


def run(name, state, hours):
    """Returns an automatic run started `hours` before now."""
    return {"name": name, "state": state, "created": NOW - timedelta(hours=hours)}


def test_persistent_drift_starts_one_run_and_one_window_alone_does_not():
    """Drift in both windows starts a run, and drift in only one does not."""
    trained = NOW - timedelta(days=5)
    drifting, ok = assessed("drifting"), assessed("ok")
    assert decide(drifting, drifting, runs=[], trained=trained, now=NOW, policy=Policy()) == (
        True,
        "drift",
    )
    assert decide(drifting, ok, runs=[], trained=trained, now=NOW, policy=Policy()) == (
        False,
        "none",
    )
    assert (
        decide(
            assessed("insufficient"), drifting, runs=[], trained=trained, now=NOW, policy=Policy()
        )[0]
        is False
    )


def test_an_active_run_a_cooldown_a_weekly_bound_and_a_young_model_each_stop_a_run():
    """An active run, the cooldown, the weekly bound and a young model each stop a new run."""
    trained, drifting = NOW - timedelta(days=5), assessed("drifting")

    def verdict(runs, trained=trained):
        return decide(drifting, drifting, runs=runs, trained=trained, now=NOW, policy=Policy())

    assert verdict([run("drift-v3-a", "RUNNING", 30)]) == (False, "active")
    assert verdict([run("drift-v3-a", "SUCCEEDED", 5)]) == (False, "cooldown")
    old = [run(f"drift-v3-{n}", "SUCCEEDED", 30 * (n + 1)) for n in range(3)]
    assert verdict(old) == (False, "bound")
    assert verdict([run("other", "RUNNING", 1)]) == (True, "drift")
    assert verdict([], trained=NOW - timedelta(hours=20)) == (False, "young")


def test_a_model_older_than_the_ceiling_is_retrained_without_drift_under_the_same_limits():
    """A model older than the age ceiling is retrained without drift, under the same limits."""
    stale, ok = NOW - timedelta(days=15), assessed("insufficient")
    assert decide(ok, ok, runs=[], trained=stale, now=NOW, policy=Policy()) == (True, "age")
    busy = [run("drift-age-v3-a", "PENDING", 1)]
    assert decide(ok, ok, runs=busy, trained=stale, now=NOW, policy=Policy()) == (False, "active")
    assert decide(ok, ok, runs=[], trained=None, now=NOW, policy=Policy()) == (False, "none")


def test_a_policy_loads_from_json_with_durations_in_hours_and_days():
    """A JSON policy overrides the defaults, with durations given in hours and days."""
    policy = load_policy('{"samples": 50, "cooldown_hours": 6, "age_days": 30}')
    assert policy.samples == 50 and policy.cooldown == timedelta(hours=6)
    assert policy.age == timedelta(days=30) and policy.psi == 0.2
    assert load_policy("") == Policy()
