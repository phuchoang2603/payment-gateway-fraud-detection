"""Drift: a model's reference profile, the population stability index against the bins of scored requests, and the rule that starts a retraining run."""

import json
from dataclasses import dataclass
from datetime import timedelta

import numpy as np

QUANTILES = np.linspace(0.1, 0.9, 9)
FLOOR = 1e-4
PREFIX = "drift-"
ACTIVE = frozenset({"PENDING", "RUNNING"})


@dataclass(frozen=True)
class Policy:
    """The thresholds of the drift rule.

    A feature breaches at `psi`, and one of the `top` features by gain breaches at `top_psi`. The
    state is `drifting` when `features` features breach or a top feature does, once `samples` requests
    have been counted. A trigger waits `cooldown` after the last drift run, at most `per_week` runs
    start in seven days, a drift run needs a serving model at least `min_age` old, and a model older
    than `age` is retrained without drift.
    """

    samples: int = 2000
    psi: float = 0.2
    top_psi: float = 0.25
    features: int = 3
    top: int = 5
    cooldown: timedelta = timedelta(hours=24)
    per_week: int = 3
    min_age: timedelta = timedelta(days=2)
    age: timedelta = timedelta(days=14)


def load_policy(text):
    """Returns the policy of a JSON object, with durations as `cooldown_hours`, `min_age_days` and `age_days`."""
    values = json.loads(text or "{}")
    for name, unit in (("cooldown", "hours"), ("min_age", "days"), ("age", "days")):
        if f"{name}_{unit}" in values:
            values[name] = timedelta(**{unit: values.pop(f"{name}_{unit}")})
    return Policy(**values)


def weighted_edges(values, weights):
    """Returns the distinct weighted deciles of `values`, the edges that split them into up to ten bins."""
    if len(values) == 0:
        return np.array([], dtype=np.float64)
    order = np.argsort(values, kind="stable")
    ordered = values[order].astype(np.float64)
    cumulative = np.cumsum(weights[order]) / weights.sum()
    picked = np.searchsorted(cumulative, QUANTILES)
    return np.unique(ordered[np.minimum(picked, len(ordered) - 1)])


def profile(split, gain=None):
    """Returns the reference profile of a weighted split, keyed by feature.

    Each feature has its bin `edges`, its weighted `share` per bin with the share of missing values
    last, and its `gain` in the model (zero without one).
    """
    weights = split.w.astype(np.float64)
    total = float(weights.sum())
    gains = gain or {}
    result = {}
    for index, name in enumerate(split.features):
        column = split.x[:, index]
        known = ~np.isnan(column)
        edges = weighted_edges(column[known], weights[known])
        bins = np.searchsorted(edges, column[known].astype(np.float64), side="right")
        share = np.bincount(bins, weights=weights[known], minlength=len(edges) + 1) / total
        result[name] = {
            "edges": [float(edge) for edge in edges],
            "share": [*share.tolist(), float(weights[~known].sum() / total)],
            "gain": float(gains.get(name, 0.0)),
        }
    return result


def edges_of(reference):
    """Returns the bin edges of a profile by feature, which the served model's configuration carries."""
    return {name: entry["edges"] for name, entry in reference.items()}


def index(share, counts):
    """Returns the population stability index of observed bin counts against reference shares, or None without counts."""
    total = sum(counts)
    if total == 0:
        return None
    expected = np.maximum(np.asarray(share, dtype=np.float64), FLOOR)
    observed = np.maximum(np.asarray(counts, dtype=np.float64) / total, FLOOR)
    return float(np.sum((observed - expected) * np.log(observed / expected)))


def assess(reference, counts, policy):
    """Returns the drift state of observed bin counts, `{feature: [count per bin, missing last]}`.

    The result has the state (`insufficient`, `ok` or `drifting`), the sample count, each feature's
    index and missing-share shift, and the features over the threshold.
    """
    samples = max((sum(values) for values in counts.values()), default=0)
    result = {"samples": samples, "psi": {}, "missing": {}, "over": []}
    if samples < policy.samples:
        return {**result, "state": "insufficient"}
    for name, entry in reference.items():
        observed = counts.get(name)
        value = index(entry["share"], observed) if observed else None
        if value is None:
            continue
        result["psi"][name] = value
        result["missing"][name] = observed[-1] / sum(observed) - entry["share"][-1]
        if value >= policy.psi:
            result["over"].append(name)
    ranked = sorted(reference, key=lambda name: reference[name]["gain"], reverse=True)
    top = [name for name in ranked[: policy.top] if reference[name]["gain"] > 0]
    leading = any(result["psi"].get(name, 0) >= policy.top_psi for name in top)
    drifting = len(result["over"]) >= policy.features or leading
    return {**result, "state": "drifting" if drifting else "ok"}


def decide(short, long, *, runs, trained, now, policy):
    """Returns `(start, reason)`: whether to start a retraining run now, and why or why not.

    `short` and `long` are the assessments of the two windows, `runs` the automatic runs as dicts with
    `name`, `state` and `created`, and `trained` when the serving version was registered. A reason of `drift` or
    `age` starts a run; `none`, `active`, `cooldown`, `bound` and `young` do not.
    """
    drifting = short["state"] == "drifting" and long["state"] == "drifting"
    aged = trained is not None and now - trained > policy.age
    if not drifting and not aged:
        return False, "none"
    mine = [run for run in runs if run["name"].startswith(PREFIX)]
    if any(run["state"] in ACTIVE for run in mine):
        return False, "active"
    if any(now - run["created"] < policy.cooldown for run in mine):
        return False, "cooldown"
    if sum(now - run["created"] < timedelta(days=7) for run in mine) >= policy.per_week:
        return False, "bound"
    if not aged and trained is not None and now - trained < policy.min_age:
        return False, "young"
    return True, "age" if aged and not drifting else "drift"
