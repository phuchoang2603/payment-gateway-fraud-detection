"""Tests for operating thresholds, metrics, the promotion gate and the chart."""

import numpy as np
import pytest
from sklearn.metrics import roc_curve

from risk_model.evaluate import GateEvaluator, measure, pr_curve_html, recall_at, threshold_at


def sample(seed=1, rows=20000, separation=2.0):
    """Returns labels, scores and weights where `separation` sets how far apart the classes score."""
    generator = np.random.default_rng(seed)
    y = (generator.random(rows) < 0.05).astype(np.float32)
    scores = 1 / (1 + np.exp(-(generator.normal(size=rows) + separation * y - 3)))
    weights = np.ones(rows, dtype=np.float32)
    return y, scores, weights


def test_thresholds_flag_the_requested_share_of_legitimate_payments():
    """Thresholds flag the requested share of legitimate payments."""
    y, scores, w = sample()
    thresholds = GateEvaluator().thresholds(y, scores, w)
    legitimate = scores[y == 0]
    assert (legitimate >= thresholds["decline"]).mean() <= 0.001
    assert (legitimate >= thresholds["step_up"]).mean() <= 0.01
    assert thresholds["decline"] > thresholds["step_up"]


def test_gate_needs_the_baseline_and_a_margin_over_production():
    """Gate needs the baseline and a margin over production."""
    y, strong, w = sample(separation=3.0)
    _, middling, _ = sample(separation=1.5)
    _, weak, _ = sample(separation=0.5)
    evaluator = GateEvaluator(margin=0.005)
    candidates = {"model": strong, "baseline": weak, "production": middling}
    thresholds = {name: evaluator.thresholds(y, values, w) for name, values in candidates.items()}
    metrics = evaluator.evaluate(thresholds, y, w, candidates)

    assert evaluator.gate(metrics).passed
    assert evaluator.gate({"model": metrics["model"], "baseline": metrics["baseline"]}).passed

    worse = evaluator.gate({**metrics, "production": {**metrics["model"], "pr_auc": 0.99}})
    assert not worse.passed and "production" in worse.reasons[0]

    beaten = evaluator.gate({"model": metrics["baseline"], "baseline": metrics["model"]})
    assert not beaten.passed and "baseline" in beaten.reasons[0]


def test_the_first_roc_threshold_is_capped_so_it_can_be_serialized():
    """roc_curve starts at infinity, so a threshold read from that point is capped at 2.0."""
    y = np.array([0, 1, 0, 1])
    scores = np.array([0.9, 0.8, 0.7, 0.6])
    curve = roc_curve(y, scores)
    assert curve[2][0] == np.inf
    assert threshold_at(curve, 0.001) == 2.0
    assert threshold_at(curve, 0.5) == pytest.approx(0.8)


def test_recall_at_a_false_positive_rate_reads_the_roc_curve():
    """Perfectly separated scores reach full recall before any false positive."""
    y = np.array([1, 1, 0, 0])
    curve = roc_curve(y, np.array([0.9, 0.8, 0.2, 0.1]))
    assert recall_at(curve, 0.01) == 1.0
    assert recall_at(roc_curve(y, np.array([0.1, 0.2, 0.8, 0.9])), 0.01) == 0.0


def test_a_decline_threshold_above_every_score_flags_nothing():
    """With nothing flagged, precision at the decline threshold is zero and not a division error."""
    y, scores, w = sample(rows=2000)
    result = measure(y, scores, w, decline=2.0)
    assert result["precision_at_decline"] == 0.0 and result["recall_at_decline"] == 0.0
    assert set(result) == {
        "pr_auc",
        "roc_auc",
        "recall_at_1pct_fpr",
        "precision_at_decline",
        "recall_at_decline",
    }


def test_a_lower_recall_than_production_fails_the_gate_even_with_enough_pr_auc():
    """The recall at 1% FPR must not drop below production's, in addition to the PR-AUC margin."""
    good = {"pr_auc": 0.5, "recall_at_1pct_fpr": 0.4}
    metrics = {
        "model": {**good, "recall_at_1pct_fpr": 0.3},
        "baseline": {"pr_auc": 0.1, "recall_at_1pct_fpr": 0.1},
        "production": {"pr_auc": 0.4, "recall_at_1pct_fpr": 0.4},
    }
    verdict = GateEvaluator(margin=0.005).gate(metrics)
    assert not verdict.passed and len(verdict.reasons) == 1 and "recall" in verdict.reasons[0]


def test_beating_production_needs_the_whole_margin():
    """A PR-AUC clearly above production's plus the margin passes, and one clearly below it fails."""
    base = {"baseline": {"pr_auc": 0.1}}
    production = {"pr_auc": 0.4, "recall_at_1pct_fpr": 0.4}
    over = {"model": {"pr_auc": 0.406, "recall_at_1pct_fpr": 0.4}, "production": production, **base}
    under = {
        "model": {"pr_auc": 0.404, "recall_at_1pct_fpr": 0.4},
        "production": production,
        **base,
    }
    evaluator = GateEvaluator(margin=0.005)
    assert evaluator.gate(over).passed and not evaluator.gate(under).passed


def test_the_chart_draws_one_line_per_curve_with_its_legend():
    """pr_curve_html returns an SVG with a polyline and a legend entry per named curve."""
    curves = {
        "model": (np.linspace(1, 0.2, 1000), np.linspace(0, 1, 1000)),
        "baseline": (np.array([1.0, 0.5]), np.array([0.0, 1.0])),
    }
    svg = pr_curve_html(curves)
    assert svg.startswith("<svg") and svg.endswith("</svg>")
    assert svg.count("<polyline") == 2 and "model</text>" in svg and "baseline</text>" in svg
    first_line = svg.split('points="')[1].split('"')[0]
    assert len(first_line.split()) == 200
