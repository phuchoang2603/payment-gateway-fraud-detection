import numpy as np

from risk_model.evaluate import GateEvaluator


def sample(seed=1, rows=20000, separation=2.0):
    generator = np.random.default_rng(seed)
    y = (generator.random(rows) < 0.05).astype(np.float32)
    scores = 1 / (1 + np.exp(-(generator.normal(size=rows) + separation * y - 3)))
    weights = np.ones(rows, dtype=np.float32)
    return y, scores, weights


def test_thresholds_flag_the_requested_share_of_legitimate_payments():
    y, scores, w = sample()
    thresholds = GateEvaluator().thresholds(y, scores, w)
    legitimate = scores[y == 0]
    assert (legitimate >= thresholds["decline"]).mean() <= 0.001
    assert (legitimate >= thresholds["step_up"]).mean() <= 0.01
    assert thresholds["decline"] > thresholds["step_up"]


def test_gate_needs_the_baseline_and_a_margin_over_production():
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
