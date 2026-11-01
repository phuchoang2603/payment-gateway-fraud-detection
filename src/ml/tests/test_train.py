import numpy as np
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from risk_model.data import Split
from risk_model.train import RiskTrainer, score

FEATURES = ["a", "b", "c"]


def make(seed, rows):
    generator = np.random.default_rng(seed)
    x = generator.normal(size=(rows, len(FEATURES))).astype(np.float32)
    x[generator.random(x.shape) < 0.1] = np.nan
    y = (np.nan_to_num(x[:, 0]) + generator.normal(size=rows) > 0.8).astype(np.float32)
    weights = np.where(y > 0, 1.0, 10.0).astype(np.float32)
    keys = np.array([f"i\x1fp{n}\x1fa{n}" for n in range(rows)], dtype=object)
    return Split(x, y, weights, keys, FEATURES)


def shuffled(split, order):
    return Split(split.x[order], split.y[order], split.w[order], split.keys[order], split.features)


@settings(max_examples=8, deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(seed=st.integers(0, 10_000), permutation=st.integers(0, 10_000))
def test_same_rows_in_any_order_train_the_same_model(seed, permutation):
    train, validation = make(seed, 300), make(seed + 1, 120)
    order = np.random.default_rng(permutation).permutation(len(train.y))
    trainer = {"max_depth": 3, "eta": 0.3}
    first = RiskTrainer(trainer, device="cpu").fit(train, validation)
    second = RiskTrainer(trainer, device="cpu").fit(shuffled(train, order), validation)
    probe = make(seed + 2, 50).x
    assert np.array_equal(score(first, probe), score(second, probe))
    assert first.attr("feature_order") == second.attr("feature_order")
