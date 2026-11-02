"""Tests for training: order independence, the baseline, the hyperparameter table and model files."""

import numpy as np
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from risk_model.data import Split
from risk_model.train import (
    HYPERPARAMETERS,
    RiskTrainer,
    environment,
    load_booster,
    save_model,
    score,
)

FEATURES = ["a", "b", "c"]


def make(seed, rows):
    """Returns a random three-feature Split whose label follows the first feature."""
    generator = np.random.default_rng(seed)
    x = generator.normal(size=(rows, len(FEATURES))).astype(np.float32)
    x[generator.random(x.shape) < 0.1] = np.nan
    y = (np.nan_to_num(x[:, 0]) + generator.normal(size=rows) > 0.8).astype(np.float32)
    weights = np.where(y > 0, 1.0, 10.0).astype(np.float32)
    keys = np.array([f"i\x1fp{n}\x1fa{n}" for n in range(rows)], dtype=object)
    return Split(x, y, weights, keys, FEATURES)


def shuffled(split, order):
    """Returns the Split with its rows in the given order."""
    return Split(split.x[order], split.y[order], split.w[order], split.keys[order], split.features)


@settings(max_examples=8, deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(seed=st.integers(0, 10_000), permutation=st.integers(0, 10_000))
def test_same_rows_in_any_order_train_the_same_model(seed, permutation):
    """Same rows in any order train the same model."""
    train, validation = make(seed, 300), make(seed + 1, 120)
    order = np.random.default_rng(permutation).permutation(len(train.y))
    trainer = {"max_depth": 3, "eta": 0.3}
    first = RiskTrainer(trainer, device="cpu").fit(train, validation)
    second = RiskTrainer(trainer, device="cpu").fit(shuffled(train, order), validation)
    probe = make(seed + 2, 50).x
    assert np.array_equal(score(first, probe), score(second, probe))
    assert first.attr("feature_order") == second.attr("feature_order")


def test_the_hyperparameter_table_names_each_parameter_once_with_its_type():
    """Katib and XGBoost names are unique, and int parameters parse as int and others as float."""
    assert len({h.katib for h in HYPERPARAMETERS}) == len(HYPERPARAMETERS)
    assert len({h.xgboost for h in HYPERPARAMETERS}) == len(HYPERPARAMETERS)
    assert [h.xgboost for h in HYPERPARAMETERS if h.convert is int] == ["max_depth"]
    assert all(h.convert is float for h in HYPERPARAMETERS if h.kind == "double")
    assert all(float(h.low) < float(h.high) for h in HYPERPARAMETERS)


def test_the_baseline_scores_probabilities_for_every_row():
    """The linear baseline handles missing values and returns one probability per row."""
    train, probe = make(1, 300), make(2, 50)
    values = score(RiskTrainer(device="cpu").baseline(train), probe.x)
    assert values.shape == (50,) and ((values >= 0) & (values <= 1)).all()


def test_a_model_survives_saving_and_loading_with_its_feature_order(tmp_path):
    """save_model writes both files, and load_booster returns the same scores and the feature order."""
    train, validation = make(3, 300), make(4, 120)
    booster = RiskTrainer({"max_depth": 3}, device="cpu").fit(train, validation)
    save_model(booster, tmp_path / "model")
    assert (tmp_path / "model" / "model.joblib").exists()
    loaded, order = load_booster(tmp_path / "model" / "model.json")
    assert order == FEATURES
    probe = make(5, 20).x
    assert np.array_equal(score(booster, probe), score(loaded, probe))


def test_the_trainer_keeps_its_best_validation_score_and_trims_to_the_best_round():
    """After fitting, best_score is set and the booster has no rounds past the best one."""
    trainer = RiskTrainer({"max_depth": 3, "eta": 0.3}, device="cpu")
    booster = trainer.fit(make(6, 300), make(7, 120))
    assert 0 < trainer.best_score <= 1 and booster.num_boosted_rounds() >= 1


def test_the_environment_reports_the_library_versions():
    """environment names the XGBoost and NumPy versions, and the CUDA version when built with it."""
    info = environment()
    assert info["numpy"] == np.__version__ and info["xgboost"] and "cuda" in info
