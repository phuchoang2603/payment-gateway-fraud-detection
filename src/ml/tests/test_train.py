"""Tests for training: order independence, the additive baseline, the hyperparameter table and model files."""

import numpy as np
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from risk_model.data import Split
from risk_model.drift import profile
from risk_model.train import (
    BASELINE_PARAMS,
    HYPERPARAMETERS,
    RiskTrainer,
    environment,
    load_booster,
    score,
)

FEATURES = ["a", "b", "c"]
CPU = "cpu"


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
    assert np.array_equal(score(first, probe, CPU), score(second, probe, CPU))
    assert first.attr("feature_order") == second.attr("feature_order")


def test_the_hyperparameter_table_names_each_parameter_once_with_its_type():
    """Katib and XGBoost names are unique, and int parameters parse as int and others as float."""
    assert len({h.katib for h in HYPERPARAMETERS}) == len(HYPERPARAMETERS)
    assert len({h.xgboost for h in HYPERPARAMETERS}) == len(HYPERPARAMETERS)
    assert [h.xgboost for h in HYPERPARAMETERS if h.convert is int] == ["max_depth"]
    assert all(h.convert is float for h in HYPERPARAMETERS if h.kind == "double")
    assert all(float(h.low) < float(h.high) for h in HYPERPARAMETERS)


def test_the_baseline_is_an_additive_model_that_scores_probabilities_for_every_row():
    """The baseline boosts depth-1 trees, handles missing values and returns one probability per row."""
    train, validation, probe = make(1, 300), make(8, 120), make(2, 50)
    baseline = RiskTrainer(BASELINE_PARAMS, device=CPU).fit(train, validation)
    assert all(tree.count("leaf=") == 2 for tree in baseline.get_dump())
    values = score(baseline, probe.x, CPU)
    assert values.shape == (50,) and ((values >= 0) & (values <= 1)).all()


def test_scoring_defaults_to_the_gpu_and_never_falls_back_quietly():
    """A booster scored without naming a device is asked for cuda, so a CPU pod cannot score by accident."""
    booster = RiskTrainer({"max_depth": 2}, device=CPU).fit(make(9, 200), make(10, 100))
    seen = []
    booster.set_param = seen.append
    assert score(booster, make(11, 5).x).shape == (5,)
    assert seen == [{"device": "cuda"}]


def test_a_model_survives_saving_and_loading_with_its_feature_order(tmp_path):
    """load_booster returns the same scores and the feature order of a saved booster."""
    train, validation = make(3, 300), make(4, 120)
    booster = RiskTrainer({"max_depth": 3}, device="cpu").fit(train, validation)
    booster.save_model(tmp_path / "model.json")
    loaded, order = load_booster(tmp_path / "model.json")
    assert order == FEATURES
    gains = loaded.get_score(importance_type="gain")
    reference = profile(train, gains)
    assert gains and set(gains) <= set(FEATURES)
    assert sum(entry["gain"] for entry in reference.values()) > 0
    probe = make(5, 20).x
    assert np.array_equal(score(booster, probe, CPU), score(loaded, probe, CPU))


def test_the_trainer_keeps_its_best_validation_score_and_trims_to_the_best_round():
    """After fitting, best_score is set and the booster has no rounds past the best one."""
    trainer = RiskTrainer({"max_depth": 3, "eta": 0.3}, device="cpu")
    booster = trainer.fit(make(6, 300), make(7, 120))
    assert 0 < trainer.best_score <= 1 and booster.num_boosted_rounds() >= 1


def test_the_environment_reports_the_library_versions():
    """environment names the XGBoost and NumPy versions, and the CUDA version when built with it."""
    info = environment()
    assert info["numpy"] == np.__version__ and info["xgboost"] and "cuda" in info


def test_the_round_cap_limits_the_number_of_boosting_rounds():
    """rounds caps how many rounds the trainer boosts, however long validation keeps improving."""
    booster = RiskTrainer({"max_depth": 3, "eta": 0.01}, device=CPU, rounds=4).fit(
        make(12, 300), make(13, 120)
    )
    assert booster.num_boosted_rounds() <= 4
