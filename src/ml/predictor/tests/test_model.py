"""Property tests for the predictor: scores depend on feature names, and missing values behave alike."""

import json

import numpy as np
import xgboost as xgb
from hypothesis import given, settings
from hypothesis import strategies as st

from payment_risk_predictor.model import PaymentRiskModel

FEATURES = ["amount", "age_days", "attempts_5m"]


def served(tmp_path_factory):
    """Returns a loaded model trained on random data, with version `v7`."""
    directory = tmp_path_factory.mktemp("models")
    generator = np.random.default_rng(3)
    x = generator.normal(size=(400, len(FEATURES))).astype(np.float32)
    x[generator.random(x.shape) < 0.1] = np.nan
    y = (np.nan_to_num(x[:, 0]) > 0.5).astype(np.float32)
    booster = xgb.train(
        {"objective": "binary:logistic", "max_depth": 3, "tree_method": "hist"},
        xgb.DMatrix(x, label=y),
        num_boost_round=20,
    )
    booster.set_attr(feature_order=json.dumps(FEATURES))
    booster.save_model(directory / "model.json")
    (directory / "version.txt").write_text("v7\n")
    model = PaymentRiskModel("payment-risk", directory)
    model.load()
    return model


values = st.one_of(st.none(), st.floats(-5, 5, allow_nan=False))
rows = st.lists(st.fixed_dictionaries({}, optional={name: values for name in FEATURES}), min_size=1)


def test_scores_follow_feature_names_not_key_order(tmp_path_factory):
    """Scores are the same for any key order and for repeated calls."""
    model = served(tmp_path_factory)

    @settings(max_examples=40, deadline=None)
    @given(rows=rows, shuffle=st.randoms())
    def check(rows, shuffle):
        reordered = []
        for row in rows:
            items = list(row.items())
            shuffle.shuffle(items)
            reordered.append(dict(items))
        first = model.score(rows)
        assert first == model.score(reordered)
        assert first == model.score(rows)

    check()


def test_missing_null_and_unknown_features_are_treated_alike(tmp_path_factory):
    """A missing, a null and an unknown feature give the same score."""
    model = served(tmp_path_factory)
    explicit = {"amount": 1.0, "age_days": None, "attempts_5m": None}
    assert model.score([explicit]) == model.score([{"amount": 1.0}])
    assert model.score([{"amount": 1.0, "other": 9}]) == model.score([{"amount": 1.0}])


def test_response_names_the_model_version(tmp_path_factory):
    """The response carries the model version and one score between 0 and 1 per instance."""
    model = served(tmp_path_factory)
    response = model.predict({"instances": [{"amount": 2.0}, {}]})
    assert response["model_version"] == "v7"
    assert len(response["predictions"]) == 2 and all(0 <= p <= 1 for p in response["predictions"])
