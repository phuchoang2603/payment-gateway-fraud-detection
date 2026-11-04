"""Training and scoring on the GPU: the XGBoost trainer, the additive baseline and the hyperparameter table."""

import json
from typing import NamedTuple

import numpy as np
import xgboost as xgb

DEFAULTS = {
    "objective": "binary:logistic",
    "eval_metric": "aucpr",
    "tree_method": "hist",
    "max_depth": 6,
    "eta": 0.1,
    "subsample": 0.8,
    "colsample_bytree": 0.8,
    "min_child_weight": 1.0,
    "lambda": 1.0,
}
ROUNDS = 2000
EARLY_STOP = 50
BASELINE_PARAMS = {"max_depth": 1}


class Hyperparameter(NamedTuple):
    """One tuned hyperparameter: its Katib name, its XGBoost name, its type and its search range."""

    katib: str
    xgboost: str
    kind: str
    low: str
    high: str

    @property
    def convert(self):
        """Returns the Python type that parses this hyperparameter's value."""
        return int if self.kind == "int" else float


HYPERPARAMETERS = (
    Hyperparameter("maxDepth", "max_depth", "int", "3", "10"),
    Hyperparameter("eta", "eta", "double", "0.02", "0.3"),
    Hyperparameter("subsample", "subsample", "double", "0.5", "1.0"),
    Hyperparameter("colsampleBytree", "colsample_bytree", "double", "0.5", "1.0"),
    Hyperparameter("minChildWeight", "min_child_weight", "double", "1", "20"),
    Hyperparameter("regLambda", "lambda", "double", "0.1", "10"),
)


class RiskTrainer:
    """Trains the fraud model on a train split and a validation split.

    `params` override the XGBoost defaults, `seed` fixes the randomness, `rounds` caps the boosting
    rounds and `device` is `cuda`, which every pipeline and job uses; only unit tests pass `cpu`. `fit` returns a booster cut at its best
    validation iteration, with its feature order stored as an attribute. The baseline the gate compares
    against is the same trainer with `BASELINE_PARAMS`: depth-1 trees, an additive model that handles
    missing values like the model itself.
    """

    def __init__(self, params=None, seed=7, device="cuda", rounds=ROUNDS):
        self.params = {**DEFAULTS, **(params or {}), "seed": seed, "device": device}
        self.rounds = rounds
        self.best_score = None

    def fit(self, train, validation):
        """Returns a booster trained with early stopping on validation PR-AUC, ordered by key."""
        train, validation = train.ordered(), validation.ordered()
        dtrain = xgb.DMatrix(train.x, label=train.y, weight=train.w, feature_names=train.features)
        dvalidation = xgb.DMatrix(
            validation.x, label=validation.y, weight=validation.w, feature_names=validation.features
        )
        booster = xgb.train(
            self.params,
            dtrain,
            num_boost_round=self.rounds,
            evals=[(dvalidation, "validation")],
            early_stopping_rounds=EARLY_STOP,
            verbose_eval=False,
        )
        self.best_score = float(booster.best_score)
        booster = booster[: booster.best_iteration + 1]
        booster.set_attr(feature_order=json.dumps(train.features))
        return booster


def score(booster, x, device="cuda"):
    """Returns the fraud probability of every row of `x`, predicted on the named device."""
    booster.set_param({"device": device})
    return booster.inplace_predict(x)


def load_booster(path):
    """Returns a booster loaded from `path` and the feature order stored in it."""
    booster = xgb.Booster()
    booster.load_model(path)
    return booster, json.loads(booster.attr("feature_order"))


def environment():
    """Returns the XGBoost, CUDA and NumPy versions a model was trained with."""
    info = xgb.build_info()
    return {
        "xgboost": xgb.__version__,
        "cuda": ".".join(map(str, info.get("CUDA_VERSION", ()))) or None,
        "numpy": np.__version__,
    }
