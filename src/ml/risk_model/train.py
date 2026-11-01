import json
from pathlib import Path

import joblib
import numpy as np
import xgboost as xgb
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

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


class RiskTrainer:
    def __init__(self, params=None, seed=7, device="cuda"):
        self.params = {**DEFAULTS, **(params or {}), "seed": seed, "device": device}
        self.seed = seed
        self.best_score = None

    def baseline(self, train):
        model = make_pipeline(
            SimpleImputer(strategy="median", keep_empty_features=True),
            StandardScaler(),
            LogisticRegression(max_iter=300, random_state=self.seed),
        )
        model.fit(train.x, train.y, logisticregression__sample_weight=train.w)
        return model

    def fit(self, train, validation):
        train, validation = train.ordered(), validation.ordered()
        dtrain = xgb.DMatrix(train.x, label=train.y, weight=train.w)
        dvalidation = xgb.DMatrix(validation.x, label=validation.y, weight=validation.w)
        booster = xgb.train(
            self.params,
            dtrain,
            num_boost_round=ROUNDS,
            evals=[(dvalidation, "validation")],
            early_stopping_rounds=EARLY_STOP,
            verbose_eval=False,
        )
        self.best_score = float(booster.best_score)
        booster = booster[: booster.best_iteration + 1]
        booster.set_attr(feature_order=json.dumps(train.features))
        return booster


def score(model, x):
    if isinstance(model, xgb.Booster):
        return model.predict(xgb.DMatrix(x))
    return model.predict_proba(x)[:, 1]


def save_model(booster, directory):
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    booster.save_model(directory / "model.json")
    joblib.dump(booster, directory / "model.joblib")


def load_booster(path):
    booster = xgb.Booster()
    booster.load_model(path)
    return booster, json.loads(booster.attr("feature_order"))


def environment():
    info = xgb.build_info()
    return {
        "xgboost": xgb.__version__,
        "cuda": ".".join(map(str, info.get("CUDA_VERSION", ()))) or None,
        "numpy": np.__version__,
    }
