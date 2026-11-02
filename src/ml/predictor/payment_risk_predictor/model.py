"""The KServe model server for payment_risk_v1: loads the registered booster and scores named features."""

import argparse
import json
import os
from pathlib import Path

import numpy as np
import xgboost as xgb
from kserve import Model, ModelServer, model_server

MODEL_DIR = Path(os.environ.get("MODEL_DIR", "/mnt/models"))


class PaymentRiskModel(Model):
    """A KServe model that scores attempts with the XGBoost booster in the model directory.

    `directory` holds `model.json` and `version.txt`. Instances name their features, so callers never
    depend on column order, and a missing or null feature is treated as missing by XGBoost.
    """

    def __init__(self, name, directory=MODEL_DIR):
        super().__init__(name)
        self.directory = Path(directory)
        self.booster = None
        self.features = []
        self.version = "unknown"

    def load(self):
        """Loads the booster, its feature order and the version name, and marks the model ready."""
        self.booster = xgb.Booster()
        self.booster.load_model(self.directory / "model.json")
        self.features = json.loads(self.booster.attr("feature_order"))
        self.version = (self.directory / "version.txt").read_text().strip()
        self.ready = True
        return self.ready

    def score(self, instances):
        """Returns the fraud probability of each instance, a dict of feature name to value."""
        x = np.array(
            [
                [np.nan if row.get(name) is None else float(row[name]) for name in self.features]
                for row in instances
            ],
            dtype=np.float32,
        ).reshape(len(instances), len(self.features))
        return self.booster.predict(xgb.DMatrix(x)).tolist()

    def predict(self, payload, headers=None):
        """Returns the scores of the payload's instances and the version of the model that made them."""
        return {
            "predictions": self.score(payload["instances"]),
            "model_version": self.version,
        }


def main():
    """Starts the model server with the model named on the command line."""
    arguments, _ = argparse.ArgumentParser(parents=[model_server.parser]).parse_known_args()
    model = PaymentRiskModel(arguments.model_name)
    model.load()
    ModelServer().start([model])


if __name__ == "__main__":
    main()
