import argparse
import json
import os
from pathlib import Path

import numpy as np
import xgboost as xgb
from kserve import Model, ModelServer, model_server

MODEL_DIR = Path(os.environ.get("MODEL_DIR", "/mnt/models"))


class PaymentRiskModel(Model):
    def __init__(self, name, directory=MODEL_DIR):
        super().__init__(name)
        self.directory = Path(directory)
        self.booster = None
        self.features = []
        self.version = "unknown"

    def load(self):
        self.booster = xgb.Booster()
        self.booster.load_model(self.directory / "model.json")
        self.features = json.loads(self.booster.attr("feature_order"))
        self.version = (self.directory / "version.txt").read_text().strip()
        self.ready = True
        return self.ready

    def score(self, instances):
        x = np.array(
            [
                [np.nan if row.get(name) is None else float(row[name]) for name in self.features]
                for row in instances
            ],
            dtype=np.float32,
        ).reshape(len(instances), len(self.features))
        return self.booster.predict(xgb.DMatrix(x)).tolist()

    def predict(self, payload, headers=None):
        return {
            "predictions": self.score(payload["instances"]),
            "model_version": self.version,
        }


def main():
    arguments, _ = argparse.ArgumentParser(parents=[model_server.parser]).parse_known_args()
    model = PaymentRiskModel(arguments.model_name)
    model.load()
    ModelServer().start([model])


if __name__ == "__main__":
    main()
