"""The training pod's entry point: `fit` trains the model on the GPU and `trial` scores one Katib trial."""

import argparse
import json
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from urllib.parse import urlsplit

from fraud_feast.lake import catalog, s3, write_json

from risk_model.data import DatasetReader
from risk_model.split import WindowSplitter
from risk_model.train import HYPERPARAMETERS, RiskTrainer, environment


def splits(arguments):
    """Returns the train and validation splits of the training set, sampled at `--rate`."""
    splitter = WindowSplitter(arguments.train, arguments.validation, arguments.test)
    reader = DatasetReader(catalog(), arguments.snapshot)
    return (
        reader.read(splitter["train"], arguments.rate),
        reader.read(splitter["validation"], arguments.rate),
    )


def gpu():
    """Returns the GPU name and driver version from `nvidia-smi`, or None without one."""
    try:
        return subprocess.run(
            ["nvidia-smi", "--query-gpu=name,driver_version", "--format=csv,noheader"],
            capture_output=True,
            text=True,
            timeout=30,
            check=True,
        ).stdout.strip()
    except (OSError, subprocess.SubprocessError):
        return None


def upload(path, uri):
    """Uploads a local file to an `s3://` URI."""
    target = urlsplit(uri)
    s3().upload_file(str(path), target.netloc, target.path.lstrip("/"))


def fit(arguments):
    """Trains on the GPU, uploads `model.json` to the model directory and writes `fit.json`."""
    started = time.monotonic()
    train, validation = splits(arguments)
    params = json.loads(arguments.params)
    booster = RiskTrainer(params, seed=arguments.seed, device="cuda").fit(train, validation)
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "model.json"
        booster.save_model(path)
        upload(path, f"{arguments.model_dir.rstrip('/')}/model.json")
    write_json(
        f"{arguments.run_dir.rstrip('/')}/fit.json",
        {
            "params": params,
            "seed": arguments.seed,
            "best_iteration": booster.num_boosted_rounds() - 1,
            "rows": {"train": len(train.y), "validation": len(validation.y)},
            "gpu": gpu(),
            "environment": environment(),
            "seconds": round(time.monotonic() - started, 1),
        },
    )


def trial(arguments):
    """Trains with one trial's hyperparameters and prints the validation PR-AUC for Katib."""
    train, validation = splits(arguments)
    params = {h.xgboost: getattr(arguments, h.xgboost) for h in HYPERPARAMETERS}
    trainer = RiskTrainer(params, seed=arguments.seed, device="cuda")
    trainer.fit(train, validation)
    print(f"aucpr={trainer.best_score:.6f}", flush=True)


def parser():
    """Returns the argument parser with a `fit` and a `trial` command."""
    root = argparse.ArgumentParser()
    commands = root.add_subparsers(dest="command", required=True)
    for name, function in (("fit", fit), ("trial", trial)):
        command = commands.add_parser(name)
        command.set_defaults(function=function)
        for window in ("snapshot", "train", "validation", "test"):
            command.add_argument(f"--{window}", required=True)
        command.add_argument("--seed", type=int, default=7)
        command.add_argument("--rate", type=float, default=None)
        if name == "fit":
            command.add_argument("--params", default="{}")
            command.add_argument("--model-dir", required=True)
            command.add_argument("--run-dir", required=True)
        else:
            for hyper in HYPERPARAMETERS:
                command.add_argument(
                    f"--{hyper.xgboost.replace('_', '-')}",
                    dest=hyper.xgboost,
                    type=hyper.convert,
                    required=True,
                )
    return root


def main(argv=None):
    """Runs the command named on the command line."""
    arguments = parser().parse_args(argv)
    arguments.function(arguments)


if __name__ == "__main__":
    sys.exit(main())
