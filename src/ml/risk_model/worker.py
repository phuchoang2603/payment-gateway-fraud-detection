import argparse
import json
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from urllib.parse import urlsplit

from fraud_feast.lake import catalog, s3, write_json

from risk_model import kube
from risk_model.data import DatasetReader
from risk_model.split import WindowSplitter
from risk_model.train import RiskTrainer, environment

HYPERPARAMETERS = {
    "max_depth": int,
    "eta": float,
    "subsample": float,
    "colsample_bytree": float,
    "min_child_weight": float,
    "lambda": float,
}


def splits(arguments):
    splitter = WindowSplitter(arguments.train, arguments.validation, arguments.test)
    reader = DatasetReader(catalog(), arguments.snapshot)
    return (
        reader.read(splitter["train"], arguments.rate),
        reader.read(splitter["validation"], arguments.rate),
    )


def gpu():
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
    target = urlsplit(uri)
    s3().upload_file(str(path), target.netloc, target.path.lstrip("/"))


def fit(arguments):
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
    train, validation = splits(arguments)
    params = {name: getattr(arguments, name) for name in HYPERPARAMETERS}
    trainer = RiskTrainer(params, seed=arguments.seed, device="cuda")
    trainer.fit(train, validation)
    print(f"aucpr={trainer.best_score:.6f}", flush=True)


def parser():
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
            for hyper, kind in HYPERPARAMETERS.items():
                command.add_argument(
                    f"--{hyper.replace('_', '-')}", dest=hyper, type=kind, required=True
                )
    return root


def main(argv=None):
    arguments = parser().parse_args(argv)
    kube.load_secrets()
    arguments.function(arguments)


if __name__ == "__main__":
    sys.exit(main())
