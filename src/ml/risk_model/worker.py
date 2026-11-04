"""The GPU job entry point: `fit`, `trial`, `gate` and `experiments`, each run as a TrainJob pod.

`fit` trains the model and its baseline, `trial` scores one Katib trial, `gate` measures the candidate
against the baseline and production, and `experiments` runs a list of fits for the notebook. Every
model is trained and scored on the GPU.
"""

import argparse
import json
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import numpy as np
from fraud_feast.lake import catalog, read_json, write_json
from sklearn.metrics import precision_recall_curve

from risk_model.data import DatasetReader, Split
from risk_model.drift import profile
from risk_model.evaluate import MARGIN, GateEvaluator, measure, pr_curve_html
from risk_model.objects import download, upload, write_text
from risk_model.split import Window, WindowSplitter
from risk_model.train import (
    BASELINE_PARAMS,
    HYPERPARAMETERS,
    ROUNDS,
    RiskTrainer,
    environment,
    load_booster,
    score,
)

DEVICE = "cuda"
CURVE_POINTS = 200


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


def fit(arguments):
    """Trains the model and its baseline, uploads both to the model directory and writes `fit.json`.

    The record carries the reference profile of the training split, which drift is measured against.
    """
    started = time.monotonic()
    train, validation = splits(arguments)
    params = json.loads(arguments.params)
    booster = RiskTrainer(params, seed=arguments.seed, device=DEVICE).fit(train, validation)
    baseline = RiskTrainer(BASELINE_PARAMS, seed=arguments.seed, device=DEVICE).fit(
        train, validation
    )
    with tempfile.TemporaryDirectory() as directory:
        for name, model in (("model", booster), ("baseline", baseline)):
            path = Path(directory) / f"{name}.json"
            model.save_model(path)
            upload(path, f"{arguments.model_dir.rstrip('/')}/{name}.json")
    write_json(
        f"{arguments.run_dir.rstrip('/')}/fit.json",
        {
            "params": params,
            "seed": arguments.seed,
            "best_iteration": booster.num_boosted_rounds() - 1,
            "rows": {"train": len(train.y), "validation": len(validation.y)},
            "reference": profile(train, booster.get_score(importance_type="gain")),
            "gpu": gpu(),
            "environment": environment(),
            "seconds": round(time.monotonic() - started, 1),
        },
    )


def trial(arguments):
    """Trains with one trial's hyperparameters and prints the validation PR-AUC for Katib."""
    train, validation = splits(arguments)
    params = {h.xgboost: getattr(arguments, h.xgboost) for h in HYPERPARAMETERS}
    trainer = RiskTrainer(params, seed=arguments.seed, device=DEVICE)
    trainer.fit(train, validation)
    print(f"aucpr={trainer.best_score:.6f}", flush=True)


def gate_report(models, tuning, holdout, margin):
    """Scores every model on the GPU and returns the gate report and the test precision-recall curves.

    `models` maps a name to its booster and the columns of the split it reads. Thresholds come from
    the validation split, and the metrics from the test split at each model's own decline threshold.
    """
    evaluator = GateEvaluator(margin)
    thresholds, scores = {}, {}
    for name, (booster, columns) in models.items():
        thresholds[name] = evaluator.thresholds(
            tuning.y, score(booster, tuning.x[:, columns], DEVICE), tuning.w
        )
        scores[name] = score(booster, holdout.x[:, columns], DEVICE)
    metrics = evaluator.evaluate(thresholds, holdout.y, holdout.w, scores)
    verdict = evaluator.gate(metrics)
    curves = {
        name: precision_recall_curve(holdout.y, values, sample_weight=holdout.w)[:2]
        for name, values in scores.items()
    }
    report = {
        "passed": verdict.passed,
        "reasons": verdict.reasons,
        "metrics": metrics,
        "thresholds": thresholds["model"],
        "margin": margin,
        "rows": {"validation": len(tuning.y), "test": len(holdout.y)},
    }
    return report, curves


def gate(arguments):
    """Measures the new model against its baseline and production and writes the report and chart."""
    splitter = WindowSplitter(arguments.train, arguments.validation, arguments.test)
    reader = DatasetReader(catalog(), arguments.snapshot)
    tuning, holdout = (reader.read(splitter[name]) for name in ("validation", "test"))
    directory = arguments.model_dir.rstrip("/")
    sources = {"model": f"{directory}/model.json", "baseline": f"{directory}/baseline.json"}
    if arguments.production_uri:
        sources["production"] = f"{arguments.production_uri.rstrip('/')}/model.json"
    models = {}
    with tempfile.TemporaryDirectory() as scratch:
        for name, uri in sources.items():
            booster, order = load_booster(download(uri, Path(scratch) / f"{name}.json"))
            if name != "production" and order != reader.features:
                raise ValueError(f"the {name}'s feature order differs from the training set's")
            models[name] = (booster, [reader.features.index(feature) for feature in order])
    report, curves = gate_report(models, tuning, holdout, arguments.margin)
    report["production"] = arguments.production or None
    root = arguments.run_dir.rstrip("/")
    write_json(f"{root}/gate.json", report)
    write_text(f"{root}/pr_curve.html", pr_curve_html(curves), "text/html")


def thin(precision, recall):
    """Returns about CURVE_POINTS points of a precision-recall curve as lists of numbers."""
    step = max(len(recall) // CURVE_POINTS, 1)
    return {
        "precision": [round(float(value), 6) for value in precision[::step]],
        "recall": [round(float(value), 6) for value in recall[::step]],
    }


def run_fit(spec, read, features, evaluator):
    """Trains one fit of an experiments spec and returns its result and its scores per window.

    A fit names its training windows, the window it stops early and sets thresholds on, and the
    windows it is evaluated on. `features` limits the columns, `rate` samples the training negatives
    and `rounds` caps the boosting rounds. Each window's scores are saved with its labels and weights.
    """
    names = spec.get("features") or features
    train = Split.join([read(window, spec.get("rate")).select(names) for window in spec["train"]])
    tune = read(spec["tune"]).select(names)
    trainer = RiskTrainer(
        spec.get("params"),
        seed=spec.get("seed", 7),
        device=DEVICE,
        rounds=spec.get("rounds", ROUNDS),
    )
    booster = trainer.fit(train, tune)
    thresholds = evaluator.thresholds(tune.y, score(booster, tune.x, DEVICE), tune.w)
    metrics, curves, scores = {}, {}, {}
    for label, window in spec["evaluate"].items():
        held = read(window).select(names)
        values = score(booster, held.x, DEVICE)
        scores[label], scores[f"{label}.y"], scores[f"{label}.w"] = values, held.y, held.w
        metrics[label] = measure(held.y, values, held.w, thresholds["decline"])
        curves[label] = thin(*precision_recall_curve(held.y, values, sample_weight=held.w)[:2])
    result = {
        "name": spec["name"],
        "best_iteration": booster.num_boosted_rounds() - 1,
        "thresholds": thresholds,
        "metrics": metrics,
        "curves": curves,
    }
    return result, scores


def experiments(arguments):
    """Runs every fit of a spec on the GPU and writes `experiments.json` and each fit's scores."""
    spec = read_json(arguments.spec)
    reader = DatasetReader(catalog(), spec["snapshot"])
    cache = {}

    def read(window, rate=None):
        if (window, rate) not in cache:
            cache[window, rate] = reader.read(Window.parse(window), rate)
        return cache[window, rate]

    root = arguments.run_dir.rstrip("/")
    evaluator = GateEvaluator()
    results = []
    with tempfile.TemporaryDirectory() as scratch:
        for index, fit_spec in enumerate(spec["fits"]):
            result, scores = run_fit(fit_spec, read, reader.features, evaluator)
            path = Path(scratch) / f"{index}.npz"
            np.savez(path, **scores)
            result["scores"] = f"{root}/experiments/{index}.npz"
            upload(path, result["scores"])
            results.append(result)
    write_json(f"{root}/experiments.json", {"results": results})


def windows(command):
    """Adds the training-set snapshot and the three windows to a command."""
    for name in ("snapshot", "train", "validation", "test"):
        command.add_argument(f"--{name}", required=True)


def parser():
    """Returns the argument parser with the `fit`, `trial`, `gate` and `experiments` commands."""
    root = argparse.ArgumentParser()
    commands = root.add_subparsers(dest="command", required=True)
    for name, function in (("fit", fit), ("trial", trial)):
        command = commands.add_parser(name)
        command.set_defaults(function=function)
        windows(command)
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
    command = commands.add_parser("gate")
    command.set_defaults(function=gate)
    windows(command)
    command.add_argument("--model-dir", required=True)
    command.add_argument("--run-dir", required=True)
    command.add_argument("--production", default="")
    command.add_argument("--production-uri", default="")
    command.add_argument("--margin", type=float, default=MARGIN)
    command = commands.add_parser("experiments")
    command.set_defaults(function=experiments)
    command.add_argument("--spec", required=True)
    command.add_argument("--run-dir", required=True)
    return root


def main(argv=None):
    """Runs the command named on the command line."""
    arguments = parser().parse_args(argv)
    arguments.function(arguments)


if __name__ == "__main__":
    sys.exit(main())
