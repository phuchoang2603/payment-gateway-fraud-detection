"""Tests for the GPU job entry point, with the GPU, storage and catalog replaced by CPU stand-ins."""

import json
import subprocess
from types import SimpleNamespace

import numpy as np
import pytest
import xgboost as xgb
from sklearn.metrics import average_precision_score

from risk_model import worker
from risk_model.data import Split
from risk_model.train import BASELINE_PARAMS, RiskTrainer, score

FEATURES = ["a", "b"]
WINDOWS = ["--train", "2026-10-01..2026-10-02", "--validation", "2026-10-03..2026-10-03"]
WINDOWS += ["--test", "2026-10-04..2026-10-04", "--snapshot", "7"]
TRIAL = ["--max-depth", "5", "--eta", "0.1", "--subsample", "0.9", "--colsample-bytree", "0.8"]
TRIAL += ["--min-child-weight", "2", "--lambda", "1.5"]


def split(rows=40, seed=1, features=FEATURES):
    """Returns a random two-feature Split with some positives."""
    generator = np.random.default_rng(seed)
    x = generator.normal(size=(rows, 2)).astype(np.float32)
    y = (x[:, 0] + 0.3 * generator.normal(size=rows) > 0.5).astype(np.float32)
    keys = np.array([f"k{n}" for n in range(rows)], dtype=object)
    weights = np.where(y > 0, 1.0, 10.0).astype(np.float32)
    return Split(x, y, weights, keys, features)


def booster(train, validation, params=None):
    """Returns a CPU booster trained on two splits."""
    return RiskTrainer(params, device="cpu").fit(train, validation)


@pytest.fixture
def cpu(monkeypatch):
    """Runs the worker's scoring and training on the CPU, which is all the test machine has."""
    monkeypatch.setattr(worker, "DEVICE", "cpu")


class FakeTrainer:
    """A trainer that records how it was built and returns a small CPU booster."""

    built = []
    best_score = 0.123456

    def __init__(self, params, seed, device):
        self.built.append((params, seed, device))

    def fit(self, train, validation):
        """Returns a booster trained on the train split."""
        return xgb.train({"objective": "binary:logistic"}, xgb.DMatrix(train.x, label=train.y), 3)


def test_the_parser_has_a_command_per_job_with_typed_options():
    """fit, trial, gate and experiments take their own options, typed by their use."""
    fit = worker.parser().parse_args(
        ["fit", *WINDOWS, "--model-dir", "s3://b/m", "--run-dir", "s3://b/r", "--rate", "0.5"]
    )
    assert fit.command == "fit" and fit.seed == 7 and fit.params == "{}" and fit.rate == 0.5
    assert fit.function is worker.fit
    trial = worker.parser().parse_args(["trial", *WINDOWS, *TRIAL])
    assert trial.function is worker.trial and trial.rate is None
    assert trial.max_depth == 5 and isinstance(trial.max_depth, int)
    assert trial.eta == 0.1 and getattr(trial, "lambda") == 1.5
    gate = worker.parser().parse_args(
        ["gate", *WINDOWS, "--model-dir", "s3://b/m", "--run-dir", "s3://b/r"]
    )
    assert gate.function is worker.gate and gate.margin == 0.005
    assert gate.production == "" and gate.production_uri == ""
    experiments = worker.parser().parse_args(
        ["experiments", "--spec", "s3://b/spec.json", "--run-dir", "s3://b/r"]
    )
    assert experiments.function is worker.experiments and experiments.spec == "s3://b/spec.json"


def test_the_splits_are_read_at_the_requested_rate(monkeypatch):
    """splits reads the train and validation windows of the snapshot with the sampling rate."""
    reads = []

    class Reader:
        """A reader that records what it is asked for."""

        def __init__(self, catalog, snapshot):
            reads.append(("open", catalog, snapshot))

        def read(self, window, rate):
            """Records the window and rate and returns the window's text."""
            reads.append((str(window), rate))
            return str(window)

    monkeypatch.setattr(worker, "catalog", lambda: "catalog")
    monkeypatch.setattr(worker, "DatasetReader", Reader)
    arguments = worker.parser().parse_args(["trial", *WINDOWS, *TRIAL, "--rate", "0.05"])
    assert worker.splits(arguments) == ("2026-10-01..2026-10-02", "2026-10-03..2026-10-03")
    assert reads == [
        ("open", "catalog", "7"),
        ("2026-10-01..2026-10-02", 0.05),
        ("2026-10-03..2026-10-03", 0.05),
    ]


def test_the_gpu_is_read_from_nvidia_smi_and_missing_without_one(monkeypatch):
    """gpu returns the stripped `nvidia-smi` output, and None when it is absent or fails."""
    monkeypatch.setattr(
        worker.subprocess, "run", lambda *a, **k: SimpleNamespace(stdout=" NVIDIA RTX, 595\n")
    )
    assert worker.gpu() == "NVIDIA RTX, 595"

    def missing(*a, **k):
        raise FileNotFoundError

    def failing(*a, **k):
        raise subprocess.CalledProcessError(1, "nvidia-smi")

    for error in (missing, failing):
        monkeypatch.setattr(worker.subprocess, "run", error)
        assert worker.gpu() is None


def test_a_fit_trains_the_model_and_its_baseline_on_the_gpu_and_writes_the_run_record(monkeypatch):
    """fit trains both with `cuda`, uploads both boosters and writes `fit.json` with params and timing."""
    FakeTrainer.built.clear()
    uploads, records = [], {}
    monkeypatch.setattr(worker, "splits", lambda arguments: (split(40), split(10)))
    monkeypatch.setattr(worker, "RiskTrainer", FakeTrainer)
    monkeypatch.setattr(
        worker, "upload", lambda path, uri: uploads.append((path.name, uri, path.exists()))
    )
    monkeypatch.setattr(worker, "write_json", lambda uri, value: records.update({uri: value}))
    monkeypatch.setattr(worker, "gpu", lambda: "GPU 0")
    monkeypatch.setattr(worker, "environment", lambda: {"xgboost": "2.1.4"})
    worker.main(
        ["fit", *WINDOWS, "--model-dir", "s3://b/runs/r/model/", "--run-dir", "s3://b/runs/r/"]
        + ["--params", '{"max_depth": 4}', "--seed", "3"]
    )
    assert FakeTrainer.built == [({"max_depth": 4}, 3, "cuda"), (BASELINE_PARAMS, 3, "cuda")]
    assert uploads == [
        ("model.json", "s3://b/runs/r/model/model.json", True),
        ("baseline.json", "s3://b/runs/r/model/baseline.json", True),
    ]
    record = records["s3://b/runs/r/fit.json"]
    assert record["params"] == {"max_depth": 4} and record["seed"] == 3
    assert record["rows"] == {"train": 40, "validation": 10} and record["best_iteration"] == 2
    assert record["gpu"] == "GPU 0" and record["environment"] == {"xgboost": "2.1.4"}
    assert isinstance(record["seconds"], float)
    assert set(record["reference"]) == set(FEATURES)
    assert all(
        len(entry["share"]) == len(entry["edges"]) + 2 for entry in record["reference"].values()
    )


def test_a_trial_prints_the_validation_score_for_katib(monkeypatch, capsys):
    """trial trains with the trial's hyperparameters and prints `aucpr=<value>` for the collector."""
    FakeTrainer.built.clear()
    monkeypatch.setattr(worker, "splits", lambda arguments: (split(40), split(10)))
    monkeypatch.setattr(worker, "RiskTrainer", FakeTrainer)
    worker.main(["trial", *WINDOWS, *TRIAL])
    assert capsys.readouterr().out == "aucpr=0.123456\n"
    ((params, seed, device),) = FakeTrainer.built
    assert params == {
        "max_depth": 5,
        "eta": 0.1,
        "subsample": 0.9,
        "colsample_bytree": 0.8,
        "min_child_weight": 2.0,
        "lambda": 1.5,
    }
    assert (seed, device) == (7, "cuda")


def swap(part):
    """Returns the split with its two feature columns exchanged."""
    return Split(part.x[:, ::-1], part.y, part.w, part.keys, FEATURES[::-1])


def test_the_gate_report_scores_each_model_through_its_own_columns(cpu):
    """A production model trained on swapped columns is scored on the test split read in its own order."""
    train, tuning, holdout = split(300, 1), split(200, 2), split(200, 3)
    older = booster(swap(train), swap(tuning), {"max_depth": 3})
    models = {
        "model": (booster(train, tuning, {"max_depth": 3}), [0, 1]),
        "baseline": (booster(train, tuning, {"max_depth": 1}), [0, 1]),
        "production": (older, [1, 0]),
    }
    report, curves = worker.gate_report(models, tuning, holdout, margin=0.005)
    assert set(report["metrics"]) == set(curves) == {"model", "baseline", "production"}
    expected = average_precision_score(
        holdout.y, score(older, swap(holdout).x, "cpu"), sample_weight=holdout.w
    )
    assert report["metrics"]["production"]["pr_auc"] == pytest.approx(expected)
    assert set(report["thresholds"]) == {"decline", "step_up"}
    assert report["margin"] == 0.005 and report["rows"] == {"validation": 200, "test": 200}
    assert isinstance(report["passed"], bool) and isinstance(report["reasons"], list)


def gate_fixtures(monkeypatch, order=None):
    """Replaces the catalog, reader, downloads and writers a gate run touches and returns what it wrote."""
    train, tuning, holdout = split(300, 1), split(200, 2), split(200, 3)
    boosters = {
        "model.json": booster(train, tuning, {"max_depth": 3}),
        "baseline.json": booster(train, tuning, {"max_depth": 1}),
    }
    if order:
        boosters["model.json"].set_attr(feature_order=json.dumps(order))
    written = {"json": {}, "text": {}}

    class Reader:
        """A reader whose validation and test windows are fixed splits."""

        features = FEATURES

        def __init__(self, catalog, snapshot):
            assert snapshot == "7"

        def read(self, window):
            """Returns the validation split for one window and the test split for the other."""
            return tuning if str(window).startswith("2026-10-03") else holdout

    def download(uri, target):
        boosters[uri.rsplit("/", 1)[1]].save_model(target)
        return str(target)

    monkeypatch.setattr(worker, "catalog", lambda: "catalog")
    monkeypatch.setattr(worker, "DatasetReader", Reader)
    monkeypatch.setattr(worker, "download", download)
    monkeypatch.setattr(
        worker, "write_json", lambda uri, value: written["json"].update({uri: value})
    )
    monkeypatch.setattr(
        worker, "write_text", lambda uri, text, kind: written["text"].update({uri: (text, kind)})
    )
    return boosters, written


def test_the_gate_writes_its_report_and_chart_for_the_model_and_the_baseline(monkeypatch, cpu):
    """Without a production version the gate compares two models and records production as None."""
    _, written = gate_fixtures(monkeypatch)
    worker.main(
        ["gate", *WINDOWS, "--model-dir", "s3://b/runs/r/model/", "--run-dir", "s3://b/runs/r/"]
    )
    report = written["json"]["s3://b/runs/r/gate.json"]
    assert set(report["metrics"]) == {"model", "baseline"} and report["production"] is None
    chart, kind = written["text"]["s3://b/runs/r/pr_curve.html"]
    assert kind == "text/html" and chart.count("<polyline") == 2


def test_the_gate_adds_production_when_a_version_is_live(monkeypatch, cpu):
    """A production URI adds its model.json to the comparison under the production name."""
    _, written = gate_fixtures(monkeypatch)
    worker.main(
        ["gate", *WINDOWS, "--model-dir", "s3://b/runs/r/model", "--run-dir", "s3://b/runs/r"]
        + ["--production", "v1", "--production-uri", "s3://b/runs/old/model/"]
    )
    report = written["json"]["s3://b/runs/r/gate.json"]
    assert set(report["metrics"]) == {"model", "baseline", "production"}
    assert report["production"] == "v1"


def test_the_gate_refuses_a_model_whose_feature_order_differs_from_the_training_set(
    monkeypatch, cpu
):
    """A new model must read the training set's columns in the training set's order."""
    gate_fixtures(monkeypatch, order=["b", "a"])
    with pytest.raises(ValueError, match="model's feature order"):
        worker.main(
            ["gate", *WINDOWS, "--model-dir", "s3://b/runs/r/model", "--run-dir", "s3://b/runs/r"]
        )


def test_a_curve_is_thinned_to_about_the_point_budget():
    """thin keeps every point of a short curve and about CURVE_POINTS of a long one."""
    short = worker.thin(np.array([1.0, 0.5, 0.25]), np.array([0.0, 0.5, 1.0]))
    assert short == {"precision": [1.0, 0.5, 0.25], "recall": [0.0, 0.5, 1.0]}
    long = worker.thin(np.linspace(1, 0, 1000), np.linspace(0, 1, 1000))
    assert len(long["precision"]) == len(long["recall"]) == 200


def test_experiments_run_each_fit_once_per_window_read_and_write_results_and_scores(
    monkeypatch, cpu
):
    """Fits share cached window reads, are scored on each evaluation window and upload their scores."""
    reads, uploads, written = [], {}, {}
    spec = {
        "snapshot": "7",
        "fits": [
            {
                "name": "all features",
                "train": ["2026-10-01..2026-10-01", "2026-10-02..2026-10-02"],
                "tune": "2026-10-03..2026-10-03",
                "evaluate": {
                    "validation": "2026-10-03..2026-10-03",
                    "test": "2026-10-04..2026-10-04",
                },
                "params": {"max_depth": 2},
                "seed": 5,
                "rounds": 3,
            },
            {
                "name": "only a",
                "features": ["a"],
                "train": ["2026-10-01..2026-10-01", "2026-10-02..2026-10-02"],
                "tune": "2026-10-03..2026-10-03",
                "evaluate": {"test": "2026-10-04..2026-10-04"},
                "rate": 0.5,
            },
        ],
    }

    class Reader:
        """A reader that counts its reads and returns a split per window."""

        features = FEATURES

        def __init__(self, catalog, snapshot):
            assert snapshot == "7"

        def read(self, window, rate=None):
            """Records the read and returns a split seeded by the window's first day."""
            reads.append((str(window), rate))
            return split(120, seed=window.first.day)

    def upload(path, uri):
        uploads[uri] = dict(np.load(path))

    monkeypatch.setattr(worker, "catalog", lambda: "catalog")
    monkeypatch.setattr(worker, "DatasetReader", Reader)
    monkeypatch.setattr(worker, "read_json", lambda uri: spec)
    monkeypatch.setattr(worker, "upload", upload)
    monkeypatch.setattr(worker, "write_json", lambda uri, value: written.update({uri: value}))
    worker.main(["experiments", "--spec", "s3://b/spec.json", "--run-dir", "s3://b/runs/r/"])
    results = written["s3://b/runs/r/experiments.json"]["results"]
    assert [result["name"] for result in results] == ["all features", "only a"]
    first, second = results
    assert set(first["metrics"]) == set(first["curves"]) == {"validation", "test"}
    assert set(second["metrics"]) == {"test"} and {"decline", "step_up"} <= set(first["thresholds"])
    assert first["scores"] == "s3://b/runs/r/experiments/0.npz"
    assert set(uploads["s3://b/runs/r/experiments/1.npz"]) == {"test", "test.y", "test.w"}
    assert set(uploads[first["scores"]]) == {
        "validation",
        "validation.y",
        "validation.w",
        "test",
        "test.y",
        "test.w",
    }
    assert (
        uploads[first["scores"]]["test"].shape == uploads[first["scores"]]["test.y"].shape == (120,)
    )
    assert first["best_iteration"] < 3
    assert reads.count(("2026-10-03..2026-10-03", None)) == 1
    assert reads.count(("2026-10-01..2026-10-01", None)) == 1
    assert ("2026-10-01..2026-10-01", 0.5) in reads
    assert 0 <= first["metrics"]["test"]["pr_auc"] <= 1 and first["best_iteration"] >= 0
