"""Tests for the training pod's entry point, with the trainer, storage and catalog replaced by fakes."""

import subprocess
from types import SimpleNamespace

import numpy as np
import xgboost as xgb

from risk_model import worker
from risk_model.data import Split

FEATURES = ["a", "b"]
WINDOWS = ["--train", "2026-10-01..2026-10-02", "--validation", "2026-10-03..2026-10-03"]
WINDOWS += ["--test", "2026-10-04..2026-10-04", "--snapshot", "7"]
TRIAL = ["--max-depth", "5", "--eta", "0.1", "--subsample", "0.9", "--colsample-bytree", "0.8"]
TRIAL += ["--min-child-weight", "2", "--lambda", "1.5"]


def split(rows=40):
    """Returns a random two-feature Split with some positives."""
    generator = np.random.default_rng(1)
    x = generator.normal(size=(rows, 2)).astype(np.float32)
    y = (x[:, 0] > 0.5).astype(np.float32)
    keys = np.array([f"k{n}" for n in range(rows)], dtype=object)
    return Split(x, y, np.ones(rows, dtype=np.float32), keys, FEATURES)


class FakeTrainer:
    """A trainer that records how it was built and returns a small CPU booster."""

    built = []
    best_score = 0.123456

    def __init__(self, params, seed, device):
        self.built.append((params, seed, device))

    def fit(self, train, validation):
        """Returns a booster trained on the train split."""
        return xgb.train({"objective": "binary:logistic"}, xgb.DMatrix(train.x, label=train.y), 3)


def test_the_parser_has_a_fit_and_a_trial_command_with_typed_options():
    """fit takes the directories and params, and trial takes every hyperparameter by its dashed name."""
    fit = worker.parser().parse_args(
        ["fit", *WINDOWS, "--model-dir", "s3://b/m", "--run-dir", "s3://b/r", "--rate", "0.5"]
    )
    assert fit.command == "fit" and fit.seed == 7 and fit.params == "{}" and fit.rate == 0.5
    assert fit.function is worker.fit
    trial = worker.parser().parse_args(["trial", *WINDOWS, *TRIAL])
    assert trial.function is worker.trial and trial.rate is None
    assert trial.max_depth == 5 and isinstance(trial.max_depth, int)
    assert trial.eta == 0.1 and getattr(trial, "lambda") == 1.5


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


def test_an_upload_goes_to_the_bucket_and_key_of_the_uri(monkeypatch):
    """upload splits an `s3://` URI into a bucket and key."""
    sent = []
    client = SimpleNamespace(upload_file=lambda *args: sent.append(args))
    monkeypatch.setattr(worker, "s3", lambda: client)
    worker.upload("/tmp/model.json", "s3://bucket/runs/r1/model/model.json")
    assert sent == [("/tmp/model.json", "bucket", "runs/r1/model/model.json")]


def test_a_fit_trains_on_the_gpu_uploads_the_model_and_writes_the_run_record(monkeypatch):
    """fit trains with `cuda`, uploads `model.json` and writes `fit.json` with params, rows and timing."""
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
    assert FakeTrainer.built == [({"max_depth": 4}, 3, "cuda")]
    assert uploads == [("model.json", "s3://b/runs/r/model/model.json", True)]
    record = records["s3://b/runs/r/fit.json"]
    assert record["params"] == {"max_depth": 4} and record["seed"] == 3
    assert record["rows"] == {"train": 40, "validation": 10} and record["best_iteration"] == 2
    assert record["gpu"] == "GPU 0" and record["environment"] == {"xgboost": "2.1.4"}
    assert isinstance(record["seconds"], float)


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
