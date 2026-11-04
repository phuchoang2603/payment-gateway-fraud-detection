"""Tests for the pipeline steps, with the lake, Spark, registry and cluster replaced by fakes."""

import inspect
import json
from datetime import date
from types import SimpleNamespace

import numpy as np
import pyarrow as pa
import pytest

from risk_model import dataset, steps
from risk_model.data import Split
from risk_model.split import Window

FEATURES = ["a", "b", "c"]
TRAIN, VALIDATION, TEST = (
    "2026-10-15..2026-10-16",
    "2026-10-17..2026-10-17",
    "2026-10-18..2026-10-18",
)
CUTOFF = "2026-10-19T00:00:00Z"
WINDOWS = {"train": TRAIN, "validation": VALIDATION, "test": TEST}


@pytest.fixture
def files(monkeypatch):
    """Replaces the lake's JSON files with a dict keyed by URI, and sets the run root."""
    stored = {}
    monkeypatch.setenv("RUN_ROOT", "s3://bucket/runs/")
    monkeypatch.setattr(steps, "write_json", lambda uri, value: stored.__setitem__(uri, value))
    monkeypatch.setattr(steps, "read_json", lambda uri: stored[uri])
    return stored


class Spark:
    """A Spark session that only records whether it was stopped."""

    stopped = False

    def stop(self):
        """Records the stop."""
        self.stopped = True


def make(rows, seed=0):
    """Returns a random three-feature Split whose label follows the first feature."""
    generator = np.random.default_rng(seed)
    x = generator.normal(size=(rows, 3)).astype(np.float32)
    y = (x[:, 0] + generator.normal(size=rows) * 0.5 > 0.8).astype(np.float32)
    weights = np.where(y > 0, 1.0, 5.0).astype(np.float32)
    keys = np.array([f"k{n}" for n in range(rows)], dtype=object)
    return Split(x, y, weights, keys, FEATURES)


class Reader:
    """A training-set reader that serves splits by window text, a table for scans and provenance."""

    snapshot_id = 7
    features = FEATURES

    def __init__(self, splits=None, table=None, report=None):
        self.splits, self.table, self.report = splits or {}, table, report

    def read(self, window):
        """Returns the split prepared for a window."""
        return self.splits[str(window)]

    def scan(self, columns=None):
        """Returns the prepared table."""
        return self.table

    def validate(self, bounds):
        """Returns the prepared report."""
        self.bounds = bounds
        return self.report

    def provenance(self):
        """Returns a fixed provenance."""
        return {
            "training_set_snapshot": 7,
            "feature_service": "payment_risk_v1",
            "features": FEATURES,
            "sources": {"t": 1},
            "window": f"{TRAIN.split('..')[0]}..{TEST.split('..')[1]}",
            "cutoff": CUTOFF,
            "sample_rate": 0.1,
        }


def serving(monkeypatch, reader):
    """Makes every `DatasetReader.at` return the reader."""
    monkeypatch.setattr(steps, "DatasetReader", SimpleNamespace(at=lambda snapshot: reader))


def test_the_run_directory_hangs_off_the_run_root(files):
    """directory joins the run root and the run ID with one slash."""
    assert steps.directory("r1") == "s3://bucket/runs/r1"


def test_flags_turn_keywords_into_dashed_options():
    """flags returns `--name value` pairs, with underscores as dashes and values as text."""
    assert steps.flags(model_dir="s3://b", seed=7) == ["--model-dir", "s3://b", "--seed", "7"]
    assert steps.flags(**{"lambda": 1.5}) == ["--lambda", "1.5"]
    assert steps.flags() == []


@pytest.mark.parametrize(
    ("text", "value"),
    [("true", True), ("True", True), ("1", True), ("false", False), ("0", False), ("", False)],
)
def test_command_line_booleans_are_parsed_case_insensitively(text, value):
    """truth accepts true in any case and 1, and treats everything else as false."""
    assert steps.truth(text) is value


def test_results_are_written_as_text_with_lowercase_booleans():
    """text renders True and False as `true` and `false`, and other values with str."""
    assert [steps.text(v) for v in (True, False, 5, "x", 0.5)] == ["true", "false", "5", "x", "0.5"]


def test_each_annotation_has_its_command_line_type():
    """int, float and bool parameters are parsed to their types and anything else stays text."""
    assert [steps.argument_type(t) for t in (int, float, bool, str)] == [
        int,
        float,
        steps.truth,
        str,
    ]
    assert steps.argument_type(inspect.Parameter.empty) is str


def test_main_parses_typed_options_and_writes_each_output(monkeypatch, tmp_path):
    """main types options by annotation, calls the step and writes each `--out` file."""

    def sample(*, count: int, rate: float, flag: bool = False, name="x"):
        """Returns values of several types."""
        return {"total": count * 2, "ok": flag, "label": name, "rate": rate}

    monkeypatch.setitem(steps.STEPS, "sample", sample)
    out = tmp_path / "nested"
    steps.main(
        ["sample", "--count", "3", "--rate", "0.5", "--flag", "True"]
        + ["--out", "total", str(out / "total"), "--out", "ok", str(out / "ok")]
        + ["--out", "label", str(out / "label"), "--out", "rate", str(out / "rate")]
    )
    assert [(out / n).read_text() for n in ("total", "ok", "label", "rate")] == [
        "6",
        "true",
        "x",
        "0.5",
    ]
    with pytest.raises(SystemExit):
        steps.main(["sample", "--rate", "0.5"])


def test_every_pipeline_step_is_registered_by_its_function_name():
    """STEPS maps each step's name to its function."""
    assert set(steps.STEPS) == {
        "plan_windows",
        "plan_examples",
        "stage_examples",
        "assemble_examples",
        "check_examples",
        "split_windows",
        "tune_params",
        "fit_on_gpu",
        "gate",
        "record_version",
        "roll_out",
        "promote_challenger",
    }
    assert all(steps.STEPS[name].__name__ == name for name in steps.STEPS)


def test_explicit_windows_skip_planning_and_always_run(monkeypatch, files):
    """Four fixed values are validated and returned with run true, without touching the lake."""
    monkeypatch.setattr(steps, "catalog", lambda: pytest.fail("the lake must not be read"))
    result = steps.plan_windows(
        run_id="r1",
        fixed_train=TRAIN,
        fixed_validation=VALIDATION,
        fixed_test=TEST,
        fixed_cutoff=CUTOFF,
    )
    assert result == {
        "train": TRAIN,
        "validation": VALIDATION,
        "test": TEST,
        "cutoff": CUTOFF,
        "run": True,
    }
    with pytest.raises(ValueError):
        steps.plan_windows(
            run_id="r1",
            fixed_train=TRAIN,
            fixed_validation=TRAIN,
            fixed_test=TEST,
            fixed_cutoff=CUTOFF,
        )


def planned(monkeypatch, covered, registered, **options):
    """Runs plan_windows for a run created on 2026-11-02 with a fake coverage and registry."""
    seen = []
    monkeypatch.setattr(steps, "catalog", lambda: "catalog")
    monkeypatch.setattr(steps, "covered_end", lambda *a: seen.append(a) or covered)
    monkeypatch.setattr(
        steps, "RegistryClient", lambda: SimpleNamespace(latest_cutoff=lambda: registered)
    )
    result = steps.plan_windows(
        run_id="r1",
        create_time="2026-11-02T03:17:00Z",
        train_days=4,
        validation_days=1,
        test_days=1,
        lag_days=1,
        **options,
    )
    return result, seen


def test_a_run_with_newer_data_trains_on_the_latest_covered_windows(monkeypatch, files):
    """The windows end on the covered day, the run proceeds, and the decision is recorded."""
    result, seen = planned(monkeypatch, date(2026, 10, 20), "2026-10-20T00:00:00Z")
    assert seen == [("catalog", 6, date(2026, 11, 1))]
    assert result == {
        "train": "2026-10-15..2026-10-18",
        "validation": "2026-10-19..2026-10-19",
        "test": "2026-10-20..2026-10-20",
        "cutoff": "2026-10-21T00:00:00Z",
        "run": True,
    }
    assert files["s3://bucket/runs/r1/windows.json"]["covered_end"] == "2026-10-20"
    assert files["s3://bucket/runs/r1/windows.json"]["run"] is True


def test_a_run_without_newer_data_stops_unless_forced(monkeypatch, files):
    """A cutoff equal to the registered one skips the run, and force overrides that."""
    stale = "2026-10-21T00:00:00Z"
    assert planned(monkeypatch, date(2026, 10, 20), stale)[0]["run"] is False
    assert planned(monkeypatch, date(2026, 10, 20), stale, force=True)[0]["run"] is True
    assert planned(monkeypatch, date(2026, 10, 20), None)[0]["run"] is True


def test_a_run_with_no_covered_span_stops_even_when_forced(monkeypatch, files):
    """With no day whose whole span has rows there is nothing to train on."""
    result, _ = planned(monkeypatch, None, None, force=True)
    assert result["run"] is False
    assert files["s3://bucket/runs/r1/windows.json"]["covered_end"] is None


@pytest.fixture
def lake(monkeypatch):
    """Replaces the dataset module's Spark, Feast and staging functions with recording fakes."""
    spark, calls = Spark(), []
    monkeypatch.setattr(dataset, "feast_store", lambda: "store")
    monkeypatch.setattr(dataset, "session", lambda store: spark)
    monkeypatch.setattr(dataset, "prepare", lambda *a: calls.append(("prepare", a)) or {"t": 1})
    monkeypatch.setattr(dataset, "stage", lambda *a: calls.append(("stage", a)) or 5)
    return SimpleNamespace(spark=spark, calls=calls)


def test_planning_examples_fences_the_sources_and_lists_the_chunks(lake, files):
    """plan_examples prepares staging over the whole range, stores the fence and returns the chunks."""
    result = steps.plan_examples(
        run_id="r1", train=TRAIN, validation=VALIDATION, test=TEST, chunk_days=2
    )
    assert json.loads(result["chunks"]) == ["2026-10-15/2026-10-17", "2026-10-17/2026-10-19"]
    assert lake.calls == [
        ("prepare", (lake.spark, "store", date(2026, 10, 15), date(2026, 10, 19)))
    ]
    assert files["s3://bucket/runs/r1/fence.json"] == {"before": {"t": 1}}
    assert lake.spark.stopped


def test_planning_stops_spark_when_it_fails(lake, files, monkeypatch):
    """A failure while preparing still stops the Spark session."""

    def broken(*_):
        raise RuntimeError("coverage")

    monkeypatch.setattr(dataset, "prepare", broken)
    with pytest.raises(RuntimeError):
        steps.plan_examples(run_id="r1", train=TRAIN, validation=VALIDATION, test=TEST)
    assert lake.spark.stopped


def test_an_existing_snapshot_needs_no_planning_or_assembly(monkeypatch, lake, files):
    """With an existing snapshot, plan returns no chunks and assemble only confirms the snapshot."""
    result = steps.plan_examples(
        run_id="r1", train=TRAIN, validation=VALIDATION, test=TEST, existing_snapshot="7"
    )
    assert result == {"chunks": "[]"} and lake.calls == []
    serving(monkeypatch, Reader())
    assembled = steps.assemble_examples(
        run_id="r1",
        train=TRAIN,
        validation=VALIDATION,
        test=TEST,
        cutoff=CUTOFF,
        existing_snapshot="7",
    )
    assert assembled == {"snapshot": "7"}


def test_a_chunk_of_days_is_staged_and_its_rows_counted(lake):
    """stage_examples parses `first/last` days, stages them and stops Spark."""
    assert steps.stage_examples(chunk="2026-10-15/2026-10-17", cutoff=CUTOFF) == {"rows": "5"}
    assert lake.calls == [
        ("stage", (lake.spark, "store", date(2026, 10, 15), date(2026, 10, 17), CUTOFF))
    ]
    assert lake.spark.stopped


def test_assembling_merges_the_fenced_chunks_and_records_the_build(lake, files, monkeypatch):
    """assemble_examples merges with the stored fence, writes the build with its time and returns the ID."""
    files["s3://bucket/runs/r1/fence.json"] = {"before": {"t": 1}}
    merged = []
    build = dataset.Build(snapshot_id=9, changed=True, rows=10, positives=2)
    monkeypatch.setattr(dataset, "merge_examples", lambda *a: merged.append(a) or build)
    result = steps.assemble_examples(
        run_id="r1", train=TRAIN, validation=VALIDATION, test=TEST, cutoff=CUTOFF
    )
    assert result == {"snapshot": "9"}
    assert merged == [
        (lake.spark, "store", date(2026, 10, 15), date(2026, 10, 19), CUTOFF, {"t": 1})
    ]
    record = files["s3://bucket/runs/r1/examples.json"]
    assert record["snapshot_id"] == 9 and record["rows"] == 10 and record["seconds"] >= 0
    assert lake.spark.stopped


def test_a_valid_training_set_passes_the_check_and_an_invalid_one_fails_the_step(
    monkeypatch, files
):
    """check_examples writes the report and exits non-zero with the failures when it is not ok."""
    reader = Reader(report={"ok": True, "failures": []})
    serving(monkeypatch, reader)
    assert steps.check_examples(
        snapshot="7", run_id="r1", fraud_rate_min=0.1, fraud_rate_max=0.2
    ) == {"report": "s3://bucket/runs/r1/check.json"}
    assert reader.bounds == (0.1, 0.2)
    serving(monkeypatch, Reader(report={"ok": False, "failures": ["2 null labels"]}))
    with pytest.raises(SystemExit, match="2 null labels"):
        steps.check_examples(snapshot="7", run_id="r1")
    assert files["s3://bucket/runs/r1/check.json"]["ok"] is False


def window_table(labels_by_window):
    """Returns an Arrow table with one row per label at noon of each window's first day."""
    started, labels = [], []
    for text, values in labels_by_window.items():
        day = Window.parse(text).start.replace(hour=12)
        started += [day] * len(values)
        labels += values
    return pa.table(
        {"started_at": pa.array(started, pa.timestamp("us", "UTC")), "label": pa.array(labels)}
    )


def test_the_split_report_counts_rows_and_the_sample_fraud_rate_of_each_window(monkeypatch, files):
    """split_windows reports the rows and positive share of every window."""
    table = window_table(
        {TRAIN: [True, False, False, False], VALIDATION: [False, True], TEST: [False]}
    )
    serving(monkeypatch, Reader(table=table))
    result = steps.split_windows(snapshot="7", run_id="r1", **WINDOWS)
    assert result == {"report": "s3://bucket/runs/r1/split.json"}
    assert files["s3://bucket/runs/r1/split.json"] == {
        "rows": {"train": 4, "validation": 2, "test": 1},
        "sample_fraud_rate": {"train": 0.25, "validation": 0.5, "test": 0.0},
    }


def test_an_empty_window_fails_the_split(monkeypatch, files):
    """A window with no rows raises, naming it."""
    serving(
        monkeypatch,
        Reader(table=window_table({TRAIN: [True, False], VALIDATION: [False], TEST: []})),
    )
    with pytest.raises(ValueError, match="windows without rows: test"):
        steps.split_windows(snapshot="7", run_id="r1", **WINDOWS)


def test_tuning_with_no_trials_leaves_the_defaults():
    """With zero trials no experiment is created and the parameters are empty."""
    assert steps.tune_params(snapshot="7", run_id="r1", trials=0, **WINDOWS) == {
        "params": "{}",
        "experiment": "",
    }


@pytest.fixture
def katib(monkeypatch):
    """Replaces the cluster calls of tune_params, and returns what was created and what to answer."""
    monkeypatch.setenv("ML_IMAGE", "ml:1")
    state = SimpleNamespace(created=[], resource={})
    monkeypatch.setattr(steps.kube, "create", lambda kind, body: state.created.append((kind, body)))

    def wait(kind, name, done, timeout):
        """Applies the step's `done` check to the prepared resource."""
        return done(state.resource), state.resource

    monkeypatch.setattr(steps.kube, "wait", wait)
    return state


def optimal(**values):
    """Returns an experiment resource whose best trial has the given Katib parameter values."""
    assignments = [{"name": name, "value": str(value)} for name, value in values.items()]
    return {
        "status": {
            "conditions": [{"type": "Succeeded", "status": "True"}],
            "currentOptimalTrial": {"parameterAssignments": assignments},
        }
    }


def test_the_experiment_searches_every_hyperparameter_and_runs_one_trial_job_per_trial(katib):
    """The experiment lists each hyperparameter's range and a TrainJob template whose args take trial values."""
    katib.resource = optimal(maxDepth=5, eta=0.1, regLambda=1.5)
    result = steps.tune_params(snapshot="7", run_id="abcdef123456", trials=8, parallel=3, **WINDOWS)
    ((kind, body),) = katib.created
    assert kind == steps.kube.EXPERIMENT and body["metadata"]["name"] == "payment-risk-abcdef12"
    spec = body["spec"]
    assert spec["maxTrialCount"] == 8 and spec["parallelTrialCount"] == 3
    assert [p["name"] for p in spec["parameters"]] == [
        "maxDepth",
        "eta",
        "subsample",
        "colsampleBytree",
        "minChildWeight",
        "regLambda",
    ]
    assert spec["parameters"][0] == {
        "name": "maxDepth",
        "parameterType": "int",
        "feasibleSpace": {"min": "3", "max": "10"},
    }
    template = spec["trialTemplate"]
    assert [p["reference"] for p in template["trialParameters"]] == [
        *(p["name"] for p in spec["parameters"]),
        "${trialSpec.Name}",
    ]
    trial = template["trialSpec"]
    assert trial["kind"] == "TrainJob" and trial["spec"]["trainer"]["image"] == "ml:1"
    args = trial["spec"]["trainer"]["args"]
    assert args[0] == "trial" and "--max-depth" in args and "--lambda" in args
    assert args[args.index("--lambda") + 1] == "${trialParameters.regLambda}"
    assert args[args.index("--rate") + 1] == "0.05"
    (patch,) = trial["spec"]["runtimePatches"]
    pod = patch["trainingRuntimeSpec"]["template"]["spec"]["replicatedJobs"][0]["template"]["spec"][
        "template"
    ]
    assert pod["metadata"]["labels"] == {"katib.kubeflow.org/trial": "${trialParameters.trialName}"}
    assert result == {
        "params": json.dumps({"eta": 0.1, "lambda": 1.5, "max_depth": 5}),
        "experiment": "payment-risk-abcdef12",
    }


def test_an_experiment_that_fails_or_finds_nothing_raises(katib):
    """A failed experiment, one still running at the deadline, or one with no best trial raises."""
    katib.resource = {"status": {"conditions": [{"type": "Failed", "status": "True"}]}}
    with pytest.raises(RuntimeError, match="failed without an optimal trial"):
        steps.tune_params(snapshot="7", run_id="r1", trials=2, **WINDOWS)
    katib.resource = {"status": {"conditions": [{"type": "Succeeded", "status": "True"}]}}
    with pytest.raises(RuntimeError, match="succeeded without an optimal trial"):
        steps.tune_params(snapshot="7", run_id="r1", trials=2, **WINDOWS)
    katib.resource = {}
    with pytest.raises(RuntimeError, match="None without"):
        steps.tune_params(snapshot="7", run_id="r1", trials=2, **WINDOWS)


def test_the_fit_job_runs_the_worker_with_the_run_files_and_defaults_empty_params(
    monkeypatch, files
):
    """fit_on_gpu starts one TrainJob named for the run and returns where the model and record are."""
    monkeypatch.setenv("ML_IMAGE", "ml:1")
    started = []
    monkeypatch.setattr(steps.kube, "train_job", lambda *a: started.append(a))
    result = steps.fit_on_gpu(snapshot="7", run_id="abcdef123456", params="", **WINDOWS)
    assert result == {
        "model_uri": "s3://bucket/runs/abcdef123456/model",
        "fit": "s3://bucket/runs/abcdef123456/fit.json",
    }
    ((name, labels, image, command, args, timeout),) = started
    assert name == "payment-risk-fit-abcdef12" and image == "ml:1" and timeout == 5400
    assert labels == {"app.kubernetes.io/name": "payment-risk", "pipeline-run": "abcdef123456"}
    assert command == ["python", "-m", "risk_model.worker"]
    assert args[0] == "fit" and args[args.index("--params") + 1] == "{}"
    assert args[args.index("--model-dir") + 1] == "s3://bucket/runs/abcdef123456/model"
    assert args[args.index("--run-dir") + 1] == "s3://bucket/runs/abcdef123456"


class Registry:
    """A registry fake with a production version and recorded registrations and promotions."""

    def __init__(self, production=None, version="v3"):
        self.current, self.version = production, version
        self.registered, self.promoted, self.puts, self.challenged = [], [], [], []
        self.running, self.retired = None, []

    def production(self):
        """Returns the production version."""
        return self.current

    def artifact_uri(self, version):
        """Returns the production model's directory."""
        return "s3://bucket/production/"

    def next_version(self):
        """Returns the version to register."""
        return self.version

    def register(self, version, uri, properties):
        """Records a registration."""
        self.registered.append((version, uri, properties))

    def require_passing(self, version):
        """Accepts every version."""
        return {}

    def promote(self, version, run_id):
        """Records a promotion."""
        self.promoted.append((version, run_id))

    def challenger(self):
        """Returns the live challenger."""
        return self.running

    def retire(self, version):
        """Records an archived version."""
        self.retired.append(version)

    def mark_challenger(self, version, run_id):
        """Records a challenger."""
        self.challenged.append((version, run_id))


def launched(monkeypatch, files, production=None):
    """Fakes the TrainJob and the files the GPU gate writes. Returns the started jobs and the registry."""
    monkeypatch.setenv("ML_IMAGE", "ml:1")
    serving(monkeypatch, Reader())
    registry = Registry(production=production)
    monkeypatch.setattr(steps, "RegistryClient", lambda: registry)
    started = []
    monkeypatch.setattr(steps.kube, "train_job", lambda *a: started.append(a))
    files["s3://bucket/runs/abcdef123456/gate.json"] = {
        "passed": True,
        "reasons": [],
        "metrics": {},
    }
    monkeypatch.setattr(steps, "read_text", lambda uri: f"<svg from {uri}>")
    return started, registry


def test_the_gate_runs_the_gpu_job_and_reads_back_its_report(monkeypatch, files):
    """gate starts one TrainJob for the run and returns the verdict, report path, summary and chart."""
    started, _ = launched(monkeypatch, files)
    result = steps.gate(
        snapshot="7", model_uri="s3://bucket/runs/r1/model/", run_id="abcdef123456", **WINDOWS
    )
    ((name, labels, image, command, args, timeout),) = started
    assert name == "payment-risk-gate-abcdef12" and image == "ml:1" and timeout == 3600
    assert labels == {"app.kubernetes.io/name": "payment-risk", "pipeline-run": "abcdef123456"}
    assert command == ["python", "-m", "risk_model.worker"] and args[0] == "gate"
    assert args[args.index("--model-dir") + 1] == "s3://bucket/runs/r1/model/"
    assert args[args.index("--run-dir") + 1] == "s3://bucket/runs/abcdef123456"
    assert (
        args[args.index("--production") + 1] == ""
        and args[args.index("--production-uri") + 1] == ""
    )
    assert args[args.index("--margin") + 1] == "0.005"
    assert (
        result["passed"] is True and result["report"] == "s3://bucket/runs/abcdef123456/gate.json"
    )
    assert result["pr_curve"] == "<svg from s3://bucket/runs/abcdef123456/pr_curve.html>"
    summary = json.loads(result["summary"])
    assert summary["training_set_snapshot"] == 7 and summary["passed"] is True


def test_the_gate_hands_the_production_version_and_its_directory_to_the_job(monkeypatch, files):
    """With a production version the job is told its name and where its model.json lives."""
    started, _ = launched(monkeypatch, files, production="v2")
    steps.gate(
        snapshot="7", model_uri="s3://bucket/runs/r1/model", run_id="abcdef123456", **WINDOWS
    )
    ((*_, args, _timeout),) = started
    assert args[args.index("--production") + 1] == "v2"
    assert args[args.index("--production-uri") + 1] == "s3://bucket/production/"


def test_experiments_write_the_spec_run_one_job_and_name_the_results(monkeypatch, files):
    """experiments uploads the spec under a fresh directory, starts one TrainJob and returns the results path."""
    monkeypatch.setenv("ML_IMAGE", "ml:1")
    texts, started = {}, []
    monkeypatch.setattr(
        steps, "write_text", lambda uri, text, kind: texts.update({uri: (text, kind)})
    )
    monkeypatch.setattr(steps.kube, "train_job", lambda *a: started.append(a))
    first = steps.experiments(run_id="r1", spec='{"fits": []}')
    second = steps.experiments(run_id="r1", spec='{"fits": []}')
    assert first["results"] != second["results"]
    root = first["results"].removesuffix("/experiments.json")
    assert root.startswith("s3://bucket/runs/r1/experiments-")
    assert texts[f"{root}/spec.json"] == ('{"fits": []}', "application/json")
    ((name, labels, image, command, args, timeout), _) = started
    assert name.startswith("payment-risk-experiments-") and image == "ml:1" and timeout == 3600
    assert args == ["experiments", "--spec", f"{root}/spec.json", "--run-dir", root]
    assert labels["pipeline-run"] == "r1"


def test_registering_a_version_packages_it_for_triton_with_its_provenance(monkeypatch, files):
    """record_version writes the Triton repository entry for the version and registers the run's facts."""
    serving(monkeypatch, Reader())
    registry = Registry(version="v4")
    monkeypatch.setattr(steps, "RegistryClient", lambda: registry)
    packaged = []
    monkeypatch.setattr(steps.triton, "package", lambda *args: packaged.append(args))
    files["gate"] = {"thresholds": {"decline": 0.9}, "metrics": {"model": {"pr_auc": 0.5}}}
    reference = {"a": {"edges": [0.5], "share": [0.4, 0.5, 0.1], "gain": 2.0}}
    files["fit"] = {
        "gpu": "RTX",
        "environment": {"xgboost": "2.1.4"},
        "seconds": 12.5,
        "reference": reference,
    }
    result = steps.record_version(
        snapshot="7",
        model_uri="s3://bucket/runs/r1/model",
        report="gate",
        fit="fit",
        params='{"eta": 0.1}',
        run_id="r1",
        trigger="drift",
    )
    assert result == {"version": "v4"}
    assert packaged == [("s3://bucket/runs/r1/model", "v4", FEATURES, {"a": [0.5]}, 0.9)]
    ((version, uri, properties),) = registry.registered
    assert (version, uri) == ("v4", "s3://bucket/runs/r1/model")
    assert properties["gate"] == "passed" and properties["test_pr_auc"] == 0.5
    assert properties["pipeline_run"] == "r1" and properties["experiment"] is None
    assert properties["reference"] == reference and properties["trigger"] == "drift"
    assert properties["training_set_snapshot"] == 7 and properties["gpu"] == "RTX"
    assert properties["thresholds"] == {"decline": 0.9} and properties["params"] == '{"eta": 0.1}'


@pytest.fixture
def serve(monkeypatch):
    """Fakes the cluster side of roll_out and returns what it did and what the predictor answers."""
    monkeypatch.setenv("SERVING_PREDICTOR", json.dumps({"minReplicas": 1}))
    state = SimpleNamespace(
        deployed=[], probed=[], probe_error=None, error=None, names=[], deleted=[]
    )
    registry = Registry(production="v1")
    monkeypatch.setattr(steps, "RegistryClient", lambda: registry)

    def deploy(version, predictor, name="payment-risk"):
        """Records the deployment and the service it went to."""
        state.deployed.append((version, predictor))
        state.names.append(name)

    monkeypatch.setattr(steps.kube, "inference_service", deploy)
    monkeypatch.setattr(steps.kube, "delete", lambda kind, name: state.deleted.append(name))

    def ready(name, version):
        """Raises the prepared error, or returns an address."""
        if state.error:
            raise state.error
        return "http://svc"

    monkeypatch.setattr(steps.kube, "wait_ready", ready)

    def probe(address, version):
        """Records the probe, or raises the prepared error."""
        state.probed.append((address, version))
        if state.probe_error:
            raise state.probe_error

    monkeypatch.setattr(steps.triton, "probe", probe)
    state.registry = registry
    return state


def test_a_version_that_serves_a_score_is_promoted(serve):
    """roll_out deploys the version with the chart's settings, probes Triton and promotes it."""
    assert steps.roll_out(version="v2", run_id="r1") == {"deployed": "v2", "previous": "v1"}
    assert serve.deployed == [("v2", {"minReplicas": 1})]
    assert serve.registry.promoted == [("v2", "r1")] and serve.probed == [("http://svc", "v2")]


def test_a_probe_that_fails_restores_the_previous_model(serve):
    """A wrong version or impossible score from Triton redeploys production, promotes nothing and fails."""
    serve.probe_error = RuntimeError("payment-risk answered version 9 instead of a v2 score")
    with pytest.raises(RuntimeError, match="instead of a v2 score"):
        steps.roll_out(version="v2", run_id="r1")
    assert [v for v, _ in serve.deployed] == ["v2", "v1"] and serve.registry.promoted == []
    assert serve.probed == [("http://svc", "v2")]


def test_a_rollout_that_never_becomes_ready_restores_the_previous_model_and_reraises(serve):
    """An error while waiting redeploys production and propagates."""
    serve.error = TimeoutError("not ready")
    with pytest.raises(TimeoutError):
        steps.roll_out(version="v2", run_id="r1")
    assert [v for v, _ in serve.deployed] == ["v2", "v1"]


def test_there_is_nothing_to_restore_for_a_first_or_repeated_version(serve):
    """With no production, or when production is the version itself, a failure restores nothing."""
    serve.error = TimeoutError("not ready")
    serve.registry.current = None
    with pytest.raises(TimeoutError):
        steps.roll_out(version="v2", run_id="r1")
    serve.registry.current = "v2"
    with pytest.raises(TimeoutError):
        steps.roll_out(version="v2", run_id="r1")
    assert [v for v, _ in serve.deployed] == ["v2", "v2"]


def test_a_challenger_is_served_beside_the_champion_and_recorded_as_the_challenger(serve):
    """A challenger goes to its own service, is probed, and is marked without touching production."""
    result = steps.roll_out(version="v2", run_id="r1", role="challenger")
    assert result == {"deployed": "v2", "previous": "v1"}
    assert serve.names == ["payment-risk-challenger"] and serve.probed == [("http://svc", "v2")]
    assert serve.registry.challenged == [("v2", "r1")] and serve.registry.promoted == []


def test_a_challenger_that_fails_is_removed_and_the_champion_is_left_alone(serve):
    """A failing challenger is deleted and raises, and nothing is promoted or marked."""
    serve.probe_error = RuntimeError("wrong version")
    with pytest.raises(RuntimeError, match="wrong version"):
        steps.roll_out(version="v2", run_id="r1", role="challenger")
    assert serve.deleted == ["payment-risk-challenger"] and serve.names == [
        "payment-risk-challenger"
    ]
    assert serve.registry.challenged == [] and serve.registry.promoted == []


def test_a_challenger_needs_a_different_production_version(serve):
    """With no production version, or production being the same version, there is nothing to run beside."""
    for current in (None, "v2"):
        serve.registry.current = current
        with pytest.raises(ValueError, match="different production"):
            steps.roll_out(version="v2", run_id="r1", role="challenger")
    assert serve.names == []


@pytest.fixture
def promoting(serve, monkeypatch):
    """Adds a challenger and fakes the route weights and the analysis, which passes unless told otherwise."""
    serve.registry.running = "v2"
    serve.weights, serve.analyses, serve.verdict = [], [], ("Successful", "")
    monkeypatch.setenv("SERVING_PREDICTOR", json.dumps({"minReplicas": 1}))
    monkeypatch.setattr(steps.kube, "set_weights", lambda percent: serve.weights.append(percent))

    def analyse(name, template, args, timeout):
        """Records the analysis and answers the prepared verdict."""
        serve.analyses.append((name, template, args))
        return serve.verdict if len(serve.analyses) >= serve.fail_at else ("Successful", "")

    serve.fail_at = 99
    monkeypatch.setattr(steps.kube, "analysis_run", analyse)
    return serve


def test_a_challenger_that_passes_every_step_replaces_the_champion(promoting):
    """Weights rise step by step with an analysis each, then drop to zero while the champion is updated."""
    result = steps.promote_challenger(run_id="abcdef0123", steps_percent="5,20")
    assert result == {"promoted": "v2", "previous": "v1"}
    assert promoting.weights == [5, 20, 0]
    assert [(n, t) for n, t, _ in promoting.analyses] == [
        ("payment-risk-abcdef01-5", "payment-risk-challenger"),
        ("payment-risk-abcdef01-20", "payment-risk-challenger"),
    ]
    assert promoting.analyses[0][2] == {
        "challengerVersion": "v2",
        "championVersion": "v1",
        "window": "10m",
    }
    assert promoting.names == ["payment-risk"] and promoting.deployed[0][0] == "v2"
    assert promoting.registry.promoted == [("v2", "abcdef0123")] and promoting.probed == [
        ("http://svc", "v2")
    ]
    assert promoting.deleted == ["payment-risk-challenger"] and promoting.registry.retired == []


def test_a_failed_analysis_removes_the_challenger_and_leaves_the_champion(promoting):
    """The weights return to zero, the challenger service goes, its version is archived and the run fails."""
    promoting.fail_at, promoting.verdict = 2, ("Failed", "decline-shift")
    with pytest.raises(
        RuntimeError, match="v2 failed its analysis at 20 percent: Failed decline-shift"
    ):
        steps.promote_challenger(run_id="abcdef0123", steps_percent="5,20")
    assert promoting.weights == [5, 20, 0]
    assert promoting.deleted == ["payment-risk-challenger"] and promoting.registry.retired == ["v2"]
    assert promoting.names == [] and promoting.registry.promoted == []


def test_a_champion_update_that_fails_restores_the_champion_and_removes_the_challenger(promoting):
    """If the new champion does not answer, the old version is redeployed and the challenger is archived."""
    promoting.probe_error = RuntimeError("no score")
    with pytest.raises(RuntimeError, match="no score"):
        steps.promote_challenger(run_id="abcdef0123", steps_percent="5")
    assert [v for v, _ in promoting.deployed] == ["v2", "v1"]
    assert promoting.deleted == ["payment-risk-challenger"] and promoting.registry.retired == ["v2"]
    assert promoting.registry.promoted == []


@pytest.mark.parametrize("plan", ["30", "20,5", "5,5", "0,5", "5,25"])
def test_steps_must_rise_from_above_zero_to_the_cap(promoting, plan):
    """A step above the 20 percent cap, a falling, repeated or zero step is refused before any weight moves."""
    with pytest.raises(ValueError, match="rise from above 0"):
        steps.promote_challenger(run_id="abcdef0123", steps_percent=plan)
    assert promoting.weights == [] and promoting.analyses == []


def test_the_chart_steps_and_window_apply_without_a_request_and_a_missing_challenger_is_refused(
    promoting, monkeypatch
):
    """The environment supplies the steps and window, and there must be a challenger beside a champion."""
    monkeypatch.setenv("EXPERIMENT_STEPS", "10")
    monkeypatch.setenv("EXPERIMENT_WINDOW", "30m")
    steps.promote_challenger(run_id="abcdef0123")
    assert promoting.weights == [10, 0] and promoting.analyses[0][2]["window"] == "30m"
    promoting.registry.running = None
    with pytest.raises(ValueError, match="no challenger"):
        steps.promote_challenger(run_id="abcdef0123")


def test_the_champion_is_labelled_for_the_waypoint_only_when_the_chart_has_one(serve, monkeypatch):
    """A rollout labels the Service when SERVING_WAYPOINT is set, and does nothing about it otherwise."""
    labelled = []
    monkeypatch.setattr(steps.kube, "use_waypoint", lambda: labelled.append(True))
    steps.roll_out(version="v2", run_id="r1")
    assert labelled == []
    monkeypatch.setenv("SERVING_WAYPOINT", "ml-waypoint")
    steps.roll_out(version="v3", run_id="r2")
    assert labelled == [True]
