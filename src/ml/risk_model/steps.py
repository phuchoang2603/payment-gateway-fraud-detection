"""The pipeline steps: each is a function run as `python -m risk_model.steps <step>` in a pipeline pod."""

import argparse
import inspect
import json
import os
import tempfile
import time
from dataclasses import asdict
from datetime import date, timedelta
from pathlib import Path
from urllib.parse import urlsplit

import pyarrow.compute as pc
from fraud_feast.lake import catalog, read_json, s3, write_json
from sklearn.metrics import precision_recall_curve

from risk_model import kube
from risk_model.data import DatasetReader
from risk_model.evaluate import MARGIN, GateEvaluator, pr_curve_html
from risk_model.registry import RegistryClient
from risk_model.schedule import Pipelines, recurring_body
from risk_model.split import WindowSplitter
from risk_model.train import HYPERPARAMETERS, RiskTrainer, load_booster, score
from risk_model.windows import covered_end, is_newer, reference_day, rolling


def directory(run_id):
    """Returns the run directory in object storage for a pipeline run ID."""
    return f"{os.environ['RUN_ROOT'].rstrip('/')}/{run_id}"


def download(uri, target):
    """Downloads an `s3://` URI to the local path `target` and returns the path as text."""
    source = urlsplit(uri)
    s3().download_file(source.netloc, source.path.lstrip("/"), str(target))
    return str(target)


def flags(**values):
    """Returns `--name value` command-line parts for keyword values, with underscores as dashes."""
    return [
        part
        for name, value in values.items()
        for part in (f"--{name.replace('_', '-')}", str(value))
    ]


def plan_windows(
    *,
    run_id,
    schedule_time="",
    create_time="",
    train_days: int = 56,
    validation_days: int = 14,
    test_days: int = 14,
    lag_days: int = 1,
    force: bool = False,
    fixed_train="",
    fixed_validation="",
    fixed_test="",
    fixed_cutoff="",
):
    """Chooses the windows and cutoff of a run, and whether it has new data to train on.

    All four `fixed_*` values override the rule. Otherwise the windows end on the latest day, at or
    before the schedule time minus `lag_days`, whose whole span the stream tables cover. The run
    proceeds unless that cutoff is not newer than the newest registered model's, or `force` is set.
    """
    if fixed_train and fixed_validation and fixed_test and fixed_cutoff:
        WindowSplitter(fixed_train, fixed_validation, fixed_test)
        return {
            "train": fixed_train,
            "validation": fixed_validation,
            "test": fixed_test,
            "cutoff": fixed_cutoff,
            "run": True,
        }
    reference = reference_day(schedule_time, create_time)
    span = train_days + validation_days + test_days
    covered = covered_end(catalog(), span, reference - timedelta(days=lag_days))
    plan = rolling(
        covered or reference - timedelta(days=lag_days), train_days, validation_days, test_days
    )
    registered = RegistryClient().latest_cutoff()
    run = covered is not None and (force or is_newer(plan.cutoff, registered))
    write_json(
        f"{directory(run_id)}/windows.json",
        {
            "reference": str(reference),
            "covered_end": None if covered is None else str(covered),
            "registered_cutoff": registered,
            "windows": [str(plan.train), str(plan.validation), str(plan.test)],
            "cutoff": plan.cutoff,
            "run": run,
        },
    )
    return {
        "train": str(plan.train),
        "validation": str(plan.validation),
        "test": str(plan.test),
        "cutoff": plan.cutoff,
        "run": run,
    }


def ensure_schedule(
    *,
    version,
    cron,
    enabled: bool = True,
    train_days: int = 56,
    validation_days: int = 14,
    test_days: int = 14,
    lag_days: int = 1,
    timeout: int = 900,
):
    """Points the recurring retraining run at a pipeline version, waiting for it to appear in the API."""
    pipelines = Pipelines()
    pipeline_id, version_id = pipelines.version(version, timeout)
    body = recurring_body(
        pipeline_id=pipeline_id,
        version_id=version_id,
        experiment_id=pipelines.experiment(),
        cron=cron,
        enabled=enabled,
        parameters={
            "train_days": train_days,
            "validation_days": validation_days,
            "test_days": test_days,
            "lag_days": lag_days,
        },
    )
    return {"recurring_run": pipelines.replace(body)}


def plan_examples(
    *, train, validation, test, cutoff, run_id, existing_snapshot="", chunk_days: int = 16
):
    """Fences the sources, prepares staging and returns the day chunks to stage, none for an existing snapshot."""
    if existing_snapshot:
        return {"chunks": "[]"}
    from risk_model.dataset import chunks, feast_store, fence_path, prepare, session

    splitter = WindowSplitter(train, validation, test)
    store = feast_store()
    spark = session(store)
    try:
        before = prepare(spark, store, splitter.start, splitter.end)
    finally:
        spark.stop()
    write_json(fence_path(directory(run_id)), {"before": before})
    pieces = chunks(splitter.start, splitter.end, chunk_days)
    return {"chunks": json.dumps([f"{first}/{last}" for first, last in pieces])}


def stage_examples(*, chunk, cutoff):
    """Stages one `first/last` chunk of days and returns the number of rows staged."""
    from risk_model.dataset import feast_store, session, stage

    first, last = (date.fromisoformat(day) for day in chunk.split("/"))
    store = feast_store()
    spark = session(store)
    try:
        return {"rows": str(stage(spark, store, first, last, cutoff))}
    finally:
        spark.stop()


def assemble_examples(*, train, validation, test, cutoff, run_id, existing_snapshot=""):
    """Merges the staged chunks into a new training-set version and returns its snapshot ID.

    A given `existing_snapshot` is only checked and returned.
    """
    splitter = WindowSplitter(train, validation, test)
    if existing_snapshot:
        return {"snapshot": str(DatasetReader.at(existing_snapshot).snapshot_id)}
    from risk_model.dataset import feast_store, fence_path, merge_examples, session

    started = time.monotonic()
    store = feast_store()
    spark = session(store)
    try:
        before = read_json(fence_path(directory(run_id)))["before"]
        built = merge_examples(spark, store, splitter.start, splitter.end, cutoff, before)
    finally:
        spark.stop()
    write_json(
        f"{directory(run_id)}/examples.json",
        {**asdict(built), "seconds": round(time.monotonic() - started, 1)},
    )
    return {"snapshot": str(built.snapshot_id)}


def check_examples(
    *,
    snapshot,
    run_id,
    fraud_rate_min: float = 0.0005,
    fraud_rate_max: float = 0.25,
):
    """Validates a training-set version, writes the report and fails the step when it is invalid."""
    report = DatasetReader.at(snapshot).validate((fraud_rate_min, fraud_rate_max))
    uri = f"{directory(run_id)}/check.json"
    write_json(uri, report)
    if not report["ok"]:
        raise SystemExit(f"training set {snapshot} failed validation: {report['failures']}")
    return {"report": uri}


def split_windows(*, snapshot, train, validation, test, run_id):
    """Counts the rows and the sampled fraud rate of each window and fails if a window is empty."""
    splitter = WindowSplitter(train, validation, test)
    table = DatasetReader.at(snapshot).scan(columns=["started_at", "label"])
    rows = splitter.counts(table["started_at"])
    empty = [name for name, count in rows.items() if not count]
    if empty:
        raise ValueError(f"windows without rows: {', '.join(empty)}")
    fraud = {}
    for name in rows:
        selected = table.filter(splitter.mask(table["started_at"], name))
        fraud[name] = pc.sum(selected["label"]).as_py() / selected.num_rows
    uri = f"{directory(run_id)}/split.json"
    write_json(uri, {"rows": rows, "sample_fraud_rate": fraud})
    return {"report": uri}


def tune_params(
    *,
    snapshot,
    train,
    validation,
    test,
    run_id,
    trials: int = 0,
    parallel: int = 2,
    tune_rate: float = 0.05,
    timeout_seconds: int = 7200,
):
    """Runs a Katib experiment of `trials` GPU trials and returns the best hyperparameters as JSON.

    With no trials it returns empty parameters, so the trainer uses its defaults.
    """
    if not trials:
        return {"params": "{}", "experiment": ""}
    arguments = [
        "trial",
        *flags(
            seed=7, rate=tune_rate, snapshot=snapshot, train=train, validation=validation, test=test
        ),
        *flags(**{h.xgboost: f"${{trialParameters.{h.katib}}}" for h in HYPERPARAMETERS}),
    ]
    name = f"payment-risk-{run_id[:8]}"
    body = {
        "apiVersion": "kubeflow.org/v1beta1",
        "kind": "Experiment",
        "metadata": {"name": name, "namespace": kube.namespace()},
        "spec": {
            "objective": {"type": "maximize", "objectiveMetricName": "aucpr"},
            "algorithm": {"algorithmName": "tpe"},
            "parallelTrialCount": parallel,
            "maxTrialCount": trials,
            "maxFailedTrialCount": 2,
            "parameters": [
                {
                    "name": h.katib,
                    "parameterType": h.kind,
                    "feasibleSpace": {"min": h.low, "max": h.high},
                }
                for h in HYPERPARAMETERS
            ],
            "trialTemplate": {
                "primaryContainerName": "node",
                "trialParameters": [
                    {"name": h.katib, "reference": h.katib} for h in HYPERPARAMETERS
                ],
                "trialSpec": kube.train_job_spec(
                    os.environ["ML_IMAGE"],
                    ["python", "-m", "risk_model.worker"],
                    arguments,
                    timeout_seconds,
                ),
            },
        },
    }
    kube.create(kube.EXPERIMENT, body)

    def done(resource):
        if kube.condition(resource, "Succeeded"):
            return "succeeded"
        if kube.condition(resource, "Failed"):
            return "failed"
        return None

    outcome, resource = kube.wait(kube.EXPERIMENT, name, done, timeout_seconds)
    optimal = resource.get("status", {}).get("currentOptimalTrial", {})
    if outcome != "succeeded" or not optimal.get("parameterAssignments"):
        raise RuntimeError(f"Experiment {name} {outcome} without an optimal trial")
    assigned = {item["name"]: item["value"] for item in optimal["parameterAssignments"]}
    by_name = {h.katib: h for h in HYPERPARAMETERS}
    params = {
        by_name[name].xgboost: by_name[name].convert(value) for name, value in assigned.items()
    }
    return {"params": json.dumps(params, sort_keys=True), "experiment": name}


def fit_on_gpu(
    *,
    snapshot,
    train,
    validation,
    test,
    params,
    run_id,
    timeout_seconds: int = 5400,
):
    """Trains the model in a one-GPU TrainJob and returns where its files and run record are."""
    root = directory(run_id)
    arguments = [
        "fit",
        *flags(
            snapshot=snapshot,
            train=train,
            validation=validation,
            test=test,
            params=params or "{}",
            model_dir=f"{root}/model",
            run_dir=root,
        ),
    ]
    kube.train_job(
        f"payment-risk-fit-{run_id[:8]}",
        {"app.kubernetes.io/name": "payment-risk", "pipeline-run": run_id},
        os.environ["ML_IMAGE"],
        ["python", "-m", "risk_model.worker"],
        arguments,
        timeout_seconds,
    )
    return {"model_uri": f"{root}/model", "fit": f"{root}/fit.json"}


def gate(*, snapshot, train, validation, test, model_uri, run_id, margin: float = MARGIN):
    """Measures the new model against the baseline and production on the test window and decides.

    Returns whether it passed, the report location, a JSON summary and a precision-recall chart.
    """
    splitter = WindowSplitter(train, validation, test)
    reader = DatasetReader.at(snapshot)
    training, tuning, holdout = (
        reader.read(splitter[name]) for name in ("train", "validation", "test")
    )
    registry = RegistryClient()
    current = registry.production()
    with tempfile.TemporaryDirectory() as scratch:
        files = Path(scratch)
        booster, features = load_booster(
            download(f"{model_uri.rstrip('/')}/model.json", files / "model.json")
        )
        if features != reader.features:
            raise ValueError("model feature order differs from the training set's")
        models = {"model": booster, "baseline": RiskTrainer(device="cpu").baseline(training)}
        columns = {"model": slice(None), "baseline": slice(None)}
        if current:
            older, order = load_booster(
                download(
                    f"{registry.artifact_uri(current).rstrip('/')}/model.json",
                    files / "production.json",
                )
            )
            models["production"] = older
            columns["production"] = [reader.features.index(name) for name in order]
    evaluator = GateEvaluator(margin)
    thresholds, scores = {}, {}
    for name, model in models.items():
        thresholds[name] = evaluator.thresholds(
            tuning.y, score(model, tuning.x[:, columns[name]]), tuning.w
        )
        scores[name] = score(model, holdout.x[:, columns[name]])
    metrics = evaluator.evaluate(thresholds, holdout.y, holdout.w, scores)
    verdict = evaluator.gate(metrics)
    root = directory(run_id)
    report = {
        "passed": verdict.passed,
        "reasons": verdict.reasons,
        "metrics": metrics,
        "thresholds": thresholds["model"],
        "production": current,
        "margin": margin,
        "rows": {"validation": len(tuning.y), "test": len(holdout.y)},
    }
    write_json(f"{root}/gate.json", report)
    curves = {
        name: precision_recall_curve(holdout.y, values, sample_weight=holdout.w)[:2]
        for name, values in scores.items()
    }
    return {
        "passed": verdict.passed,
        "report": f"{root}/gate.json",
        "summary": json.dumps({**report, **reader.provenance()}),
        "pr_curve": pr_curve_html(curves),
    }


def record_version(*, snapshot, model_uri, report, fit, params, run_id, experiment=""):
    """Registers a passing model with its provenance and metrics, and returns its version name."""
    provenance = DatasetReader.at(snapshot).provenance()
    gate_report, run = read_json(report), read_json(fit)
    registry = RegistryClient()
    version = registry.next_version()
    target = urlsplit(model_uri)
    s3().put_object(
        Bucket=target.netloc,
        Key=f"{target.path.strip('/')}/version.txt",
        Body=f"{version}\n".encode(),
    )
    registry.register(
        version,
        model_uri,
        {
            "gate": "passed",
            "feature_service": provenance["feature_service"],
            "features": provenance["features"],
            "training_set_snapshot": provenance["training_set_snapshot"],
            "sources": provenance["sources"],
            "window": provenance["window"],
            "cutoff": provenance["cutoff"],
            "sample_rate": provenance["sample_rate"],
            "pipeline_run": run_id,
            "params": params,
            "experiment": experiment or None,
            "thresholds": gate_report["thresholds"],
            "gpu": run["gpu"],
            "environment": run["environment"],
            "seconds": run["seconds"],
            **{f"test_{name}": value for name, value in gate_report["metrics"]["model"].items()},
        },
    )
    return {"version": version}


def roll_out(*, version, run_id):
    """Serves a passing version, checks it answers with a score, then promotes it.

    A version that fails to serve or to score restores the previous production version and raises.
    """
    registry = RegistryClient()
    registry.require_passing(version)
    previous = registry.production()
    predictor = json.loads(os.environ.get("SERVING_PREDICTOR", "{}"))
    kube.inference_service(version, predictor)
    try:
        address = kube.wait_ready(kube.INFERENCE_NAME, version)
        answer = kube.predict(address, [{}])
        (result,) = answer["predictions"]
        if answer["model_version"] != version or not 0.0 <= result <= 1.0:
            raise RuntimeError(f"payment-risk answered {answer} instead of a {version} score")
    except Exception:
        if previous and previous != version:
            kube.inference_service(previous, predictor)
        raise
    registry.promote(version, run_id)
    return {"deployed": version, "previous": previous or ""}


STEPS = {
    function.__name__: function
    for function in (
        plan_windows,
        ensure_schedule,
        plan_examples,
        stage_examples,
        assemble_examples,
        check_examples,
        split_windows,
        tune_params,
        fit_on_gpu,
        gate,
        record_version,
        roll_out,
    )
}


def truth(text):
    """Parses a command-line boolean: `true` in any case, or `1`."""
    return str(text).lower() in ("true", "1")


def text(value):
    """Returns a step result as the text written to its output file; booleans become `true`/`false`."""
    return ("true" if value else "false") if isinstance(value, bool) else str(value)


def argument_type(annotation):
    """Returns the argparse type for a step parameter's annotation: int, float, bool or text."""
    return {int: int, float: float, bool: truth}.get(annotation, str)


def main(argv=None):
    """Runs the step named on the command line and writes each `--out NAME PATH` result.

    Each parameter of the step function becomes a `--dashed-name` option, typed by its annotation.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("step", choices=sorted(STEPS))
    parser.add_argument("--out", nargs=2, action="append", default=[], metavar=("NAME", "PATH"))
    arguments, remaining = parser.parse_known_args(argv)
    function = STEPS[arguments.step]
    options = argparse.ArgumentParser()
    for parameter in inspect.signature(function).parameters.values():
        required = parameter.default is inspect.Parameter.empty
        options.add_argument(
            f"--{parameter.name.replace('_', '-')}",
            dest=parameter.name,
            type=argument_type(parameter.annotation),
            required=required,
            default=None if required else parameter.default,
        )
    result = function(**vars(options.parse_args(remaining)))
    for name, path in arguments.out:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        Path(path).write_text(text(result[name]))


if __name__ == "__main__":
    main()
