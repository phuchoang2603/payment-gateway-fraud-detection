"""The pipeline steps: each is a function run as `python -m risk_model.steps <step>` in a pipeline pod."""

import argparse
import inspect
import json
import os
import time
import uuid
from dataclasses import asdict
from datetime import date, timedelta
from pathlib import Path

import pyarrow.compute as pc
from fraud_feast.lake import catalog, read_json, write_json

from risk_model import drift, kube, triton
from risk_model.data import CHUNK_DAYS, DatasetReader
from risk_model.evaluate import MARGIN
from risk_model.objects import read_text, write_text
from risk_model.registry import RegistryClient
from risk_model.split import WindowSplitter
from risk_model.train import HYPERPARAMETERS
from risk_model.windows import covered_end, is_newer, reference_day, rolling


def directory(run_id):
    """Returns the run directory in object storage for a pipeline run ID."""
    return f"{os.environ['RUN_ROOT'].rstrip('/')}/{run_id}"


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
    before the creation time minus `lag_days`, whose whole span the stream tables cover. The run
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
    reference = reference_day(create_time)
    span = train_days + validation_days + test_days
    covered = covered_end(catalog(), span, reference - timedelta(days=lag_days))
    plan = rolling(
        covered or reference - timedelta(days=lag_days), train_days, validation_days, test_days
    )
    registered = RegistryClient().latest_cutoff()
    chosen = {
        "train": str(plan.train),
        "validation": str(plan.validation),
        "test": str(plan.test),
        "cutoff": plan.cutoff,
        "run": covered is not None and (force or is_newer(plan.cutoff, registered)),
    }
    write_json(
        f"{directory(run_id)}/windows.json",
        {
            "reference": str(reference),
            "covered_end": None if covered is None else str(covered),
            "registered_cutoff": registered,
            **chosen,
        },
    )
    return chosen


def plan_examples(
    *, train, validation, test, run_id, existing_snapshot="", chunk_days: int = CHUNK_DAYS
):
    """Fences the sources, prepares staging and returns the day chunks to stage, none for an existing snapshot."""
    if existing_snapshot:
        return {"chunks": "[]"}
    from risk_model.dataset import chunks, fence_path, prepare, spark_session

    splitter = WindowSplitter(train, validation, test)
    with spark_session() as (store, spark):
        before = prepare(spark, store, splitter.start, splitter.end)
    write_json(fence_path(directory(run_id)), {"before": before})
    pieces = chunks(splitter.start, splitter.end, chunk_days)
    return {"chunks": json.dumps([f"{first}/{last}" for first, last in pieces])}


def stage_examples(*, chunk, cutoff):
    """Stages one `first/last` chunk of days and returns the number of rows staged."""
    from risk_model.dataset import spark_session, stage

    first, last = (date.fromisoformat(day) for day in chunk.split("/"))
    with spark_session() as (store, spark):
        return {"rows": str(stage(spark, store, first, last, cutoff))}


def assemble_examples(*, train, validation, test, cutoff, run_id, existing_snapshot=""):
    """Merges the staged chunks into a new training-set version and returns its snapshot ID.

    A given `existing_snapshot` is only checked and returned.
    """
    splitter = WindowSplitter(train, validation, test)
    if existing_snapshot:
        return {"snapshot": str(DatasetReader.at(existing_snapshot).snapshot_id)}
    from risk_model.dataset import fence_path, merge_examples, spark_session

    started = time.monotonic()
    with spark_session() as (store, spark):
        before = read_json(fence_path(directory(run_id)))["before"]
        built = merge_examples(spark, store, splitter.start, splitter.end, cutoff, before)
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
                    *({"name": h.katib, "reference": h.katib} for h in HYPERPARAMETERS),
                    {"name": "trialName", "reference": "${trialSpec.Name}"},
                ],
                "trialSpec": kube.train_job_spec(
                    os.environ["ML_IMAGE"],
                    ["python", "-m", "risk_model.worker"],
                    arguments,
                    timeout_seconds,
                    pod_labels={"katib.kubeflow.org/trial": "${trialParameters.trialName}"},
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


def gate(
    *,
    snapshot,
    train,
    validation,
    test,
    model_uri,
    run_id,
    margin: float = MARGIN,
    timeout_seconds: int = 3600,
):
    """Measures the new model against its baseline and production on the test window and decides.

    A GPU TrainJob scores the three models. Returns whether it passed, the report location, a JSON
    summary and a precision-recall chart.
    """
    registry = RegistryClient()
    current = registry.production()
    root = directory(run_id)
    arguments = [
        "gate",
        *flags(
            snapshot=snapshot,
            train=train,
            validation=validation,
            test=test,
            model_dir=model_uri,
            run_dir=root,
            production=current or "",
            production_uri=registry.artifact_uri(current) if current else "",
            margin=margin,
        ),
    ]
    kube.train_job(
        f"payment-risk-gate-{run_id[:8]}",
        {"app.kubernetes.io/name": "payment-risk", "pipeline-run": run_id},
        os.environ["ML_IMAGE"],
        ["python", "-m", "risk_model.worker"],
        arguments,
        timeout_seconds,
    )
    report = read_json(f"{root}/gate.json")
    return {
        "passed": report["passed"],
        "report": f"{root}/gate.json",
        "summary": json.dumps({**report, **DatasetReader.at(snapshot).provenance()}),
        "pr_curve": read_text(f"{root}/pr_curve.html"),
    }


def experiments(*, run_id, spec, timeout_seconds: int = 3600):
    """Runs the fits of a JSON spec in one GPU TrainJob and returns where their results are.

    The spec names a snapshot and a list of fits, each with its features, parameters, training
    windows, the window it tunes on and the windows it is evaluated on.
    """
    token = uuid.uuid4().hex[:8]
    root = f"{directory(run_id)}/experiments-{token}"
    write_text(f"{root}/spec.json", spec, "application/json")
    kube.train_job(
        f"payment-risk-experiments-{token}",
        {"app.kubernetes.io/name": "payment-risk", "pipeline-run": run_id},
        os.environ["ML_IMAGE"],
        ["python", "-m", "risk_model.worker"],
        ["experiments", *flags(spec=f"{root}/spec.json", run_dir=root)],
        timeout_seconds,
    )
    return {"results": f"{root}/experiments.json"}


def record_version(*, snapshot, model_uri, report, fit, params, run_id, experiment="", trigger=""):
    """Registers a passing model and packages it for Triton, and returns its version name."""
    provenance = DatasetReader.at(snapshot).provenance()
    gate_report, run = read_json(report), read_json(fit)
    registry = RegistryClient()
    version = registry.next_version()
    triton.package(
        model_uri,
        version,
        provenance["features"],
        drift.edges_of(run["reference"]),
        gate_report["thresholds"]["decline"],
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
            "reference": run["reference"],
            "trigger": trigger or "manual",
            "gpu": run["gpu"],
            "environment": run["environment"],
            "seconds": run["seconds"],
            **{f"test_{name}": value for name, value in gate_report["metrics"]["model"].items()},
        },
    )
    return {"version": version}


def roll_out(*, version, run_id, role: str = "champion"):
    """Serves a passing version as the champion or the challenger, checks it answers with a score, then records it.

    A champion that fails to serve or to score restores the previous production version and raises. A
    challenger that fails is removed and raises, and leaves the champion alone.
    """
    registry = RegistryClient()
    registry.require_passing(version)
    predictor = json.loads(os.environ.get("SERVING_PREDICTOR", "{}"))
    if role == "challenger":
        champion = registry.production()
        if champion is None or champion == version:
            raise ValueError("a challenger needs a different production version to run beside")
        kube.inference_service(version, predictor, kube.CHALLENGER_NAME)
        try:
            triton.probe(kube.wait_ready(kube.CHALLENGER_NAME, version), version)
        except Exception:
            kube.delete(kube.INFERENCE, kube.CHALLENGER_NAME)
            raise
        registry.mark_challenger(version, run_id)
        return {"deployed": version, "previous": champion}
    previous = registry.production()
    kube.inference_service(version, predictor)
    try:
        triton.probe(kube.wait_ready(kube.INFERENCE_NAME, version), version)
    except Exception:
        if previous and previous != version:
            kube.inference_service(previous, predictor)
        raise
    if os.environ.get("SERVING_WAYPOINT"):
        kube.use_waypoint()
    registry.promote(version, run_id)
    return {"deployed": version, "previous": previous or ""}


def promote_challenger(*, run_id, steps_percent: str = "", timeout: int = 3600):
    """Shifts traffic to the challenger in steps, checking each with an analysis, then promotes it or removes it.

    `steps_percent` lists the weights, at most the experiment's cap, and defaults to the chart's steps. A failed
    or erroring analysis puts the weights back to zero, removes the challenger service, archives its version and
    raises. After the last step the challenger is removed and then the champion service is updated to its
    version, so the single GPU node never holds three model servers at once.
    """
    registry = RegistryClient()
    challenger, champion = registry.challenger(), registry.production()
    if challenger is None or champion is None or challenger == champion:
        raise ValueError("there is no challenger running beside a different champion")
    cap = int(os.environ.get("EXPERIMENT_MAX_PERCENT", "20"))
    plan = [
        int(part)
        for part in (steps_percent or os.environ.get("EXPERIMENT_STEPS", "5,20")).split(",")
    ]
    if plan != sorted(set(plan)) or plan[0] <= 0 or plan[-1] > cap:
        raise ValueError(f"the steps must rise from above 0 to at most {cap} percent")
    window = os.environ.get("EXPERIMENT_WINDOW", "10m")
    predictor = json.loads(os.environ.get("SERVING_PREDICTOR", "{}"))

    def remove():
        kube.set_weights(0)
        kube.delete(kube.INFERENCE, kube.CHALLENGER_NAME)
        registry.retire(challenger)

    try:
        for percent in plan:
            kube.set_weights(percent)
            args = {"challengerVersion": challenger, "championVersion": champion, "window": window}
            phase, message = kube.analysis_run(
                f"{kube.INFERENCE_NAME}-{run_id[:8]}-{percent}",
                "payment-risk-challenger",
                args,
                timeout,
            )
            if phase != "Successful":
                raise RuntimeError(
                    f"{challenger} failed its analysis at {percent} percent: {phase} {message}"
                )
    except Exception:
        remove()
        raise
    kube.set_weights(0)
    kube.delete(kube.INFERENCE, kube.CHALLENGER_NAME)
    kube.inference_service(challenger, predictor)
    try:
        triton.probe(kube.wait_ready(kube.INFERENCE_NAME, challenger), challenger)
    except Exception:
        kube.inference_service(champion, predictor)
        registry.retire(challenger)
        raise
    registry.promote(challenger, run_id)
    return {"promoted": challenger, "previous": champion}


STEPS = {
    function.__name__: function
    for function in (
        plan_windows,
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
        promote_challenger,
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
