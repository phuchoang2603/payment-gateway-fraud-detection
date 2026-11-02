from kfp import dsl, kubernetes

IMAGE = "ghcr.io/phuchoang2603/payment-gateway-fraud-detection/ml:chart"
CONFIG = "ml-training-config"
CONFIG_KEYS = (
    "AWS_REGION",
    "S3_ENDPOINT",
    "S3_PATH_STYLE",
    "ICEBERG_REST_URI",
    "ICEBERG_WAREHOUSE",
    "ICEBERG_CLIENT_ID",
    "ICEBERG_OAUTH2_URI",
    "FEAST_REGISTRY",
    "FEAST_STREAM_EPOCHS",
    "FEAST_S3_ENDPOINT_URL",
    "MODEL_REGISTRY_URL",
    "RUN_ROOT",
    "ML_IMAGE",
    "ML_NAMESPACE",
    "SERVING_PREDICTOR",
)


def step(name, flags, outputs=()):
    args = [name]
    for flag, value in flags.items():
        args += [f"--{flag}", value]
    for output, path in outputs:
        args += ["--out", output, path]
    return dsl.ContainerSpec(IMAGE, ["python", "-m", "risk_model.steps"], args)


@dsl.container_component
def plan_examples(
    run_id: str,
    train: str,
    validation: str,
    test: str,
    cutoff: str,
    existing_snapshot: str,
    chunks: dsl.OutputPath(list),
):
    return step(
        "plan_examples",
        {
            "train": train,
            "validation": validation,
            "test": test,
            "cutoff": cutoff,
            "snapshot": existing_snapshot,
            "run-id": run_id,
        },
        [("chunks", chunks)],
    )


@dsl.container_component
def stage_examples(chunk: str, cutoff: str, rows: dsl.OutputPath(str)):
    return step("stage_examples", {"chunk": chunk, "cutoff": cutoff}, [("rows", rows)])


@dsl.container_component
def assemble_examples(
    run_id: str,
    train: str,
    validation: str,
    test: str,
    cutoff: str,
    existing_snapshot: str,
    snapshot: dsl.OutputPath(str),
):
    return step(
        "assemble_examples",
        {
            "train": train,
            "validation": validation,
            "test": test,
            "cutoff": cutoff,
            "snapshot": existing_snapshot,
            "run-id": run_id,
        },
        [("snapshot", snapshot)],
    )


@dsl.container_component
def check_examples(run_id: str, snapshot: str, report: dsl.OutputPath(str)):
    return step("check_examples", {"snapshot": snapshot, "run-id": run_id}, [("report", report)])


@dsl.container_component
def split_windows(
    run_id: str, snapshot: str, train: str, validation: str, test: str, report: dsl.OutputPath(str)
):
    return step(
        "split_windows",
        {
            "snapshot": snapshot,
            "train": train,
            "validation": validation,
            "test": test,
            "run-id": run_id,
        },
        [("report", report)],
    )


@dsl.container_component
def tune_params(
    run_id: str,
    snapshot: str,
    train: str,
    validation: str,
    test: str,
    trials: int,
    params: dsl.OutputPath(str),
    experiment: dsl.OutputPath(str),
):
    return step(
        "tune_params",
        {
            "snapshot": snapshot,
            "train": train,
            "validation": validation,
            "test": test,
            "trials": trials,
            "run-id": run_id,
        },
        [("params", params), ("experiment", experiment)],
    )


@dsl.container_component
def fit_on_gpu(
    run_id: str,
    snapshot: str,
    train: str,
    validation: str,
    test: str,
    params: str,
    model_uri: dsl.OutputPath(str),
    fit: dsl.OutputPath(str),
):
    return step(
        "fit_on_gpu",
        {
            "snapshot": snapshot,
            "train": train,
            "validation": validation,
            "test": test,
            "params": params,
            "run-id": run_id,
        },
        [("model_uri", model_uri), ("fit", fit)],
    )


@dsl.container_component
def gate(
    run_id: str,
    snapshot: str,
    train: str,
    validation: str,
    test: str,
    model_uri: str,
    passed: dsl.OutputPath(str),
    report: dsl.OutputPath(str),
    summary: dsl.OutputPath(str),
    pr_curve: dsl.Output[dsl.HTML],
):
    return step(
        "gate",
        {
            "snapshot": snapshot,
            "train": train,
            "validation": validation,
            "test": test,
            "model-uri": model_uri,
            "run-id": run_id,
        },
        [("passed", passed), ("report", report), ("summary", summary), ("pr_curve", pr_curve.path)],
    )


@dsl.component(base_image="python:3.12-slim")
def publish_metrics(summary: str, metrics: dsl.Output[dsl.Metrics]):
    import json

    data = json.loads(summary)
    for prefix, name in (("test", "model"), ("baseline", "baseline"), ("production", "production")):
        for metric, value in data["metrics"].get(name, {}).items():
            metrics.log_metric(f"{prefix}_{metric}", value)
    metrics.log_metric("gate_passed", 1 if data["passed"] else 0)
    metrics.metadata["training_set_snapshot"] = str(data["training_set_snapshot"])
    metrics.metadata["feature_service"] = data["feature_service"]
    metrics.metadata["sources"] = json.dumps(data["sources"], sort_keys=True)


@dsl.container_component
def record_version(
    run_id: str,
    snapshot: str,
    model_uri: str,
    report: str,
    fit: str,
    params: str,
    experiment: str,
    version: dsl.OutputPath(str),
):
    return step(
        "record_version",
        {
            "snapshot": snapshot,
            "model-uri": model_uri,
            "report": report,
            "fit": fit,
            "params": params,
            "experiment": experiment,
            "run-id": run_id,
        },
        [("version", version)],
    )


@dsl.container_component
def roll_out(run_id: str, version: str, deployed: dsl.OutputPath(str)):
    return step("roll_out", {"version": version, "run-id": run_id}, [("version", deployed)])


def configure(task, *, nodes, memory="1G", cpu="250m", storage=True, retries=0):
    task.set_caching_options(False)
    if retries:
        task.set_retry(num_retries=retries, backoff_duration="30s")
    task.set_cpu_request(cpu)
    task.set_memory_request(memory)
    task.set_memory_limit(memory)
    kubernetes.use_config_map_as_env(task, CONFIG, {key: key for key in CONFIG_KEYS})
    if storage:
        kubernetes.use_secret_as_env(
            task,
            "ml-training-storage",
            {
                "AWS_ACCESS_KEY_ID": "AWS_ACCESS_KEY_ID",
                "AWS_SECRET_ACCESS_KEY": "AWS_SECRET_ACCESS_KEY",
            },
        )
        kubernetes.use_secret_as_env(
            task, "ml-training-iceberg", {"ICEBERG_CLIENT_SECRET": "ICEBERG_CLIENT_SECRET"}
        )
    for key, value in nodes.items():
        kubernetes.add_node_selector(task, key, value)
    return task


def spark_task(task, *, nodes, cpu, heap, memory):
    configure(task, nodes=nodes, cpu=cpu, memory=memory, retries=2)
    task.set_env_variable("SPARK_MASTER", f"local[{cpu}]")
    task.set_env_variable("PYSPARK_SUBMIT_ARGS", f"--driver-memory {heap} pyspark-shell")
    task.set_env_variable("SPARK_LOCAL_DIR", "/tmp/spark")
    kubernetes.empty_dir_mount(task, "spill", "/tmp/spark", size_limit="24Gi")
    return task
