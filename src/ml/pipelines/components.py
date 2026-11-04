"""The Kubeflow pipeline components: one container step per `risk_model.steps` function."""

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
    "SERVING_WAYPOINT",
    "EXPERIMENT_ROUTE",
    "EXPERIMENT_MAX_PERCENT",
    "EXPERIMENT_STEPS",
    "EXPERIMENT_WINDOW",
)


OFF_GPU = [{"key": "nvidia.com/gpu.present", "operator": "DoesNotExist"}]


def step(name, values, outputs=()):
    """Returns the container spec that runs one step.

    `values` is the component's `locals()`: every entry except the named `outputs` becomes a
    `--dashed-name value` argument, and each output becomes `--out name path`.
    """
    args = [name]
    for key, value in values.items():
        if key not in outputs:
            args += [f"--{key.replace('_', '-')}", value]
    for key in outputs:
        args += ["--out", key, values[key]]
    return dsl.ContainerSpec(IMAGE, ["python", "-m", "risk_model.steps"], args)


@dsl.container_component
def plan_windows(
    run_id: str,
    create_time: str,
    train_days: int,
    validation_days: int,
    test_days: int,
    lag_days: int,
    force: bool,
    fixed_train: str,
    fixed_validation: str,
    fixed_test: str,
    fixed_cutoff: str,
    train: dsl.OutputPath(str),
    validation: dsl.OutputPath(str),
    test: dsl.OutputPath(str),
    cutoff: dsl.OutputPath(str),
    run: dsl.OutputPath(str),
):
    """Chooses the run's windows and whether there is new data to train on."""
    return step("plan_windows", locals(), ("train", "validation", "test", "cutoff", "run"))


@dsl.container_component
def plan_examples(
    run_id: str,
    train: str,
    validation: str,
    test: str,
    existing_snapshot: str,
    chunk_days: int,
    chunks: dsl.OutputPath(list),
):
    """Fences the sources and lists the day chunks to stage."""
    return step("plan_examples", locals(), ("chunks",))


@dsl.container_component
def stage_examples(chunk: str, cutoff: str, rows: dsl.OutputPath(str)):
    """Stages one chunk of days with its features."""
    return step("stage_examples", locals(), ("rows",))


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
    """Merges the staged chunks into a new training-set version."""
    return step("assemble_examples", locals(), ("snapshot",))


@dsl.container_component
def check_examples(run_id: str, snapshot: str, report: dsl.OutputPath(str)):
    """Validates the training-set version."""
    return step("check_examples", locals(), ("report",))


@dsl.container_component
def split_windows(
    run_id: str,
    snapshot: str,
    train: str,
    validation: str,
    test: str,
    report: dsl.OutputPath(str),
):
    """Counts the rows and fraud rate of each window."""
    return step("split_windows", locals(), ("report",))


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
    """Searches hyperparameters with a Katib experiment."""
    return step("tune_params", locals(), ("params", "experiment"))


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
    """Trains the model in a one-GPU TrainJob."""
    return step("fit_on_gpu", locals(), ("model_uri", "fit"))


@dsl.container_component
def gate(
    run_id: str,
    snapshot: str,
    train: str,
    validation: str,
    test: str,
    model_uri: str,
    margin: float,
    passed: dsl.OutputPath(str),
    report: dsl.OutputPath(str),
    summary: dsl.OutputPath(str),
    pr_curve: dsl.Output[dsl.HTML],
):
    """Evaluates the model against the baseline and production."""
    values = {**locals(), "pr_curve": pr_curve.path}
    return step("gate", values, ("passed", "report", "summary", "pr_curve"))


@dsl.component(base_image="python:3.12-slim")
def publish_metrics(summary: str, metrics: dsl.Output[dsl.Metrics]):
    """Logs the gate's metrics for the model, the baseline and production as KFP metrics."""
    import json

    data = json.loads(summary)
    for prefix, name in (("test", "model"), ("baseline", "baseline"), ("production", "production")):
        for metric, value in data["metrics"].get(name, {}).items():
            metrics.log_metric(f"{prefix}_{metric}", value)
    metrics.log_metric("gate_passed", 1 if data["passed"] else 0)


@dsl.container_component
def record_version(
    run_id: str,
    snapshot: str,
    model_uri: str,
    report: str,
    fit: str,
    params: str,
    experiment: str,
    trigger: str,
    version: dsl.OutputPath(str),
):
    """Registers the passing model in the Model Registry."""
    return step("record_version", locals(), ("version",))


@dsl.container_component
def roll_out(run_id: str, version: str, role: str, deployed: dsl.OutputPath(str)):
    """Serves the registered version as the champion or the challenger, and checks it."""
    return step("roll_out", locals(), ("deployed",))


@dsl.container_component
def promote_challenger(run_id: str, steps_percent: str, promoted: dsl.OutputPath(str)):
    """Shifts traffic to the challenger in analysed steps, then promotes it or removes it."""
    return step("promote_challenger", locals(), ("promoted",))


def configure(task, *, nodes, memory="1G", cpu="250m", storage=True, retries=0):
    """Sets a step's caching, retries, resources, environment, credentials and placement.

    No pipeline step scores or trains: those run in GPU jobs. So every step is kept off GPU nodes, where
    its CPU request would starve the GPU job it launches.
    """
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
    kubernetes.add_node_affinity(task, match_expressions=OFF_GPU)
    return task


def spark_task(task, *, nodes, cpu, heap, memory):
    """Configures a step that runs local Spark on `cpu` cores with a driver heap and a spill volume."""
    configure(task, nodes=nodes, cpu=cpu, memory=memory, retries=2)
    task.set_env_variable("SPARK_MASTER", f"local[{cpu}]")
    task.set_env_variable("PYSPARK_SUBMIT_ARGS", f"--driver-memory {heap} pyspark-shell")
    task.set_env_variable("SPARK_LOCAL_DIR", "/tmp/spark")
    kubernetes.empty_dir_mount(task, "spill", "/tmp/spark", size_limit="24Gi")
    return task
