"""Kubernetes calls for the pipeline steps: GPU TrainJobs, Katib experiments and the InferenceService."""

import functools
import os
import time

from kubernetes import client, config
from kubernetes.client.rest import ApiException

from risk_model.cluster import MODEL_NAME, SERVICE_ACCOUNT, namespace

STORAGE_SECRET = "ml-training-storage"
ICEBERG_SECRET = "ml-training-iceberg"
PASSTHROUGH = (
    "AWS_REGION",
    "S3_ENDPOINT",
    "S3_PATH_STYLE",
    "ICEBERG_REST_URI",
    "ICEBERG_WAREHOUSE",
    "ICEBERG_CLIENT_ID",
    "ICEBERG_OAUTH2_URI",
    "MODEL_REGISTRY_URL",
)
SECRET_ENV = {
    "AWS_ACCESS_KEY_ID": STORAGE_SECRET,
    "AWS_SECRET_ACCESS_KEY": STORAGE_SECRET,
    "ICEBERG_CLIENT_SECRET": ICEBERG_SECRET,
}
RUNTIME = "payment-risk-xgboost"
MANAGER = "fraud.payment-gateway/risk-model"
TRAINJOB = ("trainer.kubeflow.org", "v1alpha1", "trainjobs")
EXPERIMENT = ("kubeflow.org", "v1beta1", "experiments")
INFERENCE = ("serving.kserve.io", "v1beta1", "inferenceservices")
INFERENCE_NAME = MODEL_NAME
CHALLENGER_NAME = f"{MODEL_NAME}-challenger"
ROUTE = ("gateway.networking.k8s.io", "v1", "httproutes")
ANALYSIS = ("argoproj.io", "v1alpha1", "analysisruns")
ANALYSIS_TEMPLATE = ("argoproj.io", "v1alpha1", "analysistemplates")
USE_WAYPOINT = "istio.io/use-waypoint"
SOURCE_URI = "internal.serving.kserve.io/storage-initializer-sourceuri"
RUNTIME_NAME = "payment-risk-triton"
AUTOSCALER_CLASS = "serving.kserve.io/autoscalerClass"


@functools.cache
def connect():
    """Loads the cluster configuration once and returns the API client every call shares."""
    if os.environ.get("KUBERNETES_SERVICE_HOST"):
        config.load_incluster_config()
    else:
        config.load_kube_config()
    return client.ApiClient()


def objects():
    """Returns the API for custom resources such as TrainJobs, Experiments and InferenceServices."""
    return client.CustomObjectsApi(connect())


def apps():
    """Returns the API for Deployments."""
    return client.AppsV1Api(connect())


def core():
    """Returns the API for Services."""
    return client.CoreV1Api(connect())


def autoscaling():
    """Returns the API for HorizontalPodAutoscalers."""
    return client.AutoscalingV2Api(connect())


def worker_env():
    """Returns the environment of a training pod: plain settings and the secret-backed credentials."""
    env = [{"name": name, "value": os.environ[name]} for name in PASSTHROUGH if name in os.environ]
    env += [
        {"name": name, "valueFrom": {"secretKeyRef": {"name": secret, "key": name}}}
        for name, secret in SECRET_ENV.items()
    ]
    return env


def json_patch(kind, name, operations):
    """Applies RFC 6902 operations to a custom resource.

    The generated client always sends a merge patch for custom resources, which rejects a list. A client
    of its own whose default `Content-Type` is the JSON-patch type is applied after that header, so only
    this call changes it.
    """
    group, version, plural = kind
    patcher = client.ApiClient(connect().configuration)
    patcher.set_default_header("Content-Type", "application/json-patch+json")
    client.CustomObjectsApi(patcher).patch_namespaced_custom_object(
        group, version, namespace(), plural, name, operations
    )


def create(kind, body):
    """Creates a custom resource in the namespace and returns its name."""
    group, version, plural = kind
    objects().create_namespaced_custom_object(group, version, namespace(), plural, body)
    return body["metadata"]["name"]


def get(kind, name):
    """Returns a custom resource of a kind by name."""
    group, version, plural = kind
    return objects().get_namespaced_custom_object(group, version, namespace(), plural, name)


def delete(kind, name):
    """Deletes a custom resource and ignores one that is already gone."""
    group, version, plural = kind
    try:
        objects().delete_namespaced_custom_object(group, version, namespace(), plural, name)
    except ApiException as error:
        if error.status != 404:
            raise


def condition(resource, kind):
    """Returns the status condition of a type that is true on a resource, or None."""
    for item in resource.get("status", {}).get("conditions", []):
        if item.get("type") == kind and item.get("status") == "True":
            return item
    return None


def wait(kind, name, done, timeout, interval=20, cleanup=True):
    """Polls a resource until `done(resource)` is truthy and returns that value with the resource.

    On timeout the resource is deleted when `cleanup` is set, and TimeoutError is raised.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        resource = get(kind, name)
        outcome = done(resource)
        if outcome:
            return outcome, resource
        time.sleep(interval)
    if cleanup:
        delete(kind, name)
    raise TimeoutError(f"{kind[2]}/{name} did not finish within {timeout}s")


def train_job_spec(image, command, args, timeout, pod_labels=None):
    """Returns a one-node TrainJob on the chart's GPU runtime, shared by every job that touches the GPU.

    The runtime holds the shared GPU claim, which a runtime patch cannot set. Secret-backed variables go
    in `spec.trainer.env`, because the Trainer webhook rejects environment set through a runtime patch
    on the node container. The patch runs the pod as the service account and adds `pod_labels`, which a
    Katib trial needs on its pods because Katib only injects its metrics collector into labelled pods.
    """
    pod = {"spec": {"serviceAccountName": SERVICE_ACCOUNT}}
    if pod_labels:
        pod["metadata"] = {"labels": pod_labels}
    node = {"template": {"spec": {"template": pod}}}
    return {
        "apiVersion": "trainer.kubeflow.org/v1alpha1",
        "kind": "TrainJob",
        "spec": {
            "runtimeRef": {
                "name": RUNTIME,
                "apiGroup": "trainer.kubeflow.org",
                "kind": "TrainingRuntime",
            },
            "trainer": {
                "numNodes": 1,
                "image": image,
                "command": command,
                "args": args,
                "env": worker_env(),
            },
            "runtimePatches": [
                {
                    "manager": MANAGER,
                    "trainingRuntimeSpec": {
                        "template": {"spec": {"replicatedJobs": [{"name": "node", **node}]}}
                    },
                }
            ],
            "activeDeadlineSeconds": timeout,
        },
    }


def finished(resource):
    """Returns `complete` or `failed: <reason>` for a finished TrainJob, and None while it runs."""
    if condition(resource, "Complete"):
        return "complete"
    failed = condition(resource, "Failed")
    if failed:
        return f"failed: {failed.get('message', failed.get('reason', ''))}"
    return None


def train_job(name, labels, image, command, args, timeout):
    """Runs a TrainJob to completion and deletes it if it fails or times out.

    Raises RuntimeError with the job's failure message, or TimeoutError.
    """
    body = train_job_spec(image, command, args, timeout)
    body["metadata"] = {"name": name, "namespace": namespace(), "labels": labels}
    create(TRAINJOB, body)
    outcome, _ = wait(TRAINJOB, name, finished, timeout + 120)
    if outcome != "complete":
        delete(TRAINJOB, name)
        raise RuntimeError(f"TrainJob {name} {outcome}")


def model_uri(version):
    """Returns the registry URI KServe resolves to a model version's files."""
    return f"model-registry://{INFERENCE_NAME}/{version}"


def inference_service(version, predictor, name=INFERENCE_NAME):
    """Creates the InferenceService `name` for a model version, or replaces its whole predictor section.

    `predictor` holds the chart's serving settings, such as replicas, resources and node selectors.
    The champion and the challenger serve the same registered model under different service names.
    """
    spec = {
        "serviceAccountName": SERVICE_ACCOUNT,
        **predictor,
        "model": {
            "modelFormat": {"name": INFERENCE_NAME},
            "runtime": RUNTIME_NAME,
            "storageUri": model_uri(version),
        },
    }
    keda = bool(predictor.get("autoScaling"))
    try:
        existing = get(INFERENCE, name)
    except ApiException as error:
        if error.status != 404:
            raise
        annotations = {"serving.kserve.io/deploymentMode": "Standard"}
        if keda:
            annotations[AUTOSCALER_CLASS] = "keda"
        create(
            INFERENCE,
            {
                "apiVersion": "serving.kserve.io/v1beta1",
                "kind": "InferenceService",
                "metadata": {"name": name, "namespace": namespace(), "annotations": annotations},
                "spec": {"predictor": spec},
            },
        )
        return
    operations = [{"op": "replace", "path": "/spec/predictor", "value": spec}]
    if keda and existing["metadata"].get("annotations", {}).get(AUTOSCALER_CLASS) != "keda":
        _drop_hpa(name)
    if keda:
        operations.append(
            {
                "op": "add",
                "path": f"/metadata/annotations/{AUTOSCALER_CLASS.replace('/', '~1')}",
                "value": "keda",
            }
        )
    json_patch(INFERENCE, name, operations)


def _drop_hpa(name):
    """Deletes the HorizontalPodAutoscaler KServe made before the service used KEDA, which would refuse a ScaledObject."""
    try:
        autoscaling().delete_namespaced_horizontal_pod_autoscaler(f"{name}-predictor", namespace())
    except ApiException as error:
        if error.status != 404:
            raise


def rolled_out(deployment, uri):
    """Returns whether a Deployment has finished rolling out the model at `uri`."""
    annotations = deployment.spec.template.metadata.annotations or {}
    status = deployment.status
    replicas = deployment.spec.replicas or 0
    return (
        annotations.get(SOURCE_URI) == uri
        and (status.observed_generation or 0) >= deployment.metadata.generation
        and replicas > 0
        and (status.replicas or 0)
        == (status.updated_replicas or 0)
        == (status.ready_replicas or 0)
        == replicas
    )


def wait_ready(name, version, timeout=1200):
    """Waits until the InferenceService is ready and serving `version`, and returns its address."""
    uri = model_uri(version)

    def done(resource):
        address = resource.get("status", {}).get("address", {}).get("url", "")
        if condition(resource, "Ready") and address:
            deployment = apps().read_namespaced_deployment(f"{name}-predictor", namespace())
            return address if rolled_out(deployment, uri) else None
        return None

    address, _ = wait(INFERENCE, name, done, timeout, interval=10, cleanup=False)
    return address


def use_waypoint(name=INFERENCE_NAME):
    """Labels the predictor Service of an InferenceService so the mesh sends its traffic through the waypoint.

    KServe does not copy labels from the InferenceService to its Service, but keeps ones that are set on it.
    Without a waypoint Gateway the label would break the Service, so the caller sets it only when one exists.
    """
    waypoint = os.environ["SERVING_WAYPOINT"]
    core().patch_namespaced_service(
        f"{name}-predictor", namespace(), {"metadata": {"labels": {USE_WAYPOINT: waypoint}}}
    )


def set_weights(percent, route=None):
    """Sends `percent` of the champion's traffic to the challenger by patching the route's two weights."""
    json_patch(
        ROUTE,
        route or os.environ["EXPERIMENT_ROUTE"],
        [
            {"op": "replace", "path": "/spec/rules/0/backendRefs/0/weight", "value": 100 - percent},
            {"op": "replace", "path": "/spec/rules/0/backendRefs/1/weight", "value": percent},
        ],
    )


def analysis_run(name, template, args, timeout):
    """Runs an analysis from a template to its end and returns its phase and message.

    An AnalysisRun carries its own metrics, so they are copied from the template with each argument
    filled from `args` or the template's default. The run is deleted afterwards.
    """
    source = get(ANALYSIS_TEMPLATE, template)["spec"]
    values = []
    for argument in source.get("args", []):
        value = args.get(argument["name"], argument.get("value"))
        if value is None:
            raise ValueError(f"the analysis {template} needs a value for {argument['name']}")
        values.append({"name": argument["name"], "value": value})
    body = {
        "apiVersion": "argoproj.io/v1alpha1",
        "kind": "AnalysisRun",
        "metadata": {"name": name, "namespace": namespace()},
        "spec": {"metrics": source["metrics"], "args": values},
    }
    create(ANALYSIS, body)

    def done(resource):
        status = resource.get("status", {})
        phase = status.get("phase", "")
        return (
            (phase, status.get("message", ""))
            if phase in ("Successful", "Failed", "Error", "Inconclusive")
            else None
        )

    try:
        result, _ = wait(ANALYSIS, name, done, timeout, interval=15)
    finally:
        delete(ANALYSIS, name)
    return result
