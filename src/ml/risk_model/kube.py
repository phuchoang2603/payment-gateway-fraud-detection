"""Kubernetes calls for the training steps: TrainJobs, Katib experiments and the InferenceService."""

import functools
import json
import os
import time
import urllib.request

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
RUNTIME = "xgboost-gpu-distributed"
MANAGER = "fraud.payment-gateway/risk-model"
TRAINJOB = ("trainer.kubeflow.org", "v1alpha1", "trainjobs")
EXPERIMENT = ("kubeflow.org", "v1beta1", "experiments")
INFERENCE = ("serving.kserve.io", "v1beta1", "inferenceservices")
INFERENCE_NAME = MODEL_NAME
SOURCE_URI = "internal.serving.kserve.io/storage-initializer-sourceuri"
RUNTIME_NAME = "payment-risk-predictor"


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


def worker_env():
    """Returns the environment of a training pod: plain settings and the secret-backed credentials."""
    env = [{"name": name, "value": os.environ[name]} for name in PASSTHROUGH if name in os.environ]
    env += [
        {"name": name, "valueFrom": {"secretKeyRef": {"name": secret, "key": name}}}
        for name, secret in SECRET_ENV.items()
    ]
    return env


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


def train_job_spec(image, command, args, timeout):
    """Returns a one-node TrainJob on the GPU runtime, shared by the fit job and Katib trials.

    Secret-backed variables go in `spec.trainer.env`, because the Trainer webhook rejects environment
    set through a runtime patch on the node container. The patch only runs the pod as the service account.
    """
    node = {"template": {"spec": {"template": {"spec": {"serviceAccountName": SERVICE_ACCOUNT}}}}}
    return {
        "apiVersion": "trainer.kubeflow.org/v1alpha1",
        "kind": "TrainJob",
        "spec": {
            "runtimeRef": {
                "name": RUNTIME,
                "apiGroup": "trainer.kubeflow.org",
                "kind": "ClusterTrainingRuntime",
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


def inference_service(version, predictor):
    """Creates the InferenceService for a model version, or replaces its whole predictor section.

    `predictor` holds the chart's serving settings, such as replicas, resources and node selectors.
    """
    name = INFERENCE_NAME
    spec = {
        "serviceAccountName": SERVICE_ACCOUNT,
        **predictor,
        "model": {
            "modelFormat": {"name": name},
            "runtime": RUNTIME_NAME,
            "storageUri": model_uri(version),
        },
    }
    try:
        get(INFERENCE, name)
    except ApiException as error:
        if error.status != 404:
            raise
        create(
            INFERENCE,
            {
                "apiVersion": "serving.kserve.io/v1beta1",
                "kind": "InferenceService",
                "metadata": {
                    "name": name,
                    "namespace": namespace(),
                    "annotations": {"serving.kserve.io/deploymentMode": "Standard"},
                },
                "spec": {"predictor": spec},
            },
        )
        return
    group, api, plural = INFERENCE
    objects().patch_namespaced_custom_object(
        group,
        api,
        namespace(),
        plural,
        name,
        [{"op": "replace", "path": "/spec/predictor", "value": spec}],
    )


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


def predict(address, rows, timeout=30):
    """Sends instances to the predictor at `address` and returns its decoded JSON answer."""
    request = urllib.request.Request(
        f"{address.rstrip('/')}/v1/models/{INFERENCE_NAME}:predict",
        data=json.dumps({"instances": rows}).encode(),
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return json.load(response)
