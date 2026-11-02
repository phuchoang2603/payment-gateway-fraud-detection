import base64
import os
import time

from kubeflow.common.types import KubernetesBackendConfig
from kubeflow.trainer import CustomTrainerContainer, TrainerClient
from kubeflow.trainer.options import (
    ActiveDeadlineSeconds,
    JobSetSpecPatch,
    JobSetTemplatePatch,
    JobSpecPatch,
    JobTemplatePatch,
    Labels,
    Name,
    PodSpecPatch,
    PodTemplatePatch,
    ReplicatedJobPatch,
    RuntimePatch,
    TrainerArgs,
    TrainerCommand,
    TrainingRuntimeSpecPatch,
)
from kubernetes import client, config
from kubernetes.client.rest import ApiException

STORAGE_SECRET = "ml-training-storage"
ICEBERG_SECRET = "ml-training-iceberg"
SERVICE_ACCOUNT = "fraud-training"
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
EXPERIMENT = ("kubeflow.org", "v1beta1", "experiments")
INFERENCE = ("serving.kserve.io", "v1beta1", "inferenceservices")
INFERENCE_NAME = "payment-risk"
RUNTIME_NAME = "payment-risk-predictor"


def namespace():
    return os.environ.get("ML_NAMESPACE", "ml")


def objects():
    if os.environ.get("KUBERNETES_SERVICE_HOST"):
        config.load_incluster_config()
    else:
        config.load_kube_config()
    return client.CustomObjectsApi()


def worker_env():
    env = [{"name": name, "value": os.environ[name]} for name in PASSTHROUGH if name in os.environ]
    env += [
        {"name": name, "valueFrom": {"secretKeyRef": {"name": secret, "key": name}}}
        for name, secret in SECRET_ENV.items()
    ]
    return env


def create(kind, body):
    group, version, plural = kind
    objects().create_namespaced_custom_object(group, version, namespace(), plural, body)
    return body["metadata"]["name"]


def get(kind, name):
    group, version, plural = kind
    return objects().get_namespaced_custom_object(group, version, namespace(), plural, name)


def delete(kind, name):
    group, version, plural = kind
    try:
        objects().delete_namespaced_custom_object(group, version, namespace(), plural, name)
    except ApiException as error:
        if error.status != 404:
            raise


def condition(resource, kind):
    for item in resource.get("status", {}).get("conditions", []):
        if item.get("type") == kind and item.get("status") == "True":
            return item
    return None


def wait(kind, name, done, timeout, interval=20, cleanup=True):
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


def runtime_patch():
    return RuntimePatch(
        training_runtime_spec=TrainingRuntimeSpecPatch(
            template=JobSetTemplatePatch(
                spec=JobSetSpecPatch(
                    replicated_jobs=[
                        ReplicatedJobPatch(
                            name="node",
                            template=JobTemplatePatch(
                                spec=JobSpecPatch(
                                    template=PodTemplatePatch(
                                        spec=PodSpecPatch(
                                            service_account_name=SERVICE_ACCOUNT,
                                        )
                                    )
                                )
                            ),
                        )
                    ]
                )
            )
        )
    )


def load_secrets():
    missing = {name: secret for name, secret in SECRET_ENV.items() if name not in os.environ}
    if not missing:
        return
    objects()
    api = client.CoreV1Api()
    for name, secret in missing.items():
        data = api.read_namespaced_secret(secret, namespace()).data
        os.environ[name] = base64.b64decode(data[name]).decode()


def train_job(name, labels, image, command, args, timeout):
    os.environ.setdefault("KUBEFLOW_SYSTEM_NAMESPACE", namespace())
    trainer = TrainerClient(KubernetesBackendConfig(namespace=namespace()))
    trainer.train(
        runtime=RUNTIME,
        trainer=CustomTrainerContainer(
            image=image,
            num_nodes=1,
            env={i["name"]: i["value"] for i in worker_env() if "value" in i},
        ),
        options=[
            Name(name),
            Labels(labels),
            TrainerCommand(command),
            TrainerArgs(args),
            ActiveDeadlineSeconds(timeout),
            runtime_patch(),
        ],
    )
    try:
        return trainer.wait_for_job_status(name, timeout=timeout, polling_interval=20)
    except (TimeoutError, RuntimeError):
        trainer.delete_job(name)
        raise


def trial_train_job(image, command, args, timeout):
    secrets = [i for i in worker_env() if "valueFrom" in i]
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
                "env": [i for i in worker_env() if "value" in i],
            },
            "runtimePatches": [
                {
                    "manager": "risk-model",
                    "trainingRuntimeSpec": {
                        "template": {
                            "spec": {
                                "replicatedJobs": [
                                    {
                                        "name": "node",
                                        "template": {
                                            "spec": {
                                                "template": {
                                                    "spec": {
                                                        "serviceAccountName": SERVICE_ACCOUNT,
                                                        "containers": [
                                                            {"name": "node", "env": secrets}
                                                        ],
                                                    }
                                                }
                                            }
                                        },
                                    }
                                ]
                            }
                        }
                    },
                }
            ],
            "activeDeadlineSeconds": timeout,
        },
    }


def inference_service(version, predictor):
    name = INFERENCE_NAME
    predictor = dict(predictor)
    uri = f"model-registry://{name}/{version}"
    model = {
        "modelFormat": {"name": name},
        "runtime": RUNTIME_NAME,
        "storageUri": uri,
    }
    body = {
        "apiVersion": "serving.kserve.io/v1beta1",
        "kind": "InferenceService",
        "metadata": {
            "name": name,
            "namespace": namespace(),
            "labels": {"networking.knative.dev/visibility": "cluster-local"},
            "annotations": {"serving.kserve.io/deploymentMode": "Serverless"},
        },
        "spec": {"predictor": {"serviceAccountName": SERVICE_ACCOUNT, **predictor, "model": model}},
    }
    try:
        existing = get(INFERENCE, name)
    except ApiException as error:
        if error.status != 404:
            raise
        create(INFERENCE, body)
        return None
    group, api, plural = INFERENCE
    current = existing["spec"]["predictor"]["model"].get("storageUri")
    revision = existing.get("status", {}).get("components", {}).get("predictor", {})
    objects().patch_namespaced_custom_object(
        group, api, namespace(), plural, name, {"spec": {"predictor": {"model": model}}}
    )
    return revision.get("latestCreatedRevision") if current != uri else "same"


def ready_host(name, previous, timeout=1200):
    def done(resource):
        status = resource.get("status", {})
        predictor = status.get("components", {}).get("predictor", {})
        created, ready = (
            predictor.get("latestCreatedRevision"),
            predictor.get("latestReadyRevision"),
        )
        if (
            condition(resource, "Ready")
            and ready
            and ready == created
            and (previous in (None, "same") or ready != previous)
        ):
            return predictor.get("url") or status.get("address", {}).get("url", "")
        return None

    url, _ = wait(INFERENCE, name, done, timeout, interval=10, cleanup=False)
    return url.split("://", 1)[-1].split("/", 1)[0]
