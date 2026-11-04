"""Tests for the Kubernetes calls, with the cluster API replaced by fakes."""

from types import SimpleNamespace

import pytest
from kubernetes.client.rest import ApiException

from risk_model import kube
from risk_model.kube import SOURCE_URI, rolled_out

URI = "model-registry://payment-risk/v2"
READY = {
    "status": {
        "conditions": [{"type": "Ready", "status": "True"}],
        "address": {"url": "http://svc"},
    }
}


class FakeObjects:
    """A stand-in for the custom-objects API that records calls and serves stored resources."""

    def __init__(self, status=None):
        self.created, self.patched, self.deleted = [], [], []
        self.status = status
        self.stored = {}

    def create_namespaced_custom_object(self, group, version, namespace, plural, body):
        """Records a creation and stores the body."""
        self.created.append((plural, namespace, body))
        self.stored[(plural, body["metadata"]["name"])] = body

    def get_namespaced_custom_object(self, group, version, namespace, plural, name):
        """Returns the stored resource with the fake's status, or raises 404."""
        if (plural, name) not in self.stored:
            raise ApiException(status=404)
        resource = self.stored[(plural, name)]
        return {**resource, "status": self.status} if self.status is not None else resource

    def patch_namespaced_custom_object(self, group, version, namespace, plural, name, body):
        """Records a patch."""
        self.patched.append((plural, name, body))

    def delete_namespaced_custom_object(self, group, version, namespace, plural, name):
        """Records a deletion, and raises 404 for a resource that is not stored."""
        self.deleted.append((plural, name))
        if (plural, name) not in self.stored:
            raise ApiException(status=404)


@pytest.fixture
def cluster(monkeypatch):
    """Replaces the API and the sleep, and returns the fake custom-objects API."""
    fake = FakeObjects()
    monkeypatch.setattr(kube, "objects", lambda: fake)
    monkeypatch.setattr(
        kube,
        "json_patch",
        lambda kind, name, operations: fake.patched.append((kind[2], name, operations)),
    )
    monkeypatch.setattr(kube.time, "sleep", lambda _: None)
    return fake


def deployment(uri=URI, generation=3, observed=3, replicas=1, updated=1, ready=1, total=1):
    """Returns a Deployment-shaped object with the given rollout state."""
    return SimpleNamespace(
        metadata=SimpleNamespace(generation=generation),
        spec=SimpleNamespace(
            replicas=replicas,
            template=SimpleNamespace(metadata=SimpleNamespace(annotations={SOURCE_URI: uri})),
        ),
        status=SimpleNamespace(
            observed_generation=observed,
            replicas=total,
            updated_replicas=updated,
            ready_replicas=ready,
        ),
    )


def test_a_finished_rollout_to_the_requested_model_is_done():
    """A Deployment with the new URI and every replica updated and ready is rolled out."""
    assert rolled_out(deployment(), URI)


@pytest.mark.parametrize(
    "stale",
    [
        deployment(uri="model-registry://payment-risk/v1"),
        deployment(observed=2),
        deployment(updated=0, total=2),
        deployment(ready=0),
        deployment(replicas=0, updated=0, ready=0, total=0),
    ],
)
def test_anything_short_of_a_finished_rollout_is_not_done(stale):
    """An old URI, an unobserved generation, a pending or unready replica or no replicas is not done."""
    assert not rolled_out(stale, URI)


def test_the_cluster_configuration_is_loaded_once_and_shared(monkeypatch):
    """Every API client is built on one client, and the configuration is read a single time."""
    loads = []
    monkeypatch.setenv("KUBERNETES_SERVICE_HOST", "10.0.0.1")
    monkeypatch.setattr(kube.config, "load_incluster_config", lambda: loads.append("in-cluster"))
    monkeypatch.setattr(kube.config, "load_kube_config", lambda: loads.append("kubeconfig"))
    monkeypatch.setattr(kube.client, "ApiClient", lambda: object())
    kube.connect.cache_clear()
    try:
        first, second = kube.objects(), kube.apps()
        kube.objects()
        assert loads == ["in-cluster"]
        assert first.api_client is second.api_client is kube.connect()
        monkeypatch.delenv("KUBERNETES_SERVICE_HOST")
        kube.connect.cache_clear()
        kube.connect()
        assert loads == ["in-cluster", "kubeconfig"]
    finally:
        kube.connect.cache_clear()


def test_the_worker_environment_has_settings_and_secret_references(monkeypatch):
    """Settings present in the environment are passed by value, and credentials by secret reference."""
    for name in kube.PASSTHROUGH:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    monkeypatch.setenv("ICEBERG_REST_URI", "http://catalog")
    env = kube.worker_env()
    plain = {item["name"]: item["value"] for item in env if "value" in item}
    secret = {
        item["name"]: item["valueFrom"]["secretKeyRef"] for item in env if "valueFrom" in item
    }
    assert plain == {"AWS_REGION": "us-east-1", "ICEBERG_REST_URI": "http://catalog"}
    assert secret == {
        "AWS_ACCESS_KEY_ID": {"name": "ml-training-storage", "key": "AWS_ACCESS_KEY_ID"},
        "AWS_SECRET_ACCESS_KEY": {"name": "ml-training-storage", "key": "AWS_SECRET_ACCESS_KEY"},
        "ICEBERG_CLIENT_SECRET": {"name": "ml-training-iceberg", "key": "ICEBERG_CLIENT_SECRET"},
    }


def test_resources_are_created_read_and_deleted_in_the_namespace(cluster, monkeypatch):
    """create returns the name, get returns the resource, and delete ignores a missing one."""
    monkeypatch.setenv("ML_NAMESPACE", "team-ml")
    body = {"metadata": {"name": "job-1"}}
    assert kube.create(kube.TRAINJOB, body) == "job-1"
    assert cluster.created == [("trainjobs", "team-ml", body)]
    assert kube.get(kube.TRAINJOB, "job-1") == body
    kube.delete(kube.TRAINJOB, "job-1")
    kube.delete(kube.TRAINJOB, "job-1")
    assert cluster.deleted == [("trainjobs", "job-1")] * 2


def test_a_failed_delete_other_than_not_found_is_raised(monkeypatch):
    """Only a 404 is ignored when deleting."""

    class Broken:
        """A custom-objects API whose delete is forbidden."""

        def delete_namespaced_custom_object(self, *_):
            """Raises 403."""
            raise ApiException(status=403)

    monkeypatch.setattr(kube, "objects", lambda: Broken())
    with pytest.raises(ApiException):
        kube.delete(kube.TRAINJOB, "job-1")


def test_a_condition_is_found_only_when_its_status_is_true():
    """condition returns the matching true condition, and None for a false, other or absent one."""
    resource = {
        "status": {
            "conditions": [
                {"type": "Ready", "status": "False"},
                {"type": "Complete", "status": "True", "message": "done"},
            ]
        }
    }
    assert kube.condition(resource, "Complete")["message"] == "done"
    assert kube.condition(resource, "Ready") is None
    assert kube.condition(resource, "Failed") is None
    assert kube.condition({}, "Complete") is None


def test_wait_polls_until_done_and_returns_the_outcome(cluster):
    """wait returns the first truthy outcome with the resource, polling until then."""
    cluster.stored[("trainjobs", "j")] = {"metadata": {"name": "j"}}
    answers = iter([None, None, "finished"])
    outcome, resource = kube.wait(kube.TRAINJOB, "j", lambda _: next(answers), timeout=60)
    assert outcome == "finished" and resource["metadata"]["name"] == "j"


def test_wait_deletes_on_timeout_unless_told_not_to(cluster):
    """A timeout raises, and deletes the resource only when cleanup is on."""
    cluster.stored[("trainjobs", "j")] = {"metadata": {"name": "j"}}
    with pytest.raises(TimeoutError, match="trainjobs/j"):
        kube.wait(kube.TRAINJOB, "j", lambda _: None, timeout=0, cleanup=False)
    assert cluster.deleted == []
    with pytest.raises(TimeoutError):
        kube.wait(kube.TRAINJOB, "j", lambda _: None, timeout=0)
    assert cluster.deleted == [("trainjobs", "j")]


def test_the_train_job_puts_secrets_in_trainer_env_and_only_the_service_account_in_the_patch():
    """The webhook forbids env in a runtime patch, so credentials go in `spec.trainer.env`."""
    job = kube.train_job_spec("ml:1", ["python", "-m", "x"], ["fit", "--seed", "7"], 5400)
    spec = job["spec"]
    assert job["kind"] == "TrainJob" and spec["activeDeadlineSeconds"] == 5400
    assert spec["runtimeRef"] == {
        "name": "payment-risk-xgboost",
        "apiGroup": "trainer.kubeflow.org",
        "kind": "TrainingRuntime",
    }
    trainer = spec["trainer"]
    assert (trainer["numNodes"], trainer["image"], trainer["command"], trainer["args"]) == (
        1,
        "ml:1",
        ["python", "-m", "x"],
        ["fit", "--seed", "7"],
    )
    assert {i["name"] for i in trainer["env"] if "valueFrom" in i} == set(kube.SECRET_ENV)
    (patch,) = spec["runtimePatches"]
    assert "/" in patch["manager"]
    (node,) = patch["trainingRuntimeSpec"]["template"]["spec"]["replicatedJobs"]
    pod = node["template"]["spec"]["template"]["spec"]
    assert node["name"] == "node" and pod == {"serviceAccountName": "fraud-training"}


@pytest.mark.parametrize(
    ("conditions", "expected"),
    [
        ([{"type": "Complete", "status": "True"}], "complete"),
        ([{"type": "Failed", "status": "True", "message": "oom"}], "failed: oom"),
        (
            [{"type": "Failed", "status": "True", "reason": "DeadlineExceeded"}],
            "failed: DeadlineExceeded",
        ),
        ([{"type": "Complete", "status": "False"}], None),
        ([], None),
    ],
)
def test_a_train_job_is_finished_when_complete_or_failed(conditions, expected):
    """finished reports completion, a failure with its message or reason, and None while running."""
    assert kube.finished({"status": {"conditions": conditions}}) == expected


def test_a_completed_train_job_is_created_waited_for_and_kept(cluster):
    """train_job creates the job with its metadata, waits for completion and does not delete it."""
    cluster.status = {"conditions": [{"type": "Complete", "status": "True"}]}
    kube.train_job("fit-1", {"run": "r"}, "ml:1", ["python"], ["fit"], 60)
    ((plural, namespace, body),) = cluster.created
    assert (plural, namespace) == ("trainjobs", "ml")
    assert body["metadata"] == {"name": "fit-1", "namespace": "ml", "labels": {"run": "r"}}
    assert cluster.deleted == []


def test_a_failed_train_job_is_deleted_and_reported(cluster):
    """A failed job is deleted and raises with the failure message."""
    cluster.status = {
        "conditions": [{"type": "Failed", "status": "True", "message": "out of memory"}]
    }
    with pytest.raises(RuntimeError, match="fit-1 failed: out of memory"):
        kube.train_job("fit-1", {}, "ml:1", ["python"], ["fit"], 60)
    assert cluster.deleted == [("trainjobs", "fit-1")]


def test_a_new_inference_service_is_created_in_standard_mode_with_the_chart_settings(cluster):
    """The first rollout creates the service with the chart's predictor settings and the model URI."""
    kube.inference_service("v2", {"minReplicas": 1, "nodeSelector": {"pool": "spot"}})
    ((plural, _, body),) = cluster.created
    assert plural == "inferenceservices"
    assert body["metadata"]["annotations"] == {"serving.kserve.io/deploymentMode": "Standard"}
    predictor = body["spec"]["predictor"]
    assert predictor["minReplicas"] == 1 and predictor["nodeSelector"] == {"pool": "spot"}
    assert predictor["serviceAccountName"] == "fraud-training"
    assert predictor["model"] == {
        "modelFormat": {"name": "payment-risk"},
        "runtime": "payment-risk-triton",
        "storageUri": URI,
    }
    assert cluster.patched == []


def test_an_existing_inference_service_has_its_whole_predictor_replaced(cluster):
    """Later rollouts replace the entire predictor section, so changed replicas and resources apply."""
    cluster.stored[("inferenceservices", "payment-risk")] = {"metadata": {"name": "payment-risk"}}
    settings = {"minReplicas": 2, "resources": {"limits": {"memory": "2Gi"}}}
    kube.inference_service("v2", settings)
    ((plural, name, body),) = cluster.patched
    assert (plural, name) == ("inferenceservices", "payment-risk")
    (operation,) = body
    assert operation["op"] == "replace" and operation["path"] == "/spec/predictor"
    assert operation["value"]["minReplicas"] == 2
    assert operation["value"]["resources"] == {"limits": {"memory": "2Gi"}}
    assert operation["value"]["model"]["storageUri"] == URI
    assert cluster.created == []


def test_a_predictor_with_autoscaling_selects_the_keda_autoscaler_class(cluster, monkeypatch):
    """KServe only creates the KEDA ScaledObject from the annotation, on a new and on an existing service."""
    monkeypatch.setattr(
        kube,
        "autoscaling",
        lambda: SimpleNamespace(delete_namespaced_horizontal_pod_autoscaler=lambda *args: None),
    )
    settings = {"minReplicas": 1, "maxReplicas": 2, "autoScaling": {"metrics": []}}
    kube.inference_service("v2", settings)
    ((_, _, body),) = cluster.created
    assert body["metadata"]["annotations"] == {
        "serving.kserve.io/deploymentMode": "Standard",
        "serving.kserve.io/autoscalerClass": "keda",
    }
    cluster.stored[("inferenceservices", "payment-risk")] = {"metadata": {"name": "payment-risk"}}
    kube.inference_service("v3", settings)
    ((_, _, (replace, annotate)),) = cluster.patched
    assert replace["op"] == "replace" and replace["value"]["autoScaling"] == {"metrics": []}
    assert annotate == {
        "op": "add",
        "path": "/metadata/annotations/serving.kserve.io~1autoscalerClass",
        "value": "keda",
    }


def test_a_challenger_service_has_its_own_name_and_serves_the_same_registered_model(cluster):
    """The service name changes, and the model format and the registry URI stay the registered model's."""
    kube.inference_service("v3", {"minReplicas": 1}, kube.CHALLENGER_NAME)
    ((_, _, body),) = cluster.created
    assert body["metadata"]["name"] == "payment-risk-challenger"
    model = body["spec"]["predictor"]["model"]
    assert model["modelFormat"] == {"name": "payment-risk"} and model["storageUri"].endswith("/v3")


def test_an_error_other_than_not_found_reading_the_inference_service_is_raised(monkeypatch):
    """Only a missing service is created; any other error propagates."""

    class Broken:
        """A custom-objects API that is forbidden."""

        def get_namespaced_custom_object(self, *_):
            """Raises 403."""
            raise ApiException(status=403)

    monkeypatch.setattr(kube, "objects", lambda: Broken())
    with pytest.raises(ApiException):
        kube.inference_service("v2", {})


def test_the_registry_uri_names_the_model_and_version():
    """model_uri points KServe's storage initializer at a registry version."""
    assert kube.model_uri("v9") == "model-registry://payment-risk/v9"


def test_wait_ready_returns_the_address_once_the_deployment_serves_the_new_model(
    cluster, monkeypatch
):
    """The service is ready only when the Deployment has also rolled out the new storage URI."""
    cluster.stored[("inferenceservices", "payment-risk")] = {"metadata": {"name": "payment-risk"}}
    cluster.status = READY["status"]
    states = iter([deployment(uri="model-registry://payment-risk/v1"), deployment()])
    asked = []

    class Apps:
        """A Deployments API that returns an old rollout and then the new one."""

        def read_namespaced_deployment(self, name, namespace):
            """Returns the next rollout state and records the request."""
            asked.append((name, namespace))
            return next(states)

    monkeypatch.setattr(kube, "apps", lambda: Apps())
    assert kube.wait_ready("payment-risk", "v2") == "http://svc"
    assert asked == [("payment-risk-predictor", "ml")] * 2


def test_wait_ready_ignores_a_service_without_an_address(cluster, monkeypatch):
    """A Ready condition without an address is not enough, and the wait times out."""
    cluster.stored[("inferenceservices", "payment-risk")] = {"metadata": {"name": "payment-risk"}}
    cluster.status = {"conditions": [{"type": "Ready", "status": "True"}]}
    monkeypatch.setattr(kube, "apps", lambda: pytest.fail("no Deployment should be read"))

    def stop(_):
        raise TimeoutError("stopped after one poll")

    monkeypatch.setattr(kube.time, "sleep", stop)
    with pytest.raises(TimeoutError, match="stopped after one poll"):
        kube.wait_ready("payment-risk", "v2")
    assert cluster.deleted == []


def test_a_json_patch_goes_through_a_client_that_sends_the_json_patch_content_type(monkeypatch):
    """The patch uses its own client with the JSON-patch default header, on the shared configuration."""
    seen = {}

    class Api:
        """An API client that records its configuration and default headers."""

        def __init__(self, configuration):
            seen["configuration"], self.headers = configuration, {}

        def set_default_header(self, name, value):
            """Records a default header."""
            self.headers[name] = value

    class Objects:
        """A custom-objects API that records its client and the patch."""

        def __init__(self, api):
            seen["api"] = api

        def patch_namespaced_custom_object(self, *arguments):
            """Records the arguments."""
            seen["arguments"] = arguments

    monkeypatch.setattr(kube, "connect", lambda: SimpleNamespace(configuration="shared"))
    monkeypatch.setattr(kube.client, "ApiClient", Api)
    monkeypatch.setattr(kube.client, "CustomObjectsApi", Objects)
    kube.json_patch(kube.INFERENCE, "payment-risk", [{"op": "remove", "path": "/x"}])
    assert seen["configuration"] == "shared"
    assert seen["api"].headers == {"Content-Type": "application/json-patch+json"}
    assert seen["arguments"] == (
        "serving.kserve.io",
        "v1beta1",
        "ml",
        "inferenceservices",
        "payment-risk",
        [{"op": "remove", "path": "/x"}],
    )


def test_pod_labels_go_into_the_runtime_patch_only_when_given():
    """A Katib trial labels its pods through the patch; other jobs have no pod metadata at all."""

    def pod(job):
        patch = job["spec"]["runtimePatches"][0]["trainingRuntimeSpec"]["template"]
        return patch["spec"]["replicatedJobs"][0]["template"]["spec"]["template"]

    plain = kube.train_job_spec("ml:1", ["x"], [], 60)
    labelled = kube.train_job_spec("ml:1", ["x"], [], 60, pod_labels={"a": "b"})
    assert "metadata" not in pod(plain) and pod(labelled)["metadata"] == {"labels": {"a": "b"}}
    assert pod(labelled)["spec"] == pod(plain)["spec"] == {"serviceAccountName": "fraud-training"}


def test_the_route_weights_always_add_up_to_one_hundred(cluster, monkeypatch):
    """The champion keeps what the challenger does not get, on the route named by the environment."""
    monkeypatch.setenv("EXPERIMENT_ROUTE", "payment-risk-ab")
    kube.set_weights(20)
    kube.set_weights(0, route="other")
    (plural, name, operations), (_, other, zero) = cluster.patched
    assert (plural, name, other) == ("httproutes", "payment-risk-ab", "other")
    assert [(o["path"], o["value"]) for o in operations] == [
        ("/spec/rules/0/backendRefs/0/weight", 80),
        ("/spec/rules/0/backendRefs/1/weight", 20),
    ]
    assert [o["value"] for o in zero] == [100, 0]


def test_the_champions_service_is_labelled_with_the_waypoint(monkeypatch):
    """The predictor Service of the named InferenceService gets the use-waypoint label."""
    sent = []
    monkeypatch.setenv("SERVING_WAYPOINT", "ml-waypoint")
    monkeypatch.setattr(
        kube,
        "core",
        lambda: SimpleNamespace(patch_namespaced_service=lambda *args: sent.append(args)),
    )
    kube.use_waypoint()
    kube.use_waypoint("payment-risk-challenger")
    assert sent == [
        (
            "payment-risk-predictor",
            "ml",
            {"metadata": {"labels": {"istio.io/use-waypoint": "ml-waypoint"}}},
        ),
        (
            "payment-risk-challenger-predictor",
            "ml",
            {"metadata": {"labels": {"istio.io/use-waypoint": "ml-waypoint"}}},
        ),
    ]


TEMPLATE = {
    "spec": {
        "args": [{"name": "window", "value": "10m"}, {"name": "challengerVersion"}],
        "metrics": [{"name": "error-rate"}],
    }
}


@pytest.mark.parametrize("phase", ["Successful", "Failed"])
def test_an_analysis_run_copies_the_template_fills_its_arguments_and_is_deleted(cluster, phase):
    """The run carries the template's metrics and arguments, returns its end phase and is deleted."""
    cluster.stored[("analysistemplates", "payment-risk-challenger")] = TEMPLATE
    cluster.status = {"phase": phase, "message": "done"}
    args = {"challengerVersion": "v4", "window": "5m"}
    assert kube.analysis_run("run-1", "payment-risk-challenger", args, 60) == (phase, "done")
    ((plural, _, body),) = cluster.created
    assert plural == "analysisruns" and body["spec"]["metrics"] == [{"name": "error-rate"}]
    assert body["spec"]["args"] == [
        {"name": "window", "value": "5m"},
        {"name": "challengerVersion", "value": "v4"},
    ]
    assert cluster.deleted == [("analysisruns", "run-1")]


def test_an_analysis_argument_without_a_value_is_refused_before_anything_is_created(cluster):
    """A template argument with no default and no value stops the run, and the defaults apply otherwise."""
    cluster.stored[("analysistemplates", "payment-risk-challenger")] = TEMPLATE
    with pytest.raises(ValueError, match="challengerVersion"):
        kube.analysis_run("run-3", "payment-risk-challenger", {}, 60)
    assert cluster.created == []


def test_an_analysis_that_never_ends_times_out_and_is_still_deleted(cluster):
    """A run still running at the timeout raises and is cleaned up."""
    cluster.stored[("analysistemplates", "payment-risk-challenger")] = TEMPLATE
    cluster.status = {"phase": "Running"}
    with pytest.raises(TimeoutError):
        kube.analysis_run("run-2", "payment-risk-challenger", {"challengerVersion": "v4"}, 0)
    assert ("analysisruns", "run-2") in cluster.deleted


def hpa_calls(cluster, monkeypatch, annotations, settings, error=None):
    """Updates an existing service and returns the names of the autoscalers that were deleted."""
    dropped = []

    def delete(name, namespace):
        dropped.append(name)
        if error:
            raise error

    monkeypatch.setattr(
        kube,
        "autoscaling",
        lambda: SimpleNamespace(delete_namespaced_horizontal_pod_autoscaler=delete),
    )
    cluster.stored[("inferenceservices", "payment-risk")] = {
        "metadata": {"annotations": annotations}
    }
    kube.inference_service("v3", settings)
    return dropped


KEDA = {"autoScaling": {"metrics": []}}


def test_a_service_that_gains_keda_loses_the_autoscaler_kserve_made_before(cluster, monkeypatch):
    """The old HorizontalPodAutoscaler would make KEDA's webhook refuse the ScaledObject, so it goes first."""
    assert hpa_calls(cluster, monkeypatch, {}, KEDA) == ["payment-risk-predictor"]


def test_a_service_that_already_uses_keda_or_never_will_keeps_its_autoscaler(cluster, monkeypatch):
    """Nothing is deleted once the class is set, or while the predictor has no autoscaling section."""
    keda = {"serving.kserve.io/autoscalerClass": "keda"}
    assert hpa_calls(cluster, monkeypatch, keda, KEDA) == []
    assert hpa_calls(cluster, monkeypatch, {}, {"minReplicas": 1}) == []


def test_an_autoscaler_that_is_already_gone_is_not_an_error_but_another_failure_is(
    cluster, monkeypatch
):
    """A 404 is ignored, and any other API error is raised."""
    assert hpa_calls(cluster, monkeypatch, {}, KEDA, ApiException(status=404)) == [
        "payment-risk-predictor"
    ]
    with pytest.raises(ApiException):
        hpa_calls(cluster, monkeypatch, {}, KEDA, ApiException(status=500))
