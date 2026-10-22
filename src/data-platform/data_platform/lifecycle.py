import json
import os
import ssl
import urllib.error
import urllib.parse
import urllib.request

from data_platform.storage import Store

TERMINAL = {"COMPLETED", "FAILED", "SUBMISSION_FAILED"}


class KubernetesAPI:
    def __init__(self):
        host = os.environ["KUBERNETES_SERVICE_HOST"]
        port = os.environ.get("KUBERNETES_SERVICE_PORT_HTTPS", "443")
        self.root = f"https://{host}:{port}"
        directory = "/var/run/secrets/kubernetes.io/serviceaccount/"
        with open(directory + "token") as stream:
            self.token = stream.read()
        self.context = ssl.create_default_context(cafile=directory + "ca.crt")

    def __call__(self, path, method="GET"):
        req = urllib.request.Request(
            self.root + path, method=method, headers={"Authorization": "Bearer " + self.token}
        )
        try:
            with urllib.request.urlopen(req, context=self.context, timeout=15) as response:
                return json.load(response)
        except urllib.error.HTTPError as error:
            if error.code == 404:
                return None
            raise


def applications_path():
    namespace = os.environ.get("POD_NAMESPACE", "fraud-data-prod")
    return f"/apis/sparkoperator.k8s.io/v1beta2/namespaces/{namespace}/sparkapplications"


def retain_and_delete(api, path, app, store):
    state = app.get("status", {}).get("applicationState", {}).get("state")
    if store:
        workflow = app["metadata"]["labels"]["dp1-workflow"]
        store.put(
            f"diagnostics/{workflow}/{app['metadata']['name']}.json",
            {
                "application": app["metadata"]["name"],
                "workflow": workflow,
                "state": state,
                "driver": app.get("status", {}).get("driverInfo", {}),
                "executors": app.get("status", {}).get("executorState", {}),
                "cleanup_requested": state not in TERMINAL,
            },
        )
    if state not in TERMINAL:
        api(path + "/" + urllib.parse.quote(app["metadata"]["name"], safe=""), "DELETE")


def cleanup(workflow, api=None, store=None):
    api = api or KubernetesAPI()
    path = applications_path()
    selector = urllib.parse.urlencode({"labelSelector": "dp1-workflow=" + workflow})
    if store is None and os.environ.get("EVIDENCE_ROOT"):
        store = Store(os.environ["EVIDENCE_ROOT"])
    apps = api(path + "?" + selector)["items"]
    for app in apps:
        retain_and_delete(api, path, app, store)


def reap(api=None, store=None):
    api = api or KubernetesAPI()
    path = applications_path()
    namespace = os.environ.get("POD_NAMESPACE", "fraud-data-prod")
    store = store or Store(os.environ["EVIDENCE_ROOT"])
    apps = api(path + "?labelSelector=dp1-workflow&limit=100")["items"]
    for app in apps:
        if app.get("status", {}).get("applicationState", {}).get("state") in TERMINAL:
            continue
        name = app["metadata"]["labels"]["dp1-workflow"]
        workflow = api(
            f"/apis/argoproj.io/v1alpha1/namespaces/{namespace}/workflows/" + urllib.parse.quote(name, safe="")
        )
        if (
            workflow is None
            or workflow.get("spec", {}).get("shutdown") in ("Stop", "Terminate")
            or workflow.get("status", {}).get("phase") in ("Succeeded", "Failed", "Error")
        ):
            retain_and_delete(api, path, app, store)
