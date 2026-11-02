import json
import os

from model_registry import ModelRegistry
from model_registry.types import ModelVersionState

NAME = "payment-risk"
DEFAULT_URL = "http://model-registry-service.ml.svc.cluster.local:8080"


def flatten(value):
    if isinstance(value, (bool, str, float)):
        return value
    if isinstance(value, int):
        return str(value) if abs(value) >= 10**9 else value
    return json.dumps(value)


class RegistryClient:
    def __init__(self, client=None, name=NAME):
        self.name = name
        self.client = client or ModelRegistry(
            os.environ.get("MODEL_REGISTRY_URL", DEFAULT_URL),
            author="fraud-training",
            is_secure=False,
        )

    def versions(self):
        if self.client.get_registered_model(self.name) is None:
            return []
        return list(self.client.get_model_versions(self.name))

    def next_version(self):
        numbers = [int(v.name[1:]) for v in self.versions() if v.name[1:].isdigit()]
        return f"v{max(numbers, default=0) + 1}"

    def register(self, version, uri, properties):
        flat = {key: flatten(value) for key, value in properties.items() if value is not None}
        self.client.register_model(
            self.name,
            uri,
            version=version,
            model_format_name="xgboost",
            model_format_version="2",
            service_account_name="fraud-training",
            metadata={**flat, "stage": "candidate"},
        )
        return version

    def get(self, version):
        found = self.client.get_model_version(self.name, version)
        if found is None:
            raise ValueError(f"{self.name} has no version {version}")
        return found

    def properties(self, version):
        return dict(self.get(version).custom_properties or {})

    def artifact_uri(self, version):
        return self.client.get_model_artifact(self.name, version).uri

    def production(self):
        for version in self.versions():
            properties = version.custom_properties or {}
            if properties.get("stage") == "production" and version.state == ModelVersionState.LIVE:
                return version.name
        return None

    def require_passing(self, version):
        properties = self.properties(version)
        if properties.get("gate") != "passed":
            raise ValueError(f"{self.name} {version} did not pass the evaluation gate")
        return properties

    def promote(self, version, run_id):
        for other in self.versions():
            properties = dict(other.custom_properties or {})
            if other.name == version:
                other.state = ModelVersionState.LIVE
                properties.update(stage="production", deployed_by_run=run_id)
            elif properties.get("stage") == "production":
                other.state = ModelVersionState.ARCHIVED
                properties["stage"] = "archived"
            else:
                continue
            other.custom_properties = properties
            self.client.update(other)
