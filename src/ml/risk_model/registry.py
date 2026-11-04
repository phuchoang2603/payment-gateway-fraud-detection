"""The Kubeflow Model Registry client: numbered versions, the production stage and promotion."""

import json
import os
import re

from model_registry import ModelRegistry
from model_registry.types import ModelVersionState

from risk_model.cluster import MODEL_NAME, SERVICE_ACCOUNT, namespace


def default_url():
    """Returns the in-cluster address of the registry service."""
    return f"http://model-registry-service.{namespace()}.svc.cluster.local:8080"


def flatten(value):
    """Returns a value the registry accepts as a custom property: a scalar or a JSON string.

    Integers of ten digits or more become strings, because the registry rejects them as integers.
    """
    if isinstance(value, (bool, str, float)):
        return value
    if isinstance(value, int):
        return str(value) if abs(value) >= 10**9 else value
    return json.dumps(value)


class RegistryClient:
    """Reads and writes the versions of one registered model.

    `client` is a Model Registry client, built from `MODEL_REGISTRY_URL` when omitted, and `name` is
    the registered model. A version records where its files are and how it was trained, and carries
    a `stage` property: `candidate` when registered, `production` while it is live and `archived`
    after a later version replaces it.
    """

    def __init__(self, client=None, name=MODEL_NAME):
        self.name = name
        self.client = client or ModelRegistry(
            os.environ.get("MODEL_REGISTRY_URL", default_url()),
            author=SERVICE_ACCOUNT,
            is_secure=False,
        )

    def versions(self):
        """Returns every version of the model, or an empty list before the first registration."""
        if self.client.get_registered_model(self.name) is None:
            return []
        return list(self.client.get_model_versions(self.name))

    def numbered(self):
        """Returns the versions named `vN`, oldest first."""
        found = [v for v in self.versions() if re.fullmatch(r"v\d+", v.name)]
        return sorted(found, key=lambda v: int(v.name[1:]))

    def next_version(self):
        """Returns the name of the version after the newest numbered one, `v1` for a new model."""
        numbered = self.numbered()
        return f"v{int(numbered[-1].name[1:]) + 1}" if numbered else "v1"

    def latest_cutoff(self):
        """Returns the cutoff recorded on the newest numbered version, or None if there is none."""
        numbered = self.numbered()
        return (numbered[-1].custom_properties or {}).get("cutoff") if numbered else None

    def register(self, version, uri, properties):
        """Registers a version at `uri` with its properties as a candidate and returns its name."""
        flat = {key: flatten(value) for key, value in properties.items() if value is not None}
        self.client.register_model(
            self.name,
            uri,
            version=version,
            model_format_name="xgboost",
            model_format_version="2",
            service_account_name=SERVICE_ACCOUNT,
            metadata={**flat, "stage": "candidate"},
        )
        return version

    def get(self, version):
        """Returns a version by name, or raises ValueError if the model has none."""
        found = self.client.get_model_version(self.name, version)
        if found is None:
            raise ValueError(f"{self.name} has no version {version}")
        return found

    def properties(self, version):
        """Returns the custom properties of a version."""
        return dict(self.get(version).custom_properties or {})

    def artifact_uri(self, version):
        """Returns where a version's model files are stored."""
        return self.client.get_model_artifact(self.name, version).uri

    def production(self):
        """Returns the name of the live production version, or None."""
        for version in self.versions():
            properties = version.custom_properties or {}
            if properties.get("stage") == "production" and version.state == ModelVersionState.LIVE:
                return version.name
        return None

    def require_passing(self, version):
        """Returns a version's properties, or raises ValueError unless it passed the gate."""
        properties = self.properties(version)
        if properties.get("gate") != "passed":
            raise ValueError(f"{self.name} {version} did not pass the evaluation gate")
        return properties

    def challenger(self):
        """Returns the name of the live challenger version, or None."""
        for version in self.versions():
            properties = version.custom_properties or {}
            if properties.get("stage") == "challenger" and version.state == ModelVersionState.LIVE:
                return version.name
        return None

    def mark_challenger(self, version, run_id):
        """Makes a version the live challenger and archives the challenger it replaces."""
        for other in self.versions():
            properties = dict(other.custom_properties or {})
            if other.name == version:
                other.state = ModelVersionState.LIVE
                properties.update(stage="challenger", deployed_by_run=run_id)
            elif properties.get("stage") == "challenger":
                other.state = ModelVersionState.ARCHIVED
                properties["stage"] = "archived"
            else:
                continue
            other.custom_properties = properties
            self.client.update(other)

    def retire(self, version):
        """Archives a version that is no longer served, such as an aborted challenger."""
        found = self.get(version)
        found.state = ModelVersionState.ARCHIVED
        found.custom_properties = {**(found.custom_properties or {}), "stage": "archived"}
        self.client.update(found)

    def promote(self, version, run_id):
        """Makes a version the live production one and archives the version it replaces."""
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
