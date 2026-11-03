"""Tests for the registry client, with the Model Registry replaced by an in-memory fake."""

from types import SimpleNamespace

import pytest
from model_registry.types import ModelVersionState

from risk_model.registry import RegistryClient, default_url, flatten


class Version:
    """A registered model version with a name, custom properties and a state."""

    def __init__(self, name, properties=None, state=ModelVersionState.LIVE):
        self.name, self.custom_properties, self.state = name, properties, state


class FakeRegistry:
    """A Model Registry client that serves the given versions and records registrations and updates."""

    def __init__(self, versions=None, model=True):
        self.versions, self.model = versions or [], model
        self.registered, self.updated = [], []

    def get_registered_model(self, name):
        """Returns a model object, or None when the model has never been registered."""
        return SimpleNamespace(name=name) if self.model else None

    def get_model_versions(self, name):
        """Returns the versions."""
        return iter(self.versions)

    def get_model_version(self, name, version):
        """Returns the version with that name, or None."""
        return next((v for v in self.versions if v.name == version), None)

    def get_model_artifact(self, name, version):
        """Returns an artifact whose URI names the version."""
        return SimpleNamespace(uri=f"s3://bucket/{version}")

    def register_model(self, name, uri, **options):
        """Records a registration."""
        self.registered.append((name, uri, options))

    def update(self, version):
        """Records an update."""
        self.updated.append(version)


def registry(*versions, model=True):
    """Returns a RegistryClient on a fake holding the versions."""
    return RegistryClient(FakeRegistry(list(versions), model=model))


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (4507818529279769299, "4507818529279769299"),
        (999_999_999, 999_999_999),
        (-1_000_000_000, "-1000000000"),
        (True, True),
        (0.5, 0.5),
        ("v1", "v1"),
        ({"a": 1}, '{"a": 1}'),
        ([1, 2], "[1, 2]"),
    ],
)
def test_flatten_fits_the_registry_property_types(value, expected):
    """Scalars pass through, big integers become text and containers become JSON."""
    assert flatten(value) == expected


def test_the_default_registry_address_follows_the_namespace(monkeypatch):
    """The in-cluster registry service is looked up in the configured namespace."""
    monkeypatch.setenv("ML_NAMESPACE", "team")
    assert default_url() == "http://model-registry-service.team.svc.cluster.local:8080"


def test_a_model_never_registered_has_no_versions():
    """Before the first registration the version list is empty and the next version is v1."""
    empty = registry(model=False)
    assert empty.versions() == [] and empty.numbered() == []
    assert empty.next_version() == "v1" and empty.latest_cutoff() is None


def test_numbered_versions_sort_by_number_and_skip_other_names():
    """v10 comes after v9, and names that are not `vN` are ignored."""
    names = ["v10", "v2", "v1-beta", "latest", "v9", "v"]
    found = registry(*[Version(name) for name in names]).numbered()
    assert [v.name for v in found] == ["v2", "v9", "v10"]


def test_the_next_version_follows_the_highest_number():
    """The next version is one more than the highest, not the count of versions."""
    assert registry(Version("v3"), Version("v10"), Version("v2")).next_version() == "v11"


def test_the_latest_cutoff_is_read_from_the_highest_version():
    """The cutoff comes from the newest numbered version, and None when it recorded none."""
    old = Version("v2", {"cutoff": "2026-10-01T00:00:00Z"})
    new = Version("v10", {"cutoff": "2026-10-21T00:00:00Z"})
    assert registry(new, old).latest_cutoff() == "2026-10-21T00:00:00Z"
    assert registry(Version("v1", None)).latest_cutoff() is None


def test_register_flattens_properties_drops_nones_and_marks_a_candidate():
    """A registration carries scalar properties and the `candidate` stage, with the service account."""
    client = registry()
    version = client.register(
        "v1",
        "s3://bucket/model",
        {"snapshot": 4507818529279769299, "gate": "passed", "skip": None, "f": [1]},
    )
    assert version == "v1"
    ((name, uri, options),) = client.client.registered
    assert (name, uri) == ("payment-risk", "s3://bucket/model")
    assert options["version"] == "v1" and options["service_account_name"] == "fraud-training"
    assert options["metadata"] == {
        "snapshot": "4507818529279769299",
        "gate": "passed",
        "f": "[1]",
        "stage": "candidate",
    }
    assert options["model_format_name"] == "xgboost"


def test_a_missing_version_is_an_error_and_a_found_one_exposes_its_properties():
    """get raises ValueError for an unknown version, and properties returns a copy of the found one's."""
    client = registry(Version("v1", {"gate": "passed"}), Version("v2", None))
    with pytest.raises(ValueError, match="no version v9"):
        client.get("v9")
    assert client.properties("v1") == {"gate": "passed"}
    assert client.properties("v2") == {}
    assert client.artifact_uri("v1") == "s3://bucket/v1"


def test_only_a_live_version_in_the_production_stage_is_production():
    """An archived or candidate version is not production even with the stage property."""
    versions = [
        Version("v1", {"stage": "production"}, ModelVersionState.ARCHIVED),
        Version("v2", {"stage": "candidate"}),
        Version("v3", None),
        Version("v4", {"stage": "production"}, ModelVersionState.LIVE),
    ]
    assert registry(*versions).production() == "v4"
    assert registry(*versions[:3]).production() is None


def test_a_version_that_did_not_pass_the_gate_cannot_be_served():
    """require_passing returns the properties of a passing version and raises for any other."""
    client = registry(
        Version("v1", {"gate": "passed"}), Version("v2", {"gate": "failed"}), Version("v3")
    )
    assert client.require_passing("v1") == {"gate": "passed"}
    for version in ("v2", "v3"):
        with pytest.raises(ValueError, match="did not pass"):
            client.require_passing(version)


def test_promotion_makes_one_version_live_and_archives_the_one_it_replaces():
    """The promoted version goes live with the run recorded, the old production is archived, others stay."""
    old = Version("v1", {"stage": "production"}, ModelVersionState.LIVE)
    new = Version("v2", {"stage": "candidate"}, ModelVersionState.ARCHIVED)
    other = Version("v3", {"stage": "candidate"})
    client = registry(old, new, other)
    client.promote("v2", "run-9")
    assert new.state == ModelVersionState.LIVE
    assert new.custom_properties == {"stage": "production", "deployed_by_run": "run-9"}
    assert old.state == ModelVersionState.ARCHIVED and old.custom_properties == {
        "stage": "archived"
    }
    assert client.client.updated == [old, new] or client.client.updated == [new, old]
    assert other not in client.client.updated and other.custom_properties == {"stage": "candidate"}


def test_the_challenger_is_the_live_version_in_the_challenger_stage():
    """An archived or other-stage version is not the challenger."""
    versions = [
        Version("v1", {"stage": "challenger"}, ModelVersionState.ARCHIVED),
        Version("v2", {"stage": "production"}),
        Version("v3", {"stage": "challenger"}),
    ]
    assert registry(*versions).challenger() == "v3"
    assert registry(*versions[:2]).challenger() is None


def test_marking_a_challenger_archives_the_one_it_replaces_and_leaves_production_alone():
    """The new challenger goes live with the run recorded, the old one is archived, production stays."""
    old = Version("v2", {"stage": "challenger"})
    new = Version("v3", {"stage": "candidate"}, ModelVersionState.ARCHIVED)
    champion = Version("v1", {"stage": "production"})
    client = registry(champion, old, new)
    client.mark_challenger("v3", "run-4")
    assert new.state == ModelVersionState.LIVE
    assert new.custom_properties == {"stage": "challenger", "deployed_by_run": "run-4"}
    assert old.state == ModelVersionState.ARCHIVED and old.custom_properties == {
        "stage": "archived"
    }
    assert champion not in client.client.updated and champion.custom_properties == {
        "stage": "production"
    }


def test_retiring_a_version_archives_it_and_keeps_its_other_properties():
    """An aborted challenger is archived with its facts intact."""
    version = Version("v3", {"stage": "challenger", "gate": "passed"})
    registry(version).retire("v3")
    assert version.state == ModelVersionState.ARCHIVED
    assert version.custom_properties == {"stage": "archived", "gate": "passed"}
