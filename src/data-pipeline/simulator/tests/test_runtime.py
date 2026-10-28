import json

import yaml

from simulator import runtime, storage
from simulator.runtime import main
from tests.conftest import small_config


def test_config_selects_the_dataset_before_generation(tmp_path, monkeypatch):
    config = small_config(output=str(tmp_path / "output")).model_dump(mode="json")
    config["dataset_version"] = "SET_UNUSED_DATASET_VERSION"
    path = tmp_path / "config.yaml"
    path.write_text(yaml.safe_dump(config))
    seen = []

    def generate(selected, *args, **kwargs):
        seen.append(selected.dataset_version)
        return {"dataset_version": selected.dataset_version, "config_sha256": "hash", "history_end": None}

    monkeypatch.setattr(storage, "generate", generate)
    monkeypatch.setattr(runtime, "CONFIG_PATH", path)
    assert main() == 2
    assert not seen
    config["dataset_version"] = "unused"
    path.write_text(yaml.safe_dump(config))
    assert main() == 0
    assert seen == ["unused"]


def test_normal_command_resumes_cleans_and_retries_publication(tmp_path, monkeypatch):
    config = small_config(
        duration_days=2,
        output=str(tmp_path / "output"),
        customer_block_size=20,
        **{"counts.customers": 20},
    )
    path = tmp_path / "config.yaml"
    path.write_text(yaml.safe_dump(config.model_dump(mode="json")))
    output = tmp_path / "output"
    monkeypatch.setenv("SIMULATOR_CONFIG", str(path))
    root = output / "historical" / config.dataset_version
    prepared = config.model_dump(mode="json")
    prepared["execution"]["prepare_only"] = True
    path.write_text(yaml.safe_dump(prepared))
    assert main() == 0
    path.write_text(yaml.safe_dump(config.model_dump(mode="json")))
    assert not (root / "manifest.json").exists()
    purge = storage.ObjectStore.purge

    def failed_cleanup(*args, **kwargs):
        raise OSError("injected cleanup failure")

    monkeypatch.setattr(storage.ObjectStore, "purge", failed_cleanup)
    assert main() == 2
    original = (root / "manifest.json").read_bytes()

    def unexpected(*args):
        raise AssertionError("completed output must not regenerate")

    monkeypatch.setattr(storage, "build_profiles", unexpected)
    monkeypatch.setattr(storage.ObjectStore, "purge", purge)
    assert main() == 0
    assert main() == 0
    assert (root / "manifest.json").read_bytes() == original
    publication = json.loads((root / "_run/publication.json").read_text())
    assert publication["cleanup"] == "removed"
    assert {path.name for path in (root / "_run").iterdir()} == {"locks", "publication.json"}
