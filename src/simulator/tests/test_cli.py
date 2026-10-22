import json

import pytest
import yaml

from simulator import storage
from simulator.cli import main
from tests.conftest import small_config


@pytest.mark.parametrize("gate", ["native", "dp1-spark-gx"])
def test_normal_command_resumes_cleans_and_retries_publication(tmp_path, monkeypatch, gate):
    config = small_config(
        duration_days=2,
        customer_block_size=20,
        validation_gate=gate,
        **{"counts.customers": 20, "data_problems.schema_cutover_date": "2026-07-02"},
    )
    path = tmp_path / "config.yaml"
    path.write_text(yaml.safe_dump(config.model_dump(mode="json")))
    output = tmp_path / "output"
    command = ["historical", "--config", str(path), "--output", str(output)]
    root = output / "historical" / config.dataset_version
    assert main([*command, "--prepare-only"]) == 0
    assert not (root / "manifest.json").exists()
    purge = storage.ObjectStore.purge

    def failed_cleanup(*args, **kwargs):
        raise OSError("injected cleanup failure")

    monkeypatch.setattr(storage.ObjectStore, "purge", failed_cleanup)
    with pytest.raises(SystemExit) as failure:
        main(command)
    assert failure.value.code == 2
    original = (root / "manifest.json").read_bytes()
    manifest = json.loads(original)
    if gate == "native":
        assert manifest["validation"] == {"payment-events": "passed", "fraud-labels": "passed"}
    else:
        assert manifest["validation"]["gate"] == gate
        assert manifest["validation"]["result"] == "skipped"

    def unexpected(*args):
        raise AssertionError("completed output must not regenerate")

    monkeypatch.setattr(storage, "build_profiles", unexpected)
    monkeypatch.setattr(storage.ObjectStore, "purge", purge)
    assert main(command) == 0
    assert main(command) == 0
    assert (root / "manifest.json").read_bytes() == original
    publication = json.loads((root / "_run/publication.json").read_text())
    assert publication["cleanup"] == "removed"
    assert {path.name for path in (root / "_run").iterdir()} == {"locks", "publication.json"}
