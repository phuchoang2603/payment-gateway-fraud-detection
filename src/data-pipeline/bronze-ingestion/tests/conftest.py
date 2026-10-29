import json
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[4]


@pytest.fixture(autouse=True)
def shared_connectors(tmp_path, monkeypatch):
    values = yaml.safe_load((ROOT / "infra/charts/metadata-integrations/values.yaml").read_text())
    path = tmp_path / "connectors.json"
    path.write_text(json.dumps(values["connectors"]))
    monkeypatch.setenv("METADATA_CONNECTORS_FILE", str(path))
