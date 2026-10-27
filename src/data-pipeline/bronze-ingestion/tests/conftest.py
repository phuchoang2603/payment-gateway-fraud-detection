import json
import os
import sys
from pathlib import Path

import pyarrow.fs as fs
import pytest
import yaml

ROOT = Path(__file__).resolve().parents[4]
GENERATOR = Path(os.environ.get("DP1_GENERATOR_ROOT", ROOT / "src/data-pipeline/simulator"))
sys.path.insert(0, str(GENERATOR))


@pytest.fixture(scope="session")
def dataset(tmp_path_factory):
    from simulator.config import HistoricalConfig
    from simulator.storage import generate

    config = yaml.safe_load((GENERATOR / "tests/small-config.yaml").read_text())
    config["duration_days"] = 2
    config["counts"]["customers"] = 8
    root = tmp_path_factory.mktemp("source")
    manifest = generate(
        HistoricalConfig.model_validate(config), fs.LocalFileSystem(), str(root), allow_small_dataset=True
    )
    output = root / "historical/smoke"
    assert isinstance(manifest.get("inventory"), list), "Integrate the scale-historical-data-generation dependency"
    return output, manifest


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    from data_platform.spark import session

    instance = session(tmp_path_factory.mktemp("warehouse"))
    yield instance
    instance.stop()


@pytest.fixture
def bounded_shuffle(spark):
    original = spark.conf.get("spark.sql.shuffle.partitions")
    spark.conf.set("spark.sql.shuffle.partitions", "4")
    yield
    spark.conf.set("spark.sql.shuffle.partitions", original)


@pytest.fixture(autouse=True)
def shared_connectors(tmp_path, monkeypatch):
    values = yaml.safe_load((ROOT / "infra/charts/metadata-integrations/values.yaml").read_text())
    path = tmp_path / "connectors.json"
    path.write_text(json.dumps(values["connectors"]))
    monkeypatch.setenv("METADATA_CONNECTORS_FILE", str(path))
