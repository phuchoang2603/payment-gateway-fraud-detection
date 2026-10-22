import os
import sys
from pathlib import Path

import pyarrow.fs as fs
import pytest
import yaml

ROOT = Path(__file__).resolve().parents[3]
GENERATOR = Path(os.environ.get("DP1_GENERATOR_ROOT", ROOT / "src/simulator"))
sys.path.insert(0, str(GENERATOR))


@pytest.fixture(scope="session")
def dataset(tmp_path_factory):
    from simulator.config import HistoricalConfig
    from simulator.storage import generate

    config = yaml.safe_load((ROOT / "src/simulator/tests/small-config.yaml").read_text())
    config["duration_days"] = 2
    config["counts"]["customers"] = 8
    config["data_problems"]["schema_cutover_date"] = "2026-07-02"
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
