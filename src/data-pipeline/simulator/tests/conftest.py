from pathlib import Path

import pyarrow.fs as fs
import pytest
import yaml

from simulator.config import HistoricalConfig
from simulator.storage import generate

SMALL_CONFIG = Path(__file__).with_name("small-config.yaml")


def small_config(**overrides) -> HistoricalConfig:
    data = yaml.safe_load(SMALL_CONFIG.read_text())
    for path, value in overrides.items():
        target = data
        *parents, leaf = path.split(".")
        for parent in parents:
            target = target.setdefault(parent, {})
        target[leaf] = value
    return HistoricalConfig.model_validate(data)


@pytest.fixture(scope="session")
def dataset(tmp_path_factory) -> tuple[Path, dict]:
    root = tmp_path_factory.mktemp("output")
    manifest = generate(small_config(), fs.LocalFileSystem(), str(root))
    return root / "historical" / "smoke", manifest
