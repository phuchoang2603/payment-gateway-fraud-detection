from pathlib import Path

import yaml

from simulator.config import HistoricalConfig

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
