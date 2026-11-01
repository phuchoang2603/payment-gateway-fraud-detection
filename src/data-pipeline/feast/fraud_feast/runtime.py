import json
import os
from pathlib import Path
from string import Template

import uvicorn
import yaml
from feast import FeatureStore
from feast.repo_config import RepoConfig

from fraud_feast.server import get_app


def config(*, online=False):
    password = os.environ["FEAST_REDIS_PASSWORD"]
    if not password:
        raise ValueError("FEAST_REDIS_PASSWORD must be set")
    connection = (
        f"{os.environ['FEAST_REDIS_ADDR']},"
        f"password={json.dumps(password).replace(',', r'\u002c')},"
        "socket_timeout=2,socket_connect_timeout=2"
    )
    text = Path(__file__).with_name("feature_store.yaml").read_text()
    values = yaml.safe_load(text)

    def expand(value):
        if isinstance(value, dict):
            return {k: expand(v) for k, v in value.items()}
        if isinstance(value, str):
            return Template(value).substitute({**os.environ, "FEAST_REDIS_CONNECTION": connection})
        return value

    if online:
        values["offline_store"] = {"type": "file"}
    values = expand(values)
    if not online:
        values["offline_store"]["spark_conf"]["spark.master"] = os.environ.get(
            "SPARK_MASTER", "local[*]"
        )
    return RepoConfig(**values)


def store(*, online=False):
    return FeatureStore(config=config(online=online))


def main():
    uvicorn.run(get_app(store(online=True)), host="0.0.0.0", port=6566, access_log=False)


if __name__ == "__main__":
    main()
