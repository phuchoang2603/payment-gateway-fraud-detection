import json
import os
from pathlib import Path
from string import Template

import uvicorn
import yaml
from feast import FeatureStore
from feast.repo_config import RepoConfig

from fraud_feast.server import get_app


def redis_connection():
    password = os.environ["FEAST_REDIS_PASSWORD"]
    if not password:
        raise ValueError("FEAST_REDIS_PASSWORD must be set")
    return (
        f"{os.environ['FEAST_REDIS_ADDR']},"
        f"password={json.dumps(password).replace(',', r'\u002c')},"
        "socket_timeout=2,socket_connect_timeout=2"
    )


def config(*, online=False, redis=True):
    values = yaml.safe_load(Path(__file__).with_name("feature_store.yaml").read_text())
    if not redis:
        values["online_store"] = None
    connection = redis_connection() if redis else ""

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


def store(*, online=False, redis=True):
    return FeatureStore(config=config(online=online, redis=redis))


def main():
    uvicorn.run(get_app(store(online=True)), host="0.0.0.0", port=6566, access_log=False)


if __name__ == "__main__":
    main()
