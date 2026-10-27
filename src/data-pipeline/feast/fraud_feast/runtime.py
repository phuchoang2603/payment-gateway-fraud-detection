import json
import os
import signal
import sys
import threading

import uvicorn
from confluent_kafka import Consumer
from feast import FeatureStore
from feast.feature_server import get_app
from feast.repo_config import RepoConfig

from fraud_feast.schema import definitions
from fraud_feast.writer import Writer, consume


def store():
    password = os.environ["FEAST_REDIS_PASSWORD"]
    if not password:
        raise ValueError("FEAST_REDIS_PASSWORD must be set")
    encoded = json.dumps(password).replace(",", r"\u002c")
    config = RepoConfig(
        project="fraud_snapshots",
        provider="local",
        registry={"path": "/tmp/feast-registry.db"},
        online_store={
            "type": "redis",
            "connection_string": f"{os.environ['FEAST_REDIS_ADDR']},password={encoded},"
            "socket_timeout=2,socket_connect_timeout=2",
            "skip_dedup": False,
        },
        entity_key_serialization_version=3,
    )
    result = FeatureStore(config=config)
    result.apply(definitions(), partial=True)
    return result


def main():
    feature_store = store()
    if sys.argv[1:] == ["serve"]:
        uvicorn.run(get_app(feature_store), host="0.0.0.0", port=6566, access_log=False)
    elif sys.argv[1:] == ["write"]:
        stopped = threading.Event()
        for sig in (signal.SIGTERM, signal.SIGINT):
            signal.signal(sig, lambda *_: stopped.set())
        consumer = Consumer(
            {
                "bootstrap.servers": os.environ["FEATURE_KAFKA_BROKERS"],
                "group.id": os.environ["FEATURE_WRITER_GROUP"],
                "enable.auto.commit": False,
                "enable.auto.offset.store": False,
                "auto.offset.reset": "earliest",
                "isolation.level": "read_committed",
                "max.poll.interval.ms": 600_000,
                "allow.auto.create.topics": False,
            }
        )
        consumer.subscribe([os.environ["FEATURE_UPDATE_TOPIC"]])
        consume(
            consumer,
            Writer(
                feature_store,
                epoch=os.environ["FEATURE_EPOCH"],
                definition_hash=os.environ["FEATURE_DEFINITION_SHA256"],
            ),
            stopped,
        )
    else:
        raise ValueError("expected serve or write")


if __name__ == "__main__":
    main()
