import os

import pytest


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    os.environ["SPARK_JARS_PACKAGES"] = ",".join(
        [
            "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.12.0",
            "org.apache.spark:spark-protobuf_2.13:4.1.3",
        ]
    )
    os.environ["BATCH_LOCAL_WAREHOUSE"] = str(tmp_path_factory.mktemp("warehouse"))
    from batch.spark import session

    session_ = session("batch-tests")
    yield session_
    session_.stop()
