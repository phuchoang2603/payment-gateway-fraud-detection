import os
from pathlib import Path

import pytest

CONTRACTS = Path(__file__).resolve().parents[4] / "contracts"


@pytest.fixture(scope="session", autouse=True)
def contract_inputs(tmp_path_factory):
    import grpc_tools
    from grpc_tools import protoc

    descriptor = tmp_path_factory.mktemp("descriptor") / "payments.pb"
    status = protoc.main(
        [
            "protoc",
            f"-I{CONTRACTS}",
            f"-I{Path(grpc_tools.__file__).parent / '_proto'}",
            "--include_imports",
            f"--descriptor_set_out={descriptor}",
            "payments/v1/events.proto",
            "payments/v1/labels.proto",
        ]
    )
    if status != 0:
        raise RuntimeError("protoc could not build the payments descriptor set")
    os.environ["PAYMENTS_DESCRIPTOR"] = str(descriptor)
    os.environ["DATA_PRODUCTS_ROOT"] = str(CONTRACTS / "data-products")


@pytest.fixture(scope="session")
def spark(tmp_path_factory, contract_inputs):
    os.environ["SPARK_JARS_PACKAGES"] = ",".join(
        [
            "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.12.0",
            "org.apache.spark:spark-protobuf_2.13:4.1.3",
            "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.3",
        ]
    )
    os.environ["BATCH_LOCAL_WAREHOUSE"] = str(tmp_path_factory.mktemp("warehouse"))
    from batch.spark import session

    session_ = session("batch-tests")
    yield session_
    session_.stop()
