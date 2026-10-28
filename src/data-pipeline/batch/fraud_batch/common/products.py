import os
from hashlib import sha256
from pathlib import Path


def contracts_root():
    configured = os.environ.get("DATA_PRODUCTS_ROOT")
    if configured:
        return Path(configured)
    for base in (Path.cwd(), Path(__file__).resolve()):
        for parent in (base, *base.parents):
            candidate = parent / "contracts/data-products"
            if candidate.is_dir():
                return candidate
    raise FileNotFoundError("set DATA_PRODUCTS_ROOT to the data-product contracts directory")


PRODUCTS = contracts_root()
PORTS = {"payment_events": "payment-events", "fraud_labels": "fraud-labels"}
SILVER_PORTS = (
    "payment_events",
    "event_delivery_provenance",
    "attempt_histories",
    "label_observations",
    "label_delivery_provenance",
)
AUDIT = (
    "_source_dataset_version",
    "_source_manifest_sha256",
    "_source_file",
    "_source_schema_version",
    "_ingestion_run_id",
    "_ingested_at",
    "_source_epoch",
    "_source_batch",
    "_transport",
)


def contract_path(table, product="bronze-payments"):
    return PRODUCTS / product / f"{PORTS[table]}.odcs.yaml"


def hashes():
    return {table: sha256(contract_path(table).read_bytes()).hexdigest() for table in PORTS}
