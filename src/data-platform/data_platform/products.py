import os
from hashlib import sha256
from pathlib import Path

PRODUCTS = Path(os.environ.get("DATA_PRODUCTS_ROOT", Path(__file__).resolve().parents[3] / "contracts/data-products"))
PORTS = {"payment_events": "payment-events", "fraud_labels": "fraud-labels"}
AUDIT = ("_source_dataset_version", "_source_manifest_sha256", "_source_file", "_ingestion_run_id", "_ingested_at")


def contract_path(table, product="bronze-payments"):
    return PRODUCTS / product / f"{PORTS[table]}.odcs.yaml"


def hashes():
    return {table: sha256(contract_path(table).read_bytes()).hexdigest() for table in PORTS}
