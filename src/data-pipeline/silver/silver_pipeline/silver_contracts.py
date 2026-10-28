import os
from hashlib import sha256
from pathlib import Path

from data_platform.products import PRODUCTS, SILVER_PORTS


def contract_path(table):
    if table not in SILVER_PORTS:
        raise ValueError("unknown Silver output contract")
    return PRODUCTS / "silver-payments" / f"{table.replace('_', '-')}.odcs.yaml"


def hashes():
    return {table: sha256(contract_path(table).read_bytes()).hexdigest() for table in SILVER_PORTS}


def product_hash():
    return sha256((PRODUCTS / "silver-payments/product.odps.yaml").read_bytes()).hexdigest()


def quality_hash():
    root = Path(os.environ.get("DATA_QUALITY_ROOT") or PRODUCTS.parent / "quality")
    return sha256((root / "silver-payments.yaml").read_bytes()).hexdigest()


def schema(table):
    from data_platform.contracts import prepare

    return prepare(table, contract_path(table)).schema
