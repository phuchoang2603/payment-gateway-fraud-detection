import os
from pathlib import Path


def products_root():
    configured = os.environ.get("DATA_PRODUCTS_ROOT")
    if configured:
        return Path(configured)
    for base in (Path.cwd(), Path(__file__).resolve()):
        for parent in (base, *base.parents):
            candidate = parent / "contracts/data-products"
            if candidate.is_dir():
                return candidate
    raise FileNotFoundError("set DATA_PRODUCTS_ROOT to the data-product contracts directory")
