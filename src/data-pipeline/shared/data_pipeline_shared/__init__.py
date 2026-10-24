import os
from pathlib import Path


def data_products_root():
    if root := os.environ.get("DATA_PRODUCTS_ROOT"):
        return Path(root)
    for parent in Path(__file__).resolve().parents:
        root = parent / "contracts/data-products"
        if root.is_dir():
            return root
    raise FileNotFoundError("Set DATA_PRODUCTS_ROOT when running outside the repository")
