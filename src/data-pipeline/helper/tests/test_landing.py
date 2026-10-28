import pytest

from pipeline_common.landing import selection, state_key
from pipeline_common.storage import Store


def test_selection_and_epoch_are_validated(tmp_path):
    store = Store(str(tmp_path))
    store.put("selections/version.json", {"dataset_version": "other"})
    with pytest.raises(ValueError, match="selection"):
        selection(store, "version")
    with pytest.raises(ValueError, match="epoch"):
        state_key("../outside")


def test_installed_contract_root_is_found_from_project_ancestors(tmp_path, monkeypatch):
    from pipeline_common import products

    root = tmp_path / "checkout"
    contracts = root / "contracts/data-products"
    contracts.mkdir(parents=True)
    installed = root / "src/data-pipeline/helper/.venv/lib/python3.12/site-packages/pipeline_common/products.py"
    installed.parent.mkdir(parents=True)
    monkeypatch.delenv("DATA_PRODUCTS_ROOT", raising=False)
    monkeypatch.setattr(products, "__file__", str(installed))
    monkeypatch.chdir(tmp_path)
    assert products.contracts_root() == contracts
