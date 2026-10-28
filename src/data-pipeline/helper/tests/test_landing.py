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
