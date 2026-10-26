import json

import pytest
from features.reference import DEFINITION_PATH, compute

EXAMPLES = json.loads(DEFINITION_PATH.with_suffix(".examples.json").read_text())


@pytest.mark.parametrize("example", EXAMPLES, ids=lambda example: example["name"])
def test_shared_feature_conformance(example):
    arguments = {key: value for key, value in example.items() if key not in {"name", "expected", "error"}}
    if "error" in example:
        with pytest.raises(ValueError, match=example["error"]):
            compute(**arguments)
    else:
        assert compute(**arguments)["values"] == example["expected"]
