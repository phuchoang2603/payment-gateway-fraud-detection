import copy
import json
from datetime import timedelta

import pytest
from features.reference import DEFINITION_PATH, compute, expiration_anchors, instant

FIXTURES = json.loads(DEFINITION_PATH.with_suffix(".examples.json").read_text())
EXAMPLES = FIXTURES["cases"]


def override(base, changes):
    result = copy.deepcopy(base)
    for key, value in changes.items():
        result[key] = (
            override(result[key], value)
            if isinstance(value, dict) and isinstance(result.get(key), dict)
            else copy.deepcopy(value)
        )
    return result


def example_input(example):
    arguments = override(FIXTURES["defaults"], example.get("input", {}))
    arguments["events"] = [
        override(
            FIXTURES["event_templates"][event["use"]],
            {k: v for k, v in event.items() if k != "use"},
        )
        for event in example["events"]
    ]
    return arguments


@pytest.mark.parametrize("example", EXAMPLES, ids=lambda example: example["name"])
def test_shared_feature_conformance(example):
    arguments = example_input(example)
    if "error" in example:
        with pytest.raises(ValueError, match=example["error"]):
            compute(**arguments)
    else:
        result = compute(**arguments)
        assert {key: result[key] for key in example["expected"]} == example["expected"]


def test_historical_clock_expiration_keeps_device_until_last_occurrence():
    arguments = example_input(EXAMPLES[0])
    first = instant("2025-08-31T12:00:00+00:00")
    starts = []
    for index, started in enumerate((first, first + timedelta(hours=1))):
        start = copy.deepcopy(arguments["events"][0])
        start["event_id"] += str(index)
        start["payment_id"] += str(index)
        start["recorded_at"] = started.isoformat()
        start["attempt_started"]["signals"]["attempted_at"] = started.isoformat()
        starts.append(start)
    arguments["events"] = starts
    anchors = expiration_anchors(starts, after=first + timedelta(days=1))
    assert anchors == [
        first + timedelta(days=1, microseconds=1),
        first + timedelta(days=1, hours=1, microseconds=1),
    ]
    counts = []
    for anchor in [first + timedelta(days=1), *anchors]:
        arguments["current"]["attempt_started"]["signals"]["attempted_at"] = anchor.isoformat()
        arguments["cutoff"] = anchor.isoformat()
        for view in arguments["views"].values():
            view["anchor"] = anchor.isoformat()
        counts.append(compute(**arguments)["values"]["customer_snapshot_distinct_devices_24h"])
    assert counts == [1, 1, 0]
