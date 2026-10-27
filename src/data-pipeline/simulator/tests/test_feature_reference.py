import copy
import json
from datetime import timedelta

import pytest
from features.reference import DEFINITION_PATH, compute, elapsed_seconds, expiration_anchors, instant

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
        override(FIXTURES["event_templates"][event["use"]], {k: v for k, v in event.items() if k != "use"})
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
    first = instant("2025-08-25T12:00:00+00:00")
    starts = []
    events = []
    for index, started in enumerate((first, first + timedelta(hours=1))):
        start, decision = copy.deepcopy(arguments["events"][1]), copy.deepcopy(arguments["events"][0])
        for event in (start, decision):
            event["event_id"] += str(index)
            event["payment_id"] += str(index)
            event["occurred_at"] = started.isoformat()
            event["recorded_at"] = started.isoformat()
        starts.append(start)
        events.extend((decision, start))
    arguments["events"] = events
    anchors = expiration_anchors(starts, after=first + timedelta(days=7), scope="customer")
    assert anchors == [first + timedelta(days=7, microseconds=1), first + timedelta(days=7, hours=1, microseconds=1)]
    counts, elapsed = [], []
    for anchor in [first + timedelta(days=7), *anchors]:
        arguments["current"]["occurred_at"] = anchor.isoformat()
        arguments["cutoff"] = anchor.isoformat()
        for view in arguments["views"].values():
            view["anchor"] = anchor.isoformat()
        result = compute(**arguments)
        counts.append(result["values"]["customer_snapshot_distinct_devices_7d"])
        elapsed.append(result["values"]["customer_seconds_since_snapshot_last_assessed_7d"])
    assert counts == [1, 1, 0]
    assert elapsed == [601200, 601200, None]


def test_future_last_assessed_is_unavailable():
    assert elapsed_seconds("2025-09-01T12:00:00.000001+00:00", "2025-09-01T12:00:00+00:00") is None
