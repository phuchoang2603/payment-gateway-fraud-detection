import json
import struct
from datetime import UTC, datetime

import pytest

from fraud_risk_serving.features import VIEW_FIELDS, RiskRequest
from fraud_risk_serving.model import HEADER

NOW = datetime(2026, 11, 4, 7, tzinfo=UTC)
ORDER = ["request_features__amount_minor", "customer_daily__attempt_count_7d", "request_features__has_device"]


@pytest.fixture
def request_input():
    return RiskRequest(
        integration_id="merchant",
        customer_id="customer",
        payment_method_fingerprint="fingerprint",
        amount_minor=12000,
        customer_account_created_at="2025-01-01T00:00:00Z",
        merchant_account_created_at="2024-01-01T00:00:00Z",
        shipping_country="US",
        ip_country="US",
        item_count=2,
        has_device=True,
    )


def feast_response(timestamp=NOW):
    names, results = [], []
    for view, fields in VIEW_FIELDS.items():
        for field in fields:
            names.append(f"{view}__{field}")
            value = True if field in ("has_device", "ip_shipping_country_mismatch") else 1
            results.append({"values": [value], "statuses": ["PRESENT"], "event_timestamps": [timestamp.isoformat()]})
    return {"metadata": {"feature_names": names}, "results": results}


def model_config(order=ORDER, version="v4"):
    return {
        "parameters": {
            key: {"string_value": value}
            for key, value in {
                "feature_order": json.dumps(order),
                "registry_version": version,
                "decline_threshold": "0.7",
            }.items()
        }
    }


def inference(score=0.8, version="4"):
    header = json.dumps(
        {
            "model_version": version,
            "outputs": [
                {"name": "output__0", "shape": [1, 2], "datatype": "FP32", "parameters": {"binary_data_size": 8}}
            ],
        }
    ).encode()
    return {"content": header + struct.pack("<2f", 1 - score, score), "headers": {HEADER: str(len(header))}}


def reference(version="v4"):
    return {
        "version": version,
        "reference": {name: {"edges": [0, 1, 10], "share": [0.1, 0.3, 0.4, 0.1, 0.1], "gain": 1} for name in ORDER},
    }
