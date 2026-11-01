import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pandas as pd
from feast import FeatureView

from fraud_feast.repo import DAILY_FIELDS, DAILY_TABLES, KEYS, STREAM_FIELDS, TABLES, definitions

CONTRACTS = Path(__file__).resolve().parents[4] / "contracts" / "features"


def test_fields_equal_contracts(monkeypatch):
    monkeypatch.setenv("FEAST_STREAM_EPOCHS", "history-ci,live-ci")
    batch = json.loads((CONTRACTS / "fraud-history-batch-v1.yaml").read_text())
    stream = json.loads((CONTRACTS / "fraud-snapshot-v2.yaml").read_text())
    for scope in KEYS:
        expected = {
            feature["name"].replace("{w}", window)
            for feature in batch["features"]
            if scope in feature["scope"].split(", ")
            for window in batch["windows"]
        }
        assert set(DAILY_FIELDS[scope]) == expected
        wire_scope = "method" if scope == "payment_method" else scope
        assert set(STREAM_FIELDS[scope]) == {
            feature["name"] for feature in stream["features"] if feature["scope"] == wire_scope
        }
    views = {item.name: item for item in definitions() if isinstance(item, FeatureView)}
    assert set(views) == {f"{scope}_{kind}" for scope in KEYS for kind in ("daily", "stream")}
    assert all(view.online for view in views.values())


def test_point_in_time_arrow_with_daily_created_ttl_epochs_and_missing_method(spark, feature_store):
    day = datetime(2025, 9, 1, tzinfo=UTC)
    for scope, key in KEYS.items():
        fields = DAILY_FIELDS[scope]
        columns = (
            f"integration_id string, {key} string, event_timestamp timestamp, created timestamp, "
            + ", ".join(
                f"{field} {'double' if field.startswith('decline_rate') else 'long'}"
                for field in fields
            )
        )
        rows = [
            (
                "i",
                "entity",
                day,
                day + timedelta(days=2),
                *[1.0 if f.startswith("decline_rate") else 7 for f in fields],
            )
        ]
        spark.createDataFrame(rows, columns).writeTo(f"lake.{DAILY_TABLES[scope]}").using(
            "iceberg"
        ).createOrReplace()
        columns = (
            f"integration_id string, {key} string, event_timestamp timestamp, created timestamp, epoch string, "
            + ", ".join(f"{field} long" for field in STREAM_FIELDS[scope])
        )
        rows = [
            (
                "i",
                "entity",
                day + timedelta(hours=hour),
                day + timedelta(days=2),
                epoch,
                *[count] * len(STREAM_FIELDS[scope]),
            )
            for hour, epoch, count in (
                (1, "history-ci", 1),
                (2, "live-ci", 2),
                (3, "ignored", 99),
                (4, "live-ci", 4),
            )
        ]
        spark.createDataFrame(rows, columns).writeTo(f"lake.{TABLES[scope]}").using(
            "iceberg"
        ).createOrReplace()
    started = [
        day + timedelta(hours=1, minutes=1),
        day + timedelta(hours=4),
        day + timedelta(days=33),
    ]
    labels = pd.DataFrame(
        {
            "label_attempt": ["history", "live", "expired", "no-method"],
            "integration_id": ["i"] * 4,
            "customer_id": ["entity"] * 4,
            "payment_method_fingerprint": ["entity", "entity", "entity", None],
            "event_timestamp": [
                *[t - timedelta(microseconds=1) for t in started],
                day + timedelta(hours=2, minutes=1),
            ],
        }
    )
    result = (
        feature_store.get_historical_features(
            entity_df=labels,
            features=feature_store.get_feature_service("fraud_model_v1"),
            full_feature_names=True,
        )
        .to_arrow()
        .to_pandas()
        .set_index("label_attempt")
    )
    assert result.loc["history", "customer_daily__attempt_count_7d"] == 7
    assert result.loc["history", "customer_stream__customer_snapshot_attempt_count_5m"] == 1
    assert result.loc["live", "customer_stream__customer_snapshot_attempt_count_5m"] == 2
    assert pd.isna(result.loc["expired", "customer_daily__attempt_count_7d"])
    assert pd.isna(result.loc["expired", "customer_stream__customer_snapshot_attempt_count_5m"])
    assert pd.isna(result.loc["no-method", "payment_method_daily__attempt_count_7d"])
    assert pd.isna(
        result.loc["no-method", "payment_method_stream__method_snapshot_attempt_count_5m"]
    )
