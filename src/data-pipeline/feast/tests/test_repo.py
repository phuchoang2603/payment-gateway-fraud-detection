import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pandas as pd
from feast import FeatureService, FeatureView
from pyspark.sql import functions as F

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
    assert all(
        "epoch" in {field.name for field in views[f"{scope}_stream"].schema} for scope in KEYS
    )
    service = next(item for item in definitions() if isinstance(item, FeatureService))
    assert all(
        field.name != "epoch"
        for projection in service.feature_view_projections
        for field in projection.features
    )


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
                day + timedelta(hours=hour),
                day + timedelta(days=2),
                *[1.0 if f.startswith("decline_rate") else count for f in fields],
            )
            for hour, count in ((0, 7), (24, 8))
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
                day + timedelta(hours=hour, microseconds=1),
                day + timedelta(days=2),
                epoch,
                *[count] * len(STREAM_FIELDS[scope]),
            )
            for hour, epoch, count in (
                (1, "history-ci", 1),
                (2, "live-ci", 2),
                (3, "ignored", 99),
                (24, "live-ci", 4),
            )
        ]
        spark.createDataFrame(rows, columns).writeTo(f"lake.{TABLES[scope]}").using(
            "iceberg"
        ).createOrReplace()
    started = [
        day + timedelta(hours=1, minutes=1),
        day + timedelta(days=1),
        day + timedelta(days=33),
        day + timedelta(hours=2, minutes=1),
        day,
        day + timedelta(days=1, microseconds=1),
    ]
    labels = pd.DataFrame(
        {
            "label_attempt": ["history", "live", "expired", "no-method", "midnight", "visible"],
            "integration_id": ["i"] * len(started),
            "customer_id": ["entity"] * len(started),
            "payment_method_fingerprint": ["entity", "entity", "entity", None, "entity", "entity"],
            "event_timestamp": started,
            "label": [True, False, False, True, False, True],
        }
    )
    labels = labels.assign(
        scored_at=labels["event_timestamp"],
        amount_minor=1200,
        customer_account_created_at=[
            day - timedelta(days=10),
            None,
            *[day - timedelta(days=10)] * 4,
        ],
        merchant_account_created_at=[
            *[day - timedelta(days=20)] * 3,
            None,
            *[day - timedelta(days=20)] * 2,
        ],
        shipping_country=["US", "US", None, None, "US", "US"],
        ip_country=["CA", None, "US", None, "US", "US"],
        item_count=2,
        has_device=False,
    )
    cutoff = day + timedelta(days=40)
    source = labels.assign(
        label_source="LABEL_SOURCE_SIMULATION_TRUTH", label_available_at=labels["event_timestamp"]
    )
    chargeback = source.iloc[[0]].assign(label_source="LABEL_SOURCE_CHARGEBACK", label=False)
    unavailable = source.iloc[[0]].assign(
        label_attempt="unavailable", label_available_at=cutoff + timedelta(microseconds=1)
    )
    spark.createDataFrame(pd.concat([source, chargeback, unavailable])).writeTo(
        "lake.gold.label_attempt"
    ).using("iceberg").createOrReplace()
    entities = (
        spark.table("lake.gold.label_attempt")
        .where(
            (F.col("label_source") == "LABEL_SOURCE_SIMULATION_TRUTH")
            & (F.col("label_available_at") <= F.lit(cutoff))
        )
        .drop("label_source", "label_available_at")
    )
    result = (
        feature_store.get_historical_features(
            entity_df=entities,
            features=feature_store.get_feature_service("payment_risk_v1"),
            full_feature_names=True,
        )
        .to_arrow()
        .to_pandas()
        .set_index("label_attempt")
    )
    assert result.loc["history", "customer_daily__attempt_count_7d"] == 7
    assert result.loc["history", "customer_stream__customer_snapshot_attempt_count_5m"] == 1
    assert result.loc["live", "customer_stream__customer_snapshot_attempt_count_5m"] == 2
    assert result.loc["live", "customer_daily__attempt_count_7d"] == 8
    assert result.loc["midnight", "customer_daily__attempt_count_7d"] == 7
    assert pd.isna(result.loc["midnight", "customer_stream__customer_snapshot_attempt_count_5m"])
    assert result.loc["visible", "customer_stream__customer_snapshot_attempt_count_5m"] == 4
    assert result["label"].to_dict() == labels.set_index("label_attempt")["label"].to_dict()
    assert pd.isna(result.loc["expired", "customer_daily__attempt_count_7d"])
    assert pd.isna(result.loc["expired", "customer_stream__customer_snapshot_attempt_count_5m"])
    assert pd.isna(result.loc["no-method", "payment_method_daily__attempt_count_7d"])
    assert pd.isna(
        result.loc["no-method", "payment_method_stream__method_snapshot_attempt_count_5m"]
    )

    from fastapi.testclient import TestClient

    from fraud_feast.request import REQUEST_FEATURES, REQUEST_FIELDS
    from fraud_feast.server import get_app

    with TestClient(get_app(feature_store)) as client:
        for index in (0, 1, 2, 3):
            example = labels.iloc[index]
            entities = {
                key: ["" if pd.isna(example[key]) else example[key]]
                for key in ("integration_id", "customer_id", "payment_method_fingerprint")
            }
            for key in REQUEST_FIELDS:
                value = example[key]
                if pd.isna(value):
                    value = None
                elif hasattr(value, "isoformat"):
                    value = value.isoformat()
                elif hasattr(value, "item"):
                    value = value.item()
                entities[key] = [value]
            response = client.post(
                "/get-online-features",
                json={
                    "feature_service": "payment_risk_v1",
                    "entities": entities,
                    "full_feature_names": True,
                },
            )
            assert response.status_code == 200, response.text
            body = response.json()
            actual = dict(zip(body["metadata"]["feature_names"], body["results"]))
            for field in REQUEST_FEATURES:
                name = f"request_features__{field}"
                expected = result.loc[example["label_attempt"], name]
                if field.endswith("_amount_to_30d_avg"):
                    # This test writes no daily rows to Redis, so the online ratio has no average.
                    assert actual[name]["values"][0] is None
                    continue
                assert actual[name]["values"][0] == (None if pd.isna(expected) else expected)
    assert result.loc["history", "request_features__customer_account_age_days"] == 10
    assert result.loc["history", "request_features__merchant_account_age_days"] == 20
    assert result.loc["history", "request_features__ip_shipping_country_mismatch"]
    assert pd.isna(result.loc["live", "request_features__ip_shipping_country_mismatch"])

    for attempt in ("live", "expired", "no-method"):
        assert pd.isna(result.loc[attempt, "request_features__ip_shipping_country_mismatch"])

    assert pd.isna(result.loc["live", "request_features__customer_account_age_days"])
    assert pd.isna(result.loc["no-method", "request_features__merchant_account_age_days"])
    # Daily rows hold count 7 or 8 for both amount_sum_minor_30d and attempt_count_30d, so the average is 1.
    assert result.loc["history", "request_features__customer_amount_to_30d_avg"] == 1200
    assert result.loc["live", "request_features__payment_method_amount_to_30d_avg"] == 1200
    assert pd.isna(result.loc["expired", "request_features__customer_amount_to_30d_avg"])
    assert pd.isna(result.loc["no-method", "request_features__payment_method_amount_to_30d_avg"])
    assert result.loc["history", "request_features__scored_hour_utc"] == 1
    assert result.loc["live", "request_features__scored_hour_utc"] == 0
