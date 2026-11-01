from datetime import UTC, datetime, timedelta

import pytest

from fraud_feast import materialize, verify_online
from fraud_feast.repo import DAILY_FIELDS

DAY = datetime(2025, 9, 1, tzinfo=UTC)
FIELDS = DAILY_FIELDS["customer"]


def test_overlap_rewrites_equal_anchor_and_deleted_entity_keeps_prior_value(spark, feature_store):
    from fraud_feast.repo import DAILY_TABLES, DAILY_TTL, KEYS

    now = DAY + timedelta(days=3)

    def save(scope, values):
        key = KEYS[scope]
        fields = DAILY_FIELDS[scope]
        schema = (
            f"integration_id string, {key} string, event_timestamp timestamp, created timestamp, "
            + ", ".join(
                f"{f} {'double' if f.startswith('decline_rate') else 'long'}" for f in fields
            )
        )

        spark.createDataFrame(
            [
                (
                    "overlap",
                    entity,
                    DAY,
                    now,
                    *[0.25 if f.startswith("decline_rate") else n for f in fields],
                )
                for entity, n in values
            ],
            schema,
        ).writeTo(f"lake.{DAILY_TABLES[scope]}").using("iceberg").createOrReplace()

    for scope in KEYS:
        save(scope, [("updated", 1), ("deleted", 1)])
    first = materialize.run(spark, feature_store, now, 2)
    assert verify_online.validate(feature_store, first) == 4
    assert all(w["start"] == (now - DAILY_TTL).isoformat() for w in first["windows"].values())
    for scope in KEYS:
        save(scope, [("updated", 2)])
    report = materialize.run(spark, feature_store, now + timedelta(hours=1), 2)
    assert verify_online.validate(feature_store, report) == 2
    for scope, key in KEYS.items():
        assert report["windows"][f"{scope}_daily"]["start"] == (now - timedelta(days=3)).isoformat()
        for entity, expected in [("updated", 2), ("deleted", 1)]:
            result = feature_store.get_online_features(
                features=[f"{scope}_daily:attempt_count_7d"],
                entity_rows=[{"integration_id": "overlap", key: entity}],
            ).to_dict()
            assert result["attempt_count_7d"] == [expected]


def test_missing_online_sample_fails(feature_store):
    expected = {
        "integration_id": "i",
        "customer_id": "absent",
        "event_timestamp": DAY,
        **dict.fromkeys(FIELDS, 7),
    }
    with pytest.raises(ValueError, match="differs"):
        verify_online.validate(feature_store, {"samples": {"customer": [expected]}})


def test_stream_recovery_materializes_latest_retained_rows(spark, feature_store):
    from test_stream_writer import row

    from fraud_feast.repo import KEYS, STREAM_FIELDS, STREAM_TTL, TABLES
    from fraud_feast.stream_writer import write_online

    now = DAY + timedelta(days=3)
    for scope, key in KEYS.items():
        fields = STREAM_FIELDS[scope]
        records = [
            row(scope, at=now - timedelta(minutes=1), value=7),
            {**row(scope, at=now - STREAM_TTL - timedelta(microseconds=1)), key: "expired"},
            {**row(scope, at=now - timedelta(minutes=1), value=7), key: "fresh"},
        ]
        schema = (
            f"integration_id string, {key} string, event_timestamp timestamp, created timestamp, "
            "epoch string, " + ", ".join(f"{field} long" for field in fields)
        )
        spark.createDataFrame(records, schema).writeTo(f"lake.{TABLES[scope]}").using(
            "iceberg"
        ).createOrReplace()
        write_online(
            feature_store,
            f"{scope}_stream",
            [{**row(scope, at=now, value=9), key: "fresh"}],
            fields,
        )
    report = materialize.run(spark, feature_store, now, 0, stream_recovery=True)
    assert all(
        window["start"] == (now - STREAM_TTL).isoformat() and window["end"] == now.isoformat()
        for window in report["windows"].values()
    )
    for scope, key in KEYS.items():
        field = STREAM_FIELDS[scope][0]
        result = feature_store.get_online_features(
            features=[f"{scope}_stream:{field}"],
            entity_rows=[
                {"integration_id": "i", key: "entity"},
                {"integration_id": "i", key: "expired"},
                {"integration_id": "i", key: "fresh"},
            ],
        ).to_dict()
        assert result[field] == [7, None, 9]
