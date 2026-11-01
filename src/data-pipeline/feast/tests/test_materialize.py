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
    explicit = materialize.run(spark, feature_store, now + timedelta(hours=2), 2, DAY.date())
    assert verify_online.validate(feature_store, explicit) == 2
    assert all(w["start"] == DAY.isoformat() for w in explicit["windows"].values())
    assert all(
        w["end"] == (now + timedelta(hours=2)).isoformat() for w in explicit["windows"].values()
    )


def test_missing_online_sample_fails(feature_store):
    expected = {
        "integration_id": "i",
        "customer_id": "absent",
        "event_timestamp": DAY,
        **dict.fromkeys(FIELDS, 7),
    }
    with pytest.raises(ValueError, match="differs"):
        verify_online.validate(feature_store, {"samples": {"customer": [expected]}})
