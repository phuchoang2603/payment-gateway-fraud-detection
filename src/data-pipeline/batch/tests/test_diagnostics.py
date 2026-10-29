from batch import diagnostics
from batch.schema import EVENT_COLUMNS, business_schema
from tests.events import approved, created, started


def test_declared_field_nulls_are_counted_against_their_payload_per_version(spark):
    v3 = approved("pay-1", "e-1", 2, 1, "att-1")
    v2 = approved("pay-2", "e-2", 2, 1, "att-2")
    v2["schema_version"] = 2
    v2["risk_decision_recorded"]["assessment"]["assessment_id"] = None
    v2["risk_decision_recorded"]["assessment"]["evidence_sha256"] = None
    events = spark.createDataFrame([v3, v2], schema=business_schema(spark, EVENT_COLUMNS))

    versions = {row["schema_version"]: row for row in diagnostics.version_nulls(events)}

    assert versions[2]["risk_assessment.assessment_id"] == {"nulls": 1, "denominator": 1}
    assert versions[3]["risk_assessment.assessment_id"] == {"nulls": 0, "denominator": 1}
    assert versions[3]["attempt_signals.user_agent_family"]["denominator"] == 0


def test_purchase_shares_use_purchases_as_denominator(spark):
    rows = [
        created("pay-1", "c-1"),
        created("pay-2", "c-2"),
        created("pay-3", "c-3"),
        started("pay-1", "s-1", 2, 1, "a-1"),
    ]
    events = spark.createDataFrame(rows, schema=business_schema(spark, EVENT_COLUMNS))

    report = diagnostics.report(events)

    assert report["purchases"]["count"] == 3
    assert report["purchases"]["top_merchant_share"] == {"value": 1.0, "denominator": 3}
    assert report["near_duplicate_rate"]["denominator"] == 4
