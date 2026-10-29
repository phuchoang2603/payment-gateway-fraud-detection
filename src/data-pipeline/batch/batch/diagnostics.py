import json
import os

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from batch.bronze import EVENTS, current_snapshot
from batch.spark import session
from batch.store import Store


def report(events: DataFrame, purchases_per_top: int = 10):
    purchases = events.where(F.col("payment_created").isNotNull())
    purchase_count = purchases.count()
    merchants = purchases.groupBy(F.col("payment_created.merchant.merchant_id").alias("merchant")).count()
    ranked = merchants.orderBy(F.desc("count"))
    top = ranked.limit(purchases_per_top).agg(F.sum("count").alias("n")).collect()[0]["n"] or 0
    items = purchases.select(F.explode("payment_created.snapshot.items").alias("item"))
    item_count = items.count()
    categories = items.groupBy(F.col("item.category").alias("category")).count()
    top_category = categories.orderBy(F.desc("count")).limit(1).collect()
    distinct_events = events.select("event_id").distinct().count()
    total_rows = events.count()
    return {
        "snapshot_rows": total_rows,
        "distinct_events": distinct_events,
        "near_duplicate_rate": {
            "numerator": "deliveries beyond the first per event_id",
            "value": (total_rows - distinct_events) / distinct_events if distinct_events else None,
            "denominator": distinct_events,
        },
        "purchases": {
            "count": purchase_count,
            "top_merchant_share": {
                "value": ranked.first()["count"] / purchase_count if purchase_count else None,
                "denominator": purchase_count,
            },
            "top_10_merchant_share": {
                "value": top / purchase_count if purchase_count else None,
                "denominator": purchase_count,
            },
        },
        "line_items": {
            "count": item_count,
            "top_category_share": {
                "value": top_category[0]["count"] / item_count if top_category else None,
                "category": top_category[0]["category"] if top_category else None,
                "denominator": item_count,
            },
        },
        "schema_versions": version_nulls(events),
        "cardinality": {
            "method": "approx_count_distinct, estimate with default relative standard deviation 5%",
            "customers": approx_distinct(events, F.col("payment_created.customer.customer_id")),
            "merchants": approx_distinct(events, F.col("payment_created.merchant.merchant_id")),
            "devices": approx_distinct(events, F.col("attempt_started.signals.device_fingerprint")),
        },
    }


def version_nulls(events: DataFrame):
    declared = {
        "risk_assessment.assessment_id": ("risk_decision_recorded", "risk_decision_recorded.assessment.assessment_id"),
        "risk_assessment.evidence_sha256": (
            "risk_decision_recorded",
            "risk_decision_recorded.assessment.evidence_sha256",
        ),
        "attempt_signals.user_agent_family": ("attempt_started", "attempt_started.signals.user_agent_family"),
    }
    rows = []
    for version in events.select("schema_version").distinct().orderBy("schema_version").collect():
        scoped = events.where(F.col("schema_version") == version["schema_version"])
        fields = {}
        for name, (payload, column) in declared.items():
            present = scoped.where(F.col(payload).isNotNull())
            fields[name] = {
                "nulls": present.where(F.col(column).isNull()).count(),
                "denominator": present.count(),
            }
        rows.append({"schema_version": version["schema_version"], "rows": scoped.count(), **fields})
    return rows


def approx_distinct(events: DataFrame, column):
    return events.agg(F.approx_count_distinct(column).alias("n")).collect()[0]["n"]


def main():
    run_id = os.environ["RUN_ID"]
    spark = session("dp1-diagnostics")
    try:
        snapshot = current_snapshot(spark, EVENTS)
        events = spark.read.option("versionAsOf", snapshot).table(EVENTS)
        result = {"run_id": run_id, "bronze_snapshot_id": snapshot, **report(events)}
        Store(os.environ["RUN_REPORT_ROOT"]).write_json(f"{run_id}/diagnostics.json", result)
        print(json.dumps(result, sort_keys=True, default=str))
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
