import json
from datetime import datetime, timezone

from data_platform.manifest import physical_schemas
from pyspark.sql import Window
from pyspark.sql import functions as F

from silver_pipeline.silver_runs import digest


def distribution(frame, name, denominator):
    counts = frame.groupBy(F.col(name).alias("value")).count()
    return (
        counts.withColumn("denominator", F.sum("count").over(Window.partitionBy()))
        .withColumn("share", F.col("count") / F.col("denominator"))
        .withColumn("denominator_kind", F.lit(denominator))
    )


def source_diagnostics(events, canonical, provenance, relative_error=0.05):
    if not 0 < relative_error <= 0.39:
        raise ValueError("relative error must be greater than zero and at most 0.39")
    created = canonical.where(F.col("payment_created").isNotNull())
    merchant = created.select(F.col("payment_created.merchant.merchant_id").alias("merchant"))
    region = created.select(F.col("payment_created.snapshot.shipping.region").alias("region"))
    categories = created.select(F.explode("payment_created.snapshot.items").alias("item")).select(
        F.col("item.category").alias("category")
    )
    started = canonical.where(F.col("attempt_started").isNotNull())
    identity_columns = {
        "customer": "attempt_started.context.customer.customer_id",
        "merchant": "attempt_started.context.merchant.merchant_id",
        "device": "attempt_started.signals.device_fingerprint",
        "ip_network": "attempt_started.signals.ip_network_fingerprint",
        "payment_method": "attempt_started.signals.payment_method.fingerprint",
        "address": "attempt_started.context.snapshot.shipping.address_fingerprint",
    }
    cardinality = started.agg(
        *[
            F.approx_count_distinct(
                F.when(F.col(column).isNotNull(), F.struct("integration_id", F.col(column))), relative_error
            ).alias(name)
            for name, column in identity_columns.items()
        ],
        F.count(F.lit(1)).alias("attempt_denominator"),
    ).withColumn("relative_error", F.lit(relative_error))
    decisions = events.where(F.col("risk_decision_recorded").isNotNull())
    assessment = "risk_decision_recorded.assessment"
    references = decisions.groupBy("_source_schema_version", F.to_date("occurred_at").alias("event_date")).agg(
        F.count(F.lit(1)).alias("eligible_deliveries"),
        F.sum(F.when(F.col(f"{assessment}.assessment_id").isNull(), 1).otherwise(0)).alias("assessment_id_nulls"),
        F.sum(F.when(F.col(f"{assessment}.evidence_sha256").isNull(), 1).otherwise(0)).alias("evidence_sha256_nulls"),
        F.sum(F.when(F.col(f"{assessment}.assessment_id").isNotNull(), 1).otherwise(0)).alias("assessment_id_values"),
        F.sum(F.when(F.col(f"{assessment}.evidence_sha256").isNotNull(), 1).otherwise(0)).alias(
            "evidence_sha256_values"
        ),
        F.sum(
            F.when(
                F.col(f"{assessment}.assessment_id").isNotNull()
                & (F.length(F.col(f"{assessment}.assessment_id")) == 0),
                1,
            ).otherwise(0)
        ).alias("invalid_assessment_ids"),
        F.sum(
            F.when(
                F.col(f"{assessment}.evidence_sha256").isNotNull()
                & ~F.col(f"{assessment}.evidence_sha256").rlike("^[a-f0-9]{64}$"),
                1,
            ).otherwise(0)
        ).alias("invalid_evidence_hashes"),
    )
    versions = physical_schemas()["payment_events"]
    for field in ("assessment_id", "evidence_sha256"):
        defined = F.create_map(
            *[
                value
                for version, definition in versions.items()
                for value in (F.lit(version), F.lit(field in definition["assessment_fields"]))
            ]
        )
        references = references.withColumn(f"{field}_physically_present", defined[F.col("_source_schema_version")])
    accounting = provenance.agg(
        *[
            F.coalesce(F.sum(column), F.lit(0)).alias(column)
            for column in (
                "deliveries",
                "representative_deliveries",
                "identical_duplicate_deliveries",
                "pending_context_deliveries",
                "conflicting_deliveries",
            )
        ]
    ).withColumn(
        "duplicate_rate",
        F.when(F.col("deliveries") > 0, F.col("identical_duplicate_deliveries") / F.col("deliveries")),
    )
    return {
        "merchant_shares": distribution(merchant, "merchant", "canonical_payment_created_events"),
        "region_shares": distribution(region, "region", "canonical_payment_created_events"),
        "category_shares": distribution(categories, "category", "canonical_checkout_line_items"),
        "identity_cardinality": cardinality,
        "assessment_references": references,
        "event_delivery_accounting": accounting,
    }


def retain_diagnostics(store, candidate, frames):
    limits = {
        "merchant_shares": 20,
        "region_shares": 1000,
        "category_shares": 1000,
        "identity_cardinality": 1,
        "assessment_references": 10000,
        "event_delivery_accounting": 1,
    }
    results = {}
    for name, limit in limits.items():
        frame = frames.diagnostics[name]
        if name.endswith("_shares"):
            frame = frame.orderBy(F.col("count").desc(), F.col("value").asc())
        rows = frame.limit(limit + 1).collect()
        results[name] = {"rows": [row.asDict(recursive=True) for row in rows[:limit]], "truncated": len(rows) > limit}
    duplicates = frames.events.canonical.groupBy("integration_id", "event_id").count().where("count > 1")
    duplicate_rows = duplicates.agg(F.coalesce(F.sum(F.col("count") - 1), F.lit(0)).alias("duplicates")).first()[0]
    canonical_rows = frames.events.canonical.count()
    results["post_dedup_duplicates"] = {
        "extra_deliveries": duplicate_rows,
        "canonical_events": canonical_rows,
        "rate": duplicate_rows / canonical_rows if canonical_rows else None,
    }
    document = {
        "candidate_id": candidate["candidate_id"],
        "transformation_revision": candidate["transformation_revision"],
        "inputs": candidate["inputs"],
        "candidate_snapshots": candidate["snapshots"],
        "captured_at": datetime.now(timezone.utc).isoformat(),
        "status": "candidate",
        "results": results,
    }
    document = json.loads(json.dumps(document, default=str))
    checksum = digest(document)
    key = f"dp2/diagnostics/{candidate['candidate_id']}/{checksum}.json"
    if store.get(key) is None:
        store.put(key, document, create=True)
    return {"key": key, "sha256": checksum}
