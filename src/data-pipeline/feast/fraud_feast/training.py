from datetime import date, datetime


def entity_rows(spark, start, end, cutoff):
    from pyspark.sql import functions as F

    first, last = date.fromisoformat(str(start)), date.fromisoformat(str(end))
    if first >= last:
        raise ValueError("training start must precede end")
    cutoff = (
        datetime.fromisoformat(cutoff.replace("Z", "+00:00")) if isinstance(cutoff, str) else cutoff
    )
    if cutoff.tzinfo is None:
        raise ValueError("training cutoff must include a UTC offset")
    keys = ["integration_id", "payment_id", "attempt_id"]
    attempts = (
        spark.table("lake.silver.attempts")
        .where(
            F.col("lifecycle_valid")
            & (F.col("started_at") >= F.lit(first))
            & (F.col("started_at") < F.lit(last))
        )
        .select(*keys, "started_at")
    )
    labels = (
        spark.table("lake.gold.label_attempt")
        .where(
            (F.col("label_source") == "LABEL_SOURCE_SIMULATION_TRUTH")
            & (F.col("label_available_at") <= F.lit(cutoff))
        )
        .drop("event_timestamp")
    )
    body = "attempt_started"
    context = f"{body}.context.snapshot"
    starts = (
        spark.table("lake.silver.payment_events")
        .where(F.col("payload_case") == "attempt_started")
        .select(
            "integration_id",
            "payment_id",
            F.col(f"{body}.attempt_id").alias("attempt_id"),
            F.col(f"{context}.total.minor_units").alias("amount_minor"),
            F.col(f"{context}.customer_account_created_at").alias("customer_account_created_at"),
            F.col(f"{context}.merchant_account_created_at").alias("merchant_account_created_at"),
            F.col(f"{context}.shipping.country").alias("shipping_country"),
            F.col(f"{body}.signals.ip_country").alias("ip_country"),
            F.size(f"{context}.items").cast("long").alias("item_count"),
            F.col(f"{body}.signals.device_fingerprint").isNotNull().alias("has_device"),
        )
    )
    return (
        labels.join(attempts, keys)
        .join(starts, keys)
        .withColumn("scored_at", F.col("started_at"))
        .withColumn("event_timestamp", F.col("started_at"))
    )
