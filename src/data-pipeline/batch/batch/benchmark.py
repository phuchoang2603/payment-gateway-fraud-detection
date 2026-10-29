import json
import os
import statistics
import time

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from batch.bronze import current_snapshot
from batch.silver import ATTEMPTS
from batch.spark import session
from batch.store import Store

SALT_BUCKETS = 16
TIMED_RUNS = 3
SORT_MERGE = {
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.autoBroadcastJoinThreshold": "-1",
}


def merchant_totals(attempts: DataFrame) -> DataFrame:
    return attempts.groupBy("merchant_id").agg(
        F.count("*").alias("merchant_attempts"),
        F.sum("amount_minor").alias("merchant_amount"),
    )


def per_merchant_day(joined: DataFrame) -> DataFrame:
    return joined.groupBy("merchant_id", F.to_date("started_at").alias("day")).agg(
        F.count("*").alias("attempts"),
        F.sum("amount_minor").alias("amount_minor"),
        F.max("merchant_attempts").alias("merchant_attempts"),
    )


def join_baseline(attempts: DataFrame) -> DataFrame:
    return per_merchant_day(attempts.join(merchant_totals(attempts), "merchant_id"))


def join_salted(attempts: DataFrame) -> DataFrame:
    """Each hot merchant's rows spread over SALT_BUCKETS join keys; the totals are replicated to every bucket."""
    salted = attempts.withColumn("salt", F.pmod(F.hash("attempt_id"), F.lit(SALT_BUCKETS)))
    buckets = F.sequence(F.lit(0), F.lit(SALT_BUCKETS - 1))
    totals = merchant_totals(attempts).withColumn("salt", F.explode(buckets))
    joined = salted.join(totals, ["merchant_id", "salt"])
    return per_merchant_day(joined)


JOIN_VARIANTS = {
    "baseline_no_skew_join": (join_baseline, {**SORT_MERGE, "spark.sql.adaptive.skewJoin.enabled": "false"}),
    "aqe_skew_join": (join_baseline, {**SORT_MERGE, "spark.sql.adaptive.skewJoin.enabled": "true"}),
    "salted": (join_salted, {**SORT_MERGE, "spark.sql.adaptive.skewJoin.enabled": "false"}),
}


def distinct_devices_baseline(attempts: DataFrame) -> DataFrame:
    day = F.to_date("started_at")
    return attempts.groupBy("customer_id", "payment_method_fingerprint", day.alias("day")).agg(
        F.countDistinct("device_fingerprint").alias("devices"), F.count("*").alias("attempts")
    )


def distinct_devices_optimized(attempts: DataFrame) -> DataFrame:
    per_device = attempts.groupBy(
        "customer_id", "payment_method_fingerprint", F.to_date("started_at").alias("day"), "device_fingerprint"
    ).agg(F.count("*").alias("device_attempts"))
    return per_device.groupBy("customer_id", "payment_method_fingerprint", "day").agg(
        F.count("device_fingerprint").alias("devices"), F.sum("device_attempts").alias("attempts")
    )


DISTINCT_VARIANTS = {
    "baseline": (distinct_devices_baseline, {}),
    "per_device_first": (distinct_devices_optimized, {}),
}


def same_output(baseline: DataFrame, candidate: DataFrame) -> None:
    differences = baseline.exceptAll(candidate).count() + candidate.exceptAll(baseline).count()
    if differences:
        raise AssertionError(f"variant output differs from baseline in {differences} rows")


def measure(spark, build, settings: dict, attempts: DataFrame) -> dict:
    previous = {key: spark.conf.get(key) for key in settings}
    for key, value in settings.items():
        spark.conf.set(key, value)
    try:
        frame = build(attempts)
        frame.count()
        times = []
        for _ in range(TIMED_RUNS):
            started = time.perf_counter()
            frame.count()
            times.append(time.perf_counter() - started)
        return {
            "median_seconds": statistics.median(times),
            "timed_runs": TIMED_RUNS,
            "output_rows": frame.count(),
        }, frame
    finally:
        for key, value in previous.items():
            spark.conf.set(key, value)


def run_family(spark, variants: dict, attempts: DataFrame):
    results, frames = {}, {}
    for name, (build, settings) in variants.items():
        results[name], frames[name] = measure(spark, build, settings, attempts)
    baseline_name = next(iter(variants))
    for name, frame in frames.items():
        if name != baseline_name:
            same_output(frames[baseline_name], frame)
    return results


def hot_key_share(attempts: DataFrame) -> dict:
    totals = attempts.groupBy("merchant_id").count()
    top = totals.orderBy(F.desc("count")).first()
    total = attempts.count()
    return {"merchant_id": top["merchant_id"], "share": top["count"] / total, "denominator": total}


def benchmark(spark, attempts: DataFrame) -> dict:
    return {
        "hot_key": hot_key_share(attempts),
        "skewed_merchant_join": run_family(spark, JOIN_VARIANTS, attempts),
        "distinct_devices_daily": run_family(spark, DISTINCT_VARIANTS, attempts),
    }


def main():
    run_id = os.environ["RUN_ID"]
    spark = session("dp2-benchmark")
    try:
        snapshot = current_snapshot(spark, ATTEMPTS)
        attempts = spark.read.option("versionAsOf", snapshot).table(ATTEMPTS)
        result = {"run_id": run_id, "silver_snapshot_id": snapshot, **benchmark(spark, attempts)}
        Store(os.environ["RUN_REPORT_ROOT"]).write_json(f"{run_id}/benchmark.json", result)
        print(json.dumps(result, sort_keys=True, default=str))
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
