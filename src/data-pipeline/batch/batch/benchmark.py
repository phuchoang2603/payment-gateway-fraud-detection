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


def merchant_daily_baseline(attempts: DataFrame) -> DataFrame:
    return attempts.groupBy("merchant_id", F.to_date("started_at").alias("day")).agg(
        F.count("*").alias("attempts"), F.sum("total_minor_units").alias("amount_minor_units")
    )


def merchant_daily_optimized(attempts: DataFrame) -> DataFrame:
    salted = attempts.withColumn("salt", F.abs(F.hash("attempt_id")) % SALT_BUCKETS).withColumn(
        "day", F.to_date("started_at")
    )
    partial = salted.groupBy("merchant_id", "day", "salt").agg(
        F.count("*").alias("attempts"), F.sum("total_minor_units").alias("amount_minor_units")
    )
    return partial.groupBy("merchant_id", "day").agg(
        F.sum("attempts").alias("attempts"), F.sum("amount_minor_units").alias("amount_minor_units")
    )


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


AGGREGATIONS = {
    "skewed_merchant_daily": (merchant_daily_baseline, merchant_daily_optimized),
    "distinct_devices_daily": (distinct_devices_baseline, distinct_devices_optimized),
}


def same_output(baseline: DataFrame, candidate: DataFrame) -> None:
    differences = baseline.exceptAll(candidate).count() + candidate.exceptAll(baseline).count()
    if differences:
        raise AssertionError(f"variant output differs from baseline in {differences} rows")


def median_seconds(variant, attempts: DataFrame) -> float:
    variant(attempts).count()
    timings = []
    for _ in range(TIMED_RUNS):
        started = time.perf_counter()
        variant(attempts).count()
        timings.append(time.perf_counter() - started)
    return statistics.median(timings)


def benchmark(attempts: DataFrame):
    results = {}
    for name, (baseline, optimized) in AGGREGATIONS.items():
        same_output(baseline(attempts), optimized(attempts))
        results[name] = {
            "baseline_median_seconds": median_seconds(baseline, attempts),
            "optimized_median_seconds": median_seconds(optimized, attempts),
            "timed_runs": TIMED_RUNS,
            "output_rows": baseline(attempts).count(),
        }
    return results


def main():
    run_id = os.environ["RUN_ID"]
    spark = session("dp2-benchmark")
    try:
        snapshot = current_snapshot(spark, ATTEMPTS)
        attempts = spark.read.option("snapshot-id", snapshot).table(ATTEMPTS)
        result = {"run_id": run_id, "silver_snapshot_id": snapshot, "aggregations": benchmark(attempts)}
        Store(os.environ["RUN_REPORT_ROOT"]).write_json(f"{run_id}/benchmark.json", result)
        print(json.dumps(result, sort_keys=True))
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
