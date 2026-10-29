import json
import os

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from batch.bronze import current_snapshot
from batch.metrics import phase
from batch.silver import ATTEMPTS
from batch.spark import session
from batch.store import Store

SALT_BUCKETS = 16
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
    "aqe_skew_join": (
        join_baseline,
        {
            **SORT_MERGE,
            "spark.sql.adaptive.skewJoin.enabled": "true",
            "spark.sql.adaptive.forceOptimizeSkewedJoin": "true",
        },
    ),
    "salted": (join_salted, {**SORT_MERGE, "spark.sql.adaptive.skewJoin.enabled": "false"}),
}


def fingerprint(frame: DataFrame):
    """Row count, an order-independent checksum, and the executed plan of the run."""
    digest = frame.select(F.xxhash64(*frame.columns).cast("decimal(38,0)").alias("hash")).agg(
        F.count("*").alias("rows"), F.sum("hash").alias("checksum")
    )
    row = digest.first()
    return row["rows"], row["checksum"], digest._jdf.queryExecution().executedPlan()


def skew_report(spark, plan) -> dict:
    """Whether the final adaptive plan contains a skew-split sort-merge join or a skewed shuffle read."""
    joins, reads = [], []
    stack = [plan]
    while stack:
        node = stack.pop()
        name = node.getClass().getSimpleName()
        if name == "AdaptiveSparkPlanExec":
            stack.append(node.executedPlan())
            continue
        if name.endswith("QueryStageExec"):
            stack.append(node.plan())
            continue
        if name == "ReusedExchangeExec":
            stack.append(node.child())
            continue
        if name == "SortMergeJoinExec":
            joins.append(bool(node.isSkewJoin()))
        if name == "AQEShuffleReadExec":
            reads.append(bool(node.hasSkewedPartition()))
        children = spark._jvm.scala.collection.JavaConverters.seqAsJavaList(node.children())
        for index in range(children.size()):
            stack.append(children.get(index))
    return {"skew_join": any(joins), "skewed_shuffle_read": any(reads), "plan": str(plan)[:4000]}


def same_output(baseline, candidate, name: str) -> None:
    if baseline != candidate:
        raise AssertionError(f"variant {name} output differs from baseline: {candidate} != {baseline}")


def run_variant(spark, attempts: DataFrame, name: str, build, settings: dict, phases: dict):
    previous = {key: spark.conf.get(key) for key in settings}
    for key, value in settings.items():
        spark.conf.set(key, value)
    try:
        frame = build(attempts)
        with phase(spark, phases, f"skewed_merchant_join {name}"):
            rows, checksum, plan = fingerprint(frame)
        return rows, checksum, skew_report(spark, plan)
    finally:
        for key, value in previous.items():
            spark.conf.set(key, value)


def skewed_merchant_join(spark, attempts: DataFrame, phases: dict, aqe_thresholds: dict | None = None) -> dict:
    attempts.count()
    results, baseline = {}, None
    for name, (build, settings) in JOIN_VARIANTS.items():
        if name == "aqe_skew_join":
            settings = {**settings, **(aqe_thresholds or {})}
        rows, checksum, skew = run_variant(spark, attempts, name, build, settings, phases)
        if name == "aqe_skew_join" and not (skew["skew_join"] or skew["skewed_shuffle_read"]):
            raise AssertionError(
                "aqe_skew_join: the final adaptive plan has no skew-split join or skewed shuffle read\n" + skew["plan"]
            )
        guard = (rows, checksum)
        if baseline is None:
            baseline = guard
        same_output(baseline, guard, name)
        results[name] = {"output_rows": rows, "checksum": str(checksum), "skew": skew}
    return results


def hot_key_share(attempts: DataFrame) -> dict:
    totals = attempts.groupBy("merchant_id").count()
    top = totals.orderBy(F.desc("count")).first()
    total = attempts.count()
    return {"merchant_id": top["merchant_id"], "share": top["count"] / total, "denominator": total}


def benchmark(spark, attempts: DataFrame) -> dict:
    phases: dict = {}
    return {
        "hot_key": hot_key_share(attempts),
        "skewed_merchant_join": skewed_merchant_join(spark, attempts, phases),
        "phases": phases,
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
