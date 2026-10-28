import contextlib
import io
import json
import os
import re
import time
from datetime import datetime, timezone
from urllib.parse import urlsplit

from pyspark import StorageLevel
from pyspark.sql import functions as F

from fraud_batch.common.images import immutable_image
from fraud_batch.common.storage import Store
from fraud_batch.silver.silver import canonical_events
from fraud_batch.silver.silver_runs import digest, verify_candidate
from fraud_batch.silver.silver_source import selected_bronze


def explain(frame):
    output = io.StringIO()
    with contextlib.redirect_stdout(output):
        frame.explain(mode="formatted")
    return output.getvalue()


def job_stages(context, group):
    tracker = context.statusTracker()
    jobs = sorted(tracker.getJobIdsForGroup(group))
    stages, unavailable = set(), []
    for job in jobs:
        info = tracker.getJobInfo(job)
        if info is None:
            unavailable.append(job)
        else:
            stages.update(info.stageIds)
    return {"job_ids": jobs, "stage_ids": sorted(stages), "unavailable_job_ids": unavailable}


def safe_location(value):
    parsed = urlsplit(value)
    if parsed.username or parsed.password or parsed.query or parsed.fragment:
        return "redacted"
    return value


def workloads(events, optimized, salt_buckets):
    created = (
        events.where(F.col("payment_created").isNotNull())
        .select(
            "integration_id",
            "event_id",
            F.col("payment_created.merchant.merchant_id").alias("merchant_id"),
            F.explode("payment_created.snapshot.items").alias("item"),
        )
        .select("integration_id", "event_id", "merchant_id", F.col("item.category").alias("category"))
    )
    keys = ["integration_id", "merchant_id", "category"]
    if optimized:
        salted = created.withColumn("bucket", F.pmod(F.xxhash64("event_id"), F.lit(salt_buckets)))
        shares = salted.groupBy(*keys, "bucket").count().groupBy(*keys).agg(F.sum("count").alias("count"))
    else:
        shares = created.groupBy(*keys).count()
    methods = (
        events.where(F.col("attempt_started").isNotNull())
        .select(
            "integration_id",
            F.col("attempt_started.context.customer.customer_id").alias("customer_id"),
            F.col("attempt_started.signals.payment_method.fingerprint").alias("payment_method"),
        )
        .where(F.col("payment_method").isNotNull())
    )
    customers = ["integration_id", "customer_id"]
    cardinality = (
        methods.distinct().groupBy(*customers).count().withColumnRenamed("count", "distinct_methods")
        if optimized
        else methods.groupBy(*customers).agg(F.countDistinct("payment_method").alias("distinct_methods"))
    )
    return {"merchant_category_counts": shares, "customer_payment_methods": cardinality}


def run_variant(spark, events, variant, salt_buckets):
    context = spark.sparkContext
    group = f"dp2-benchmark-{variant}-{time.time_ns()}"
    context.setJobGroup(group, f"Silver {variant} benchmark", interruptOnCancel=True)
    started = time.monotonic()
    cached = {}
    try:
        outputs = workloads(events, variant == "optimized", salt_buckets)
        plans = {name: explain(frame) for name, frame in outputs.items()}
        cached = {name: frame.persist(StorageLevel.DISK_ONLY) for name, frame in outputs.items()}
        counts, timings = {}, {}
        for name, frame in cached.items():
            workload_started = time.monotonic()
            counts[name] = frame.count()
            timings[name] = time.monotonic() - workload_started
        elapsed = time.monotonic() - started
    except Exception:
        for frame in cached.values():
            frame.unpersist()
        raise
    finally:
        context.clearJobGroup()
    return cached, {
        "variant": variant,
        "runtime_seconds": elapsed,
        "row_counts": counts,
        "workload_seconds": timings,
        "plans": plans,
        **job_stages(context, group),
    }


def benchmark(spark, bronze_store, evidence_store, candidate):
    source = selected_bronze(spark, bronze_store, candidate)
    canonical = canonical_events(source["payment_events"])
    if canonical.conflicts.limit(1).count():
        raise ValueError("benchmark input contains conflicting event identities")
    salt_buckets = int(os.environ.get("SILVER_BENCHMARK_SALT_BUCKETS", "8"))
    if salt_buckets < 2:
        raise ValueError("benchmark salt buckets must be at least two")
    partitions = int(spark.conf.get("spark.sql.shuffle.partitions"))
    if partitions < 1:
        raise ValueError("benchmark requires positive shuffle partitions")
    baseline, optimized = None, None
    report = {
        "candidate_id": candidate["candidate_id"],
        "inputs": candidate["inputs"],
        "transformation_revision": candidate["transformation_revision"],
        "contracts": candidate["contracts"],
        "spark_application_id": spark.sparkContext.applicationId,
        "conditions": {
            "shuffle_partitions": partitions,
            "adaptive_enabled": spark.conf.get("spark.sql.adaptive.enabled"),
            "executor_instances": spark.sparkContext.getConf().get("spark.executor.instances", "unavailable"),
            "executor_memory": spark.sparkContext.getConf().get("spark.executor.memory", "unavailable"),
            "driver_memory": spark.sparkContext.getConf().get("spark.driver.memory", "unavailable"),
            "event_log_enabled": spark.sparkContext.getConf().get("spark.eventLog.enabled", "false"),
            "event_log_directory": safe_location(
                spark.sparkContext.getConf().get("spark.eventLog.dir", "unavailable")
            ),
            "cache": "DISK_ONLY per variant and released before the next; filesystem cache not controlled",
            "warmup": "none",
            "repetitions": 1,
            "order": ["baseline", "optimized"],
            "salt_buckets": salt_buckets,
            "workloads": {
                "merchant_category_counts": "direct counts versus event-hash salted partial counts",
                "customer_payment_methods": "countDistinct versus distinct keys followed by count",
            },
            "limitations": "Timings exclude the bidirectional comparison. Collect task durations, shuffle/spill metrics and screenshots manually from Spark UI or the retained event log; repeat trials separately. No improvement is assumed.",
        },
    }
    retained = False
    try:
        baseline, baseline_report = run_variant(spark, canonical.canonical, "baseline", salt_buckets)
        report["baseline"] = baseline_report
        for frame in baseline.values():
            frame.unpersist()
        optimized, optimized_report = run_variant(spark, canonical.canonical, "optimized", salt_buckets)
        report["optimized"] = optimized_report
        equality = {
            name: not baseline[name].exceptAll(optimized[name]).limit(1).count()
            and not optimized[name].exceptAll(baseline[name]).limit(1).count()
            for name in baseline
        }
        report.update(
            captured_at=datetime.now(timezone.utc).isoformat(),
            bidirectional_content_equal=equality,
            outcome="passed" if all(equality.values()) else "failed",
        )
        report = json.loads(json.dumps(report, default=str))
        checksum = digest(report)
        key = f"dp2/benchmarks/{candidate['candidate_id']}/{checksum}.json"
        evidence_store.put(key, report, create=True)
        retained = True
        if not all(equality.values()):
            raise ValueError(f"benchmark outputs differ; retained report: {key}")
        return {"key": key, "sha256": checksum, "spark_application_id": spark.sparkContext.applicationId}
    except Exception as error:
        if not retained:
            report.update(
                captured_at=datetime.now(timezone.utc).isoformat(), outcome="error", error_type=type(error).__name__
            )
            report = json.loads(json.dumps(report, default=str))
            checksum = digest(report)
            evidence_store.put(f"dp2/benchmarks/{candidate['candidate_id']}/{checksum}.json", report, create=True)
        raise
    finally:
        if baseline is not None:
            for frame in baseline.values():
                frame.unpersist()
        if optimized is not None:
            for frame in optimized.values():
                frame.unpersist()


def main():
    bronze_evidence = os.environ["BRONZE_EVIDENCE_ROOT"]
    silver_evidence = os.environ["SILVER_EVIDENCE_ROOT"]
    candidate_id = os.environ["SILVER_CANDIDATE_ID"]
    image = immutable_image(os.environ["IMAGE_REFERENCE"])
    completeness = os.environ.get("SILVER_COMPLETENESS")
    if completeness is not None and completeness not in ("complete", "recurring"):
        raise ValueError("SILVER_COMPLETENESS must be complete or recurring")
    if not re.fullmatch(r"[a-f0-9]{64}", candidate_id):
        raise ValueError("invalid candidate identity")
    store = Store(silver_evidence)
    candidate = store.get(f"dp2/candidates/{candidate_id}.json")
    if (
        not candidate
        or candidate.get("transformation_revision") != image
        or (completeness is not None and candidate.get("completeness") != completeness)
    ):
        raise ValueError("benchmark requires the original candidate image and pinned inputs")
    verify_candidate(candidate)
    from fraud_batch.common.spark import session

    spark = session()
    try:
        result = benchmark(spark, Store(bronze_evidence), store, candidate)
        candidate["benchmark"] = result
        store.put(f"dp2/candidates/{candidate['candidate_id']}.json", candidate)
    finally:
        spark.stop()
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
