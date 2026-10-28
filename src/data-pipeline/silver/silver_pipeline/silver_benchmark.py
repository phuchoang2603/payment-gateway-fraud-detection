import argparse
import contextlib
import io
import json
import os
import re
import time
from datetime import datetime, timezone
from urllib.parse import urlsplit

from pipeline_common.storage import Store
from pyspark import StorageLevel

from silver_pipeline.silver_runs import digest, verify_candidate
from silver_pipeline.silver_source import selected_bronze
from silver_pipeline.silver_transform import build, validate_basics
from silver_pipeline.silver_write import candidate_outputs


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


def run_variant(spark, source, completeness, variant, partitions, relative_error):
    context = spark.sparkContext
    group = f"dp2-benchmark-{variant}-{time.time_ns()}"
    context.setJobGroup(group, f"Silver {variant} benchmark", interruptOnCancel=True)
    started = time.monotonic()
    cached = {}
    try:
        if variant == "partitioned":
            source = {
                "payment_events": source["payment_events"].repartition(partitions, "integration_id", "event_id"),
                "fraud_labels": source["fraud_labels"].repartition(
                    partitions, "integration_id", "payment_id", "attempt_id"
                ),
            }
        frames = build(source, relative_error)
        validate_basics(frames, completeness)
        outputs = candidate_outputs(frames)
        plans = {name: explain(frame) for name, frame in outputs.items()}
        cached = {name: frame.persist(StorageLevel.DISK_ONLY) for name, frame in outputs.items()}
        counts = {name: frame.count() for name, frame in cached.items()}
        elapsed = time.monotonic() - started
    except Exception:
        for frame in cached.values():
            frame.unpersist()
        raise
    finally:
        context.clearJobGroup()
    jobs = job_stages(context, group)
    return cached, {
        "variant": variant,
        "runtime_seconds": elapsed,
        "row_counts": counts,
        "plans": plans,
        **jobs,
    }


def benchmark(spark, bronze_store, evidence_store, candidate, relative_error=0.05):
    source = selected_bronze(spark, bronze_store, candidate)
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
            "order": ["baseline", "partitioned"],
            "relative_error": relative_error,
            "limitations": "Timings exclude the bidirectional comparison. Collect task durations, shuffle/spill metrics and screenshots manually from Spark UI or the retained event log; repeat trials separately. No improvement is assumed.",
        },
    }
    retained = False
    try:
        baseline, baseline_report = run_variant(
            spark, source, candidate["completeness"], "baseline", partitions, relative_error
        )
        report["baseline"] = baseline_report
        for frame in baseline.values():
            frame.unpersist()
        optimized, optimized_report = run_variant(
            spark, source, candidate["completeness"], "partitioned", partitions, relative_error
        )
        report["partitioned"] = optimized_report
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


def main(argv=None):
    parser = argparse.ArgumentParser(prog="dp2-benchmark")
    parser.add_argument(
        "--bronze-evidence",
        default=os.environ.get("BRONZE_EVIDENCE_ROOT"),
        required=not os.environ.get("BRONZE_EVIDENCE_ROOT"),
    )
    parser.add_argument(
        "--silver-evidence",
        default=os.environ.get("SILVER_EVIDENCE_ROOT"),
        required=not os.environ.get("SILVER_EVIDENCE_ROOT"),
    )
    parser.add_argument(
        "--candidate-id",
        default=os.environ.get("SILVER_CANDIDATE_ID"),
        required=not os.environ.get("SILVER_CANDIDATE_ID"),
    )
    parser.add_argument(
        "--image-digest", default=os.environ.get("IMAGE_DIGEST"), required=not os.environ.get("IMAGE_DIGEST")
    )
    parser.add_argument("--relative-error", type=float, default=0.05)
    parser.add_argument("--completeness", choices=("complete", "recurring"))
    args = parser.parse_args(argv)
    if not re.fullmatch(r"[a-f0-9]{64}", args.candidate_id):
        parser.error("invalid candidate identity")
    store = Store(args.silver_evidence)
    candidate = store.get(f"dp2/candidates/{args.candidate_id}.json")
    if (
        not candidate
        or candidate.get("transformation_revision") != args.image_digest
        or (args.completeness is not None and candidate.get("completeness") != args.completeness)
    ):
        raise ValueError("benchmark requires the original candidate image and pinned inputs")
    verify_candidate(candidate)
    from pipeline_common.spark import session

    spark = session()
    try:
        result = benchmark(spark, Store(args.bronze_evidence), store, candidate, args.relative_error)
        candidate["benchmark"] = result
        store.put(f"dp2/candidates/{candidate['candidate_id']}.json", candidate)
    finally:
        spark.stop()
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
