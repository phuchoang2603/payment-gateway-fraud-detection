import json

import pytest
from pyspark.sql import functions as F

from batch.silver import benchmark
from tests.events import at

LOW_SKEW_THRESHOLDS = {
    "spark.sql.adaptive.coalescePartitions.enabled": "false",
    "spark.sql.adaptive.advisoryPartitionSizeInBytes": "256",
    "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes": "1",
    "spark.sql.adaptive.skewJoin.skewedPartitionFactor": "1.5",
}


def attempt_rows():
    hot = [("m-1", f"c-{n % 9}", "pm-1", f"d-{n % 4}", f"hot-{n}", 100 + n, at(n)) for n in range(80)]
    cold = [("m-2", "c-1", "pm-2", "d-9", "cold-1", 500, at(1)), ("m-3", "c-2", "pm-3", None, "cold-2", 700, at(2))]
    return hot + cold


COLUMNS = (
    "merchant_id string, customer_id string, payment_method_fingerprint string, device_fingerprint string, "
    "attempt_id string, amount_minor bigint, started_at timestamp"
)


@pytest.fixture
def attempts(spark):
    return spark.createDataFrame(attempt_rows(), COLUMNS)


def guard(frame):
    rows, checksum, _ = benchmark.fingerprint(frame)
    return rows, checksum


@pytest.fixture
def skewed_attempts(spark):
    hot = [("m-1", f"c-{n % 9}", "pm-1", f"d-{n % 4}", f"hot-{n}", 100 + n, at(n % 60)) for n in range(2000)]
    cold = [("m-2", "c-1", "pm-2", "d-9", "cold-1", 500, at(1)), ("m-3", "c-2", "pm-3", None, "cold-2", 700, at(2))]
    return spark.createDataFrame(hot + cold, COLUMNS)


def test_every_skewed_join_variant_returns_the_baseline_output(spark, skewed_attempts):
    results = benchmark.skewed_merchant_join(spark, skewed_attempts, {}, aqe_thresholds=LOW_SKEW_THRESHOLDS)

    assert set(results) == set(benchmark.JOIN_VARIANTS)
    assert len({(r["output_rows"], r["checksum"]) for r in results.values()}) == 1


def test_aqe_skew_join_applies_on_a_skewed_fixture(spark, skewed_attempts):
    settings = {**benchmark.JOIN_VARIANTS["aqe_skew_join"][1], **LOW_SKEW_THRESHOLDS}

    totals = benchmark.collected_totals(skewed_attempts)
    _, _, skew = benchmark.run_variant(
        spark, skewed_attempts, totals, "aqe_skew_join", benchmark.join_baseline, settings, {}
    )

    assert skew["skew_join"] or skew["skewed_shuffle_read"], skew["plan"]


def test_aqe_variant_that_does_not_skew_fails_the_run(spark, attempts):
    with pytest.raises(AssertionError, match="no skew-split join"):
        benchmark.skewed_merchant_join(spark, attempts, {})


def test_unequal_join_variant_fails_the_equal_output_guard(spark, attempts):
    baseline = guard(benchmark.join_baseline(attempts, benchmark.merchant_totals(attempts)))
    truncated = guard(benchmark.join_baseline(attempts, benchmark.merchant_totals(attempts)).limit(1))

    with pytest.raises(AssertionError, match="differs"):
        benchmark.same_output(baseline, truncated, "truncated")


def test_same_row_count_with_a_changed_value_fails_the_equal_output_guard(spark, attempts):
    baseline = guard(benchmark.join_baseline(attempts, benchmark.merchant_totals(attempts)))
    changed = guard(
        benchmark.join_baseline(attempts, benchmark.merchant_totals(attempts)).withColumn(
            "amount_minor", F.col("amount_minor") + 1
        )
    )

    with pytest.raises(AssertionError, match="differs"):
        benchmark.same_output(baseline, changed, "changed")


def test_salted_join_matches_baseline_for_the_hot_merchant(spark, attempts):
    benchmark.same_output(
        guard(benchmark.join_baseline(attempts, benchmark.merchant_totals(attempts))),
        guard(benchmark.join_salted(attempts, benchmark.merchant_totals(attempts))),
        "salted",
    )


def test_hot_key_share_uses_all_attempts_as_denominator(spark, attempts):
    share = benchmark.hot_key_share(attempts)

    assert share["merchant_id"] == "m-1"
    assert share["denominator"] == 82


def test_workflow_entrypoint_keeps_salt_identity_and_pins_the_report(spark, attempts, report_root, monkeypatch):
    monkeypatch.setenv("RUN_ID", "skew-entrypoint")
    monkeypatch.setenv("DP_START", "2026-07-31")
    monkeypatch.setenv("DP_END", "2026-07-31")
    monkeypatch.setattr(benchmark, "session", lambda _: spark)
    monkeypatch.setattr(spark, "stop", lambda: None)
    monkeypatch.setattr(benchmark, "ref_snapshot", lambda *_: 123)
    monkeypatch.setattr(benchmark, "read_at", lambda *_: attempts.withColumn("payment_day", F.to_date("started_at")))

    def compare(session, selected):
        totals = benchmark.collected_totals(selected)
        baseline = guard(benchmark.join_baseline(selected, totals))
        salted = guard(benchmark.join_salted(selected, totals))
        benchmark.same_output(baseline, salted, "salted")
        selected.unpersist()
        return {"equal_output_rows": baseline[0]}

    monkeypatch.setattr(benchmark, "benchmark", compare)

    assert benchmark.main() == 0
    report = json.loads((report_root / "skew-entrypoint" / "benchmark.json").read_text())
    assert report["silver_snapshot_id"] == 123
    assert report["attempts"] == 82
    assert report["equal_output_rows"] == 3


def test_join_failure_restores_an_unset_adaptive_broadcast_threshold(spark, attempts):
    key = "spark.sql.adaptive.autoBroadcastJoinThreshold"
    previous = spark.conf.get(key, None)
    spark.conf.unset(key)

    def broken(*_):
        assert spark.conf.get(key) == "-1"
        raise RuntimeError("join failed")

    try:
        with pytest.raises(RuntimeError, match="join failed"):
            benchmark.run_variant(spark, attempts, attempts, "broken", broken, benchmark.SORT_MERGE, {})
        assert spark.conf.get(key, None) is None
    finally:
        if previous is not None:
            spark.conf.set(key, previous)
