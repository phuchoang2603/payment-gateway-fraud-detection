import pytest

from batch import benchmark
from tests.events import at


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


def test_every_skewed_join_variant_returns_the_baseline_output(spark, attempts):
    results = benchmark.run_family(spark, benchmark.JOIN_VARIANTS, attempts)

    assert set(results) == set(benchmark.JOIN_VARIANTS)
    assert {r["output_rows"] for r in results.values()} == {results["baseline_no_skew_join"]["output_rows"]}


def test_unequal_join_variant_fails_the_equal_output_guard(spark, attempts):
    baseline = benchmark.join_baseline(attempts)
    truncated = benchmark.join_baseline(attempts).limit(1)

    with pytest.raises(AssertionError, match="differs"):
        benchmark.same_output(baseline, truncated)


def test_unequal_distinct_device_variant_fails_the_equal_output_guard(spark, attempts):
    baseline = benchmark.distinct_devices_baseline(attempts)
    candidate = benchmark.distinct_devices_optimized(attempts).where("attempts > 1")

    with pytest.raises(AssertionError, match="differs"):
        benchmark.same_output(baseline, candidate)


def test_salted_join_matches_baseline_for_the_hot_merchant(spark, attempts):
    benchmark.same_output(benchmark.join_baseline(attempts), benchmark.join_salted(attempts))


def test_hot_key_share_uses_all_attempts_as_denominator(spark, attempts):
    share = benchmark.hot_key_share(attempts)

    assert share["merchant_id"] == "m-1"
    assert share["denominator"] == 82
