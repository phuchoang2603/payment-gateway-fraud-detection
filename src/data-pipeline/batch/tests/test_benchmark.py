import pytest

from batch import benchmark
from tests.events import at

ROWS = [
    ("m-1", "c-1", "pm-1", "d-1", "att-1", 100, at(0)),
    ("m-1", "c-1", "pm-1", "d-2", "att-2", 200, at(1)),
    ("m-1", "c-2", "pm-2", None, "att-3", 300, at(2)),
    ("m-2", "c-1", "pm-1", "d-1", "att-4", 400, at(3)),
]
COLUMNS = "merchant_id string, customer_id string, payment_method_fingerprint string, device_fingerprint string, attempt_id string, amount_minor bigint, started_at timestamp"


@pytest.fixture
def attempts(spark):
    return spark.createDataFrame(ROWS, COLUMNS)


@pytest.mark.parametrize("name", list(benchmark.AGGREGATIONS))
def test_unequal_variant_fails_the_equal_output_guard(attempts, name):
    baseline, optimized = benchmark.AGGREGATIONS[name]
    benchmark.same_output(baseline(attempts), optimized(attempts))

    with pytest.raises(AssertionError, match="differs from baseline"):
        benchmark.same_output(baseline(attempts), optimized(attempts).limit(1))


def test_salted_merchant_aggregation_matches_baseline_when_attempts_share_a_merchant(attempts):
    benchmark.same_output(benchmark.merchant_daily_baseline(attempts), benchmark.merchant_daily_optimized(attempts))
