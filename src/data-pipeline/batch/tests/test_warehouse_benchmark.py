from datetime import timedelta

import pytest

from batch import warehouse_benchmark
from tests.test_marts import TODAY, attempt, client, rebuild  # noqa: F401


@pytest.fixture
def built(client):  # noqa: F811
    client.execute("DROP DATABASE IF EXISTS marts_benchmark SYNC")
    client.execute("CREATE DATABASE marts_benchmark")
    for n in range(6):
        attempt(client, f"p-{n}", TODAY - timedelta(days=n), merchant=f"m-{n % 3}", customer=f"c-{n % 2}")
    rebuild(client)
    return client


def test_the_report_has_every_query_and_variant(built):
    report = warehouse_benchmark.run(built, "bench")

    assert set(report["results"]) == set(warehouse_benchmark.QUERIES)
    for query, by_variant in report["results"].items():
        assert set(by_variant) == {"A_raw_iceberg", "B_unsorted_mergetree", "C_indexed_mart", "C_primary_key_only"}
        for result in by_variant.values():
            assert (
                result["plan"] and result["cold"] is not None and len(result["warm"]) == warehouse_benchmark.WARM_RUNS
            )
    assert built.value(f"EXISTS TABLE {warehouse_benchmark.SCRATCH}") == 0


def test_a_variant_that_returns_different_rows_fails_the_benchmark(built):
    attempt(built, "late", TODAY - timedelta(days=1), merchant="m-0", customer="c-0")

    with pytest.raises(warehouse_benchmark.VariantsDisagree, match="A_raw_iceberg"):
        warehouse_benchmark.run(built, "bench")
