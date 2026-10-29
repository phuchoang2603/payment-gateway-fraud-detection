from datetime import date

import pytest
from pyspark.sql import functions as F

from batch import benchmark, storage_layout
from batch.schema import EVENT_COLUMNS, business_schema, ddl
from tests.events import created

PARTITIONED = "lake.benchmark_tests.events_partitioned"
UNPARTITIONED = "lake.benchmark_tests.events_unpartitioned"


def test_unequal_layout_output_fails_the_equal_output_guard(spark):
    expected = spark.createDataFrame([(1,), (2,)], "n int")
    benchmark.same_output(expected, expected)

    with pytest.raises(AssertionError, match="differs"):
        benchmark.same_output(expected, spark.createDataFrame([(1,), (3,)], "n int"))


def test_partition_filter_plans_fewer_files_than_an_unpartitioned_copy(spark):
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.benchmark_tests")
    for table in (PARTITIONED, UNPARTITIONED):
        spark.sql(f"DROP TABLE IF EXISTS {table} PURGE")
    spark.sql(f"CREATE TABLE {PARTITIONED} ({ddl(EVENT_COLUMNS)}) USING iceberg PARTITIONED BY (days(occurred_at))")
    spark.sql(f"CREATE TABLE {UNPARTITIONED} ({ddl(EVENT_COLUMNS)}) USING iceberg")
    schema = business_schema(spark, EVENT_COLUMNS)
    batches = [[created(f"pay-{day}", f"evt-{day}-{n}", minutes=day * 1440 + n) for day in range(3)] for n in range(2)]
    for day in range(3):
        rows_of_day = [batch[day] for batch in batches]
        spark.createDataFrame(rows_of_day, schema).coalesce(1).writeTo(PARTITIONED).append()
    for batch in batches:
        spark.createDataFrame(batch, schema).coalesce(1).writeTo(UNPARTITIONED).append()

    def one_day(table):
        return (
            spark.table(table)
            .where(F.col("occurred_at") >= F.lit("2026-08-01 00:00:00").cast("timestamp"))
            .where(F.col("occurred_at") < F.lit("2026-08-02 00:00:00").cast("timestamp"))
            .agg(F.count("*").alias("rows"))
        )

    benchmark.same_output(one_day(PARTITIONED), one_day(UNPARTITIONED))
    assert one_day(PARTITIONED).first()["rows"] == 2

    expression = storage_layout.day_filter(spark, "occurred_at", date(2026, 8, 1))
    partitioned = storage_layout.planned(spark, PARTITIONED, expression)
    unpartitioned = storage_layout.planned(spark, UNPARTITIONED, expression)
    assert unpartitioned["files_planned"] == 2
    assert partitioned["files_planned"] < unpartitioned["files_planned"]
