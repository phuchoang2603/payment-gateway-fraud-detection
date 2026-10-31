from datetime import date, datetime, timedelta, timezone

from pyspark.sql import functions as F

from batch import maintenance
from batch.bronze.schema import EVENT_COLUMNS, business_schema, ddl
from tests.events import created

PARTITIONED = "lake.maintenance_tests.events_partitioned"
UNPARTITIONED = "lake.maintenance_tests.events_unpartitioned"


def test_partition_filter_plans_fewer_files_than_an_unpartitioned_copy(spark):
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.maintenance_tests")
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

    day_rows = spark.table(PARTITIONED).where(F.to_date("occurred_at") == F.lit("2026-08-01")).count()
    assert day_rows == 2

    expression = maintenance.day_filter(spark, "occurred_at", date(2026, 8, 1))
    partitioned = maintenance.planned(spark, PARTITIONED, expression)
    unpartitioned = maintenance.planned(spark, UNPARTITIONED, expression)
    assert unpartitioned["files_planned"] == 2
    assert partitioned["files_planned"] < unpartitioned["files_planned"]


def test_customer_window_builds_one_iceberg_expression_from_three_predicates(spark):
    expression = maintenance.customer_window(spark, "c-1", date(2026, 8, 1), date(2026, 8, 31))

    assert "customer_id" in expression.toString()
    assert "payment_day" in expression.toString()


def test_nightly_drops_only_audit_branches_older_than_the_retention(spark):
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.maintenance_tests")
    spark.sql(f"DROP TABLE IF EXISTS {PARTITIONED} PURGE")
    spark.sql(f"CREATE TABLE {PARTITIONED} ({ddl(EVENT_COLUMNS)}) USING iceberg PARTITIONED BY (days(occurred_at))")
    spark.createDataFrame([created("pay-1", "evt-1")], business_schema(spark, EVENT_COLUMNS)).writeTo(
        PARTITIONED
    ).append()
    for branch in ("audit-old-run", "keep-me"):
        spark.sql(f"ALTER TABLE {PARTITIONED} CREATE BRANCH `{branch}`")
    later = datetime.now(timezone.utc) + timedelta(days=30)

    assert maintenance.stale_branches(spark, PARTITIONED, later) == ["audit-old-run"]
    assert maintenance.stale_branches(spark, PARTITIONED, later - timedelta(days=60)) == []
