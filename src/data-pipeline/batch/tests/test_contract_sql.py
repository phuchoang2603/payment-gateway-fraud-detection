import base64
import re
import urllib.error
import urllib.request
from pathlib import Path

import pyspark.sql.types as T
import yaml

from batch.gold import features as gold
from batch.gold import marts
from batch.silver import curate as silver
from batch.spark import ICEBERG_CATALOG
from tests.events import BASE, approved, created, label, started, succeeded
from tests.lake import load_bronze

CONTRACTS = Path(__file__).resolve().parents[4] / "contracts" / "data-products"
PRODUCTS = ("bronze-payments", "silver-payments", "gold-features")
MART_PRODUCTS = ("payment-marts",)
DATABASE = "lakehouse"
AUTH = "Basic " + base64.b64encode(b"test:test").decode()
PRIMITIVES = {
    T.StringType: "String",
    T.LongType: "Int64",
    T.IntegerType: "Int32",
    T.ShortType: "Int16",
    T.ByteType: "Int8",
    T.BooleanType: "Bool",
    T.DoubleType: "Float64",
    T.FloatType: "Float32",
    T.DateType: "Date",
    T.TimestampType: "DateTime64(6, 'UTC')",
    T.BinaryType: "String",
}


def clickhouse_type(data_type):
    if isinstance(data_type, T.StructType):
        fields = ", ".join(f"`{field.name}` {clickhouse_type(field.dataType)}" for field in data_type.fields)
        return f"Tuple({fields})"
    if isinstance(data_type, T.ArrayType):
        return f"Array({clickhouse_type(data_type.elementType)})"
    if isinstance(data_type, T.DecimalType):
        return f"Nullable(Decimal({data_type.precision}, {data_type.scale}))"
    for spark_type, name in PRIMITIVES.items():
        if isinstance(data_type, spark_type):
            return f"Nullable({name})"
    raise TypeError(f"no ClickHouse type for {data_type}")


def create_table(physical, schema):
    columns = ", ".join(f"`{field.name}` {clickhouse_type(field.dataType)}" for field in schema.fields)
    return f"CREATE TABLE {DATABASE}.`{physical}` ({columns}) ENGINE = Memory"


def run_sql(clickhouse, sql):
    url = f"http://{clickhouse.get_container_host_ip()}:{clickhouse.get_exposed_port(8123)}/"
    try:
        request = urllib.request.Request(url, data=sql.encode(), method="POST", headers={"Authorization": AUTH})
        with urllib.request.urlopen(request, timeout=120):
            return None
    except urllib.error.HTTPError as error:
        return error.read().decode(errors="replace").strip().splitlines()[0]


def contract_tables(product):
    for contract in sorted((CONTRACTS / product).glob("*.odcs.yaml")):
        yield from yaml.safe_load(contract.read_text())["schema"]


def test_every_lakehouse_and_mart_rule_analyzes_on_the_pinned_clickhouse(spark, clickhouse, tmp_path, monkeypatch):
    monkeypatch.setenv("RUN_REPORT_ROOT", str(tmp_path / "reports"))
    load_bronze(
        spark,
        [
            created("pay-1", "evt-1"),
            started("pay-1", "evt-2", 2, 1, "att-1"),
            approved("pay-1", "evt-3", 3, 2, "att-1"),
            succeeded("pay-1", "evt-4", 4, 3, "att-1"),
        ],
        [label("pay-1", "att-1", available=10)],
    )
    silver.run(spark, "contract-sql")
    day = BASE.date()
    gold.run(spark, "contract-sql", day, day)
    for database in (DATABASE, marts.DATABASE):
        assert run_sql(clickhouse, f"DROP DATABASE IF EXISTS {database} SYNC") is None
        assert run_sql(clickhouse, f"CREATE DATABASE {database}") is None
    for table in marts.LIVE:
        assert run_sql(clickhouse, marts.ddl(table, staging=False, retention_days=180)) is None

    rules = []
    for product in PRODUCTS:
        for table in contract_tables(product):
            physical = table["physicalName"]
            quality = [rule for rule in table.get("quality", []) if rule.get("type") == "sql"]
            if not quality:
                continue
            schema = spark.table(f"{ICEBERG_CATALOG}.{physical}").schema
            error = run_sql(clickhouse, create_table(physical, schema))
            assert error is None, f"{physical}: {error}"
            rules += [(physical, rule) for rule in quality]

    for product in MART_PRODUCTS:
        for table in contract_tables(product):
            rules += [(f"{marts.DATABASE}.{table['physicalName']}", rule) for rule in table.get("quality", [])]

    assert len(rules) >= 40
    failures = []
    for physical, rule in rules:
        target = physical if physical.startswith(f"{marts.DATABASE}.") else f"{DATABASE}.`{physical}`"
        sql = rule["query"].replace("{object}", target).strip()
        sql = re.sub(r"\{table:([A-Za-z0-9_]+)\}", rf"{marts.DATABASE}.\1", sql)
        error = run_sql(clickhouse, f"EXPLAIN {sql}")
        if error:
            failures.append(f"{physical}.{rule['id']}: {error}")
    assert not failures, "\n".join(failures)


def test_the_check_rejects_a_spark_function(clickhouse):
    error = run_sql(clickhouse, "EXPLAIN SELECT count(*) FROM system.one WHERE array_length([1]) = 0")

    assert error and "array_length" in error
