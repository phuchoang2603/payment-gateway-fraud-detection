import os
import re
import subprocess
from pathlib import Path

import yaml

from batch.schema import EVENT_COLUMNS, LABEL_COLUMNS

CONTRACTS = Path(__file__).resolve().parents[4] / "contracts" / "data-products"
PRODUCTS = ("bronze-payments", "silver-payments")
IMAGE = "clickhouse/clickhouse-server:26.10.2.103"
DATABASE = "lakehouse"
NESTED = {**EVENT_COLUMNS, **LABEL_COLUMNS}
LOGICAL = {
    "string": "Nullable(String)",
    "integer": "Nullable(Int32)",
    "number": "Nullable(Float64)",
    "boolean": "Nullable(Bool)",
    "timestamp": "Nullable(DateTime64(6, 'UTC'))",
    "array": "Array(Nullable(String))",
}
PHYSICAL = {
    "BIGINT": "Nullable(Int64)",
    "INTEGER": "Nullable(Int32)",
    "UINTEGER": "Nullable(UInt32)",
    "BOOLEAN": "Nullable(Bool)",
    "DOUBLE": "Nullable(Float64)",
    "TIMESTAMPTZ": "Nullable(DateTime64(6, 'UTC'))",
    "VARCHAR": "Nullable(String)",
    "VARBINARY": "Nullable(String)",
}
SPARK = {
    "string": "Nullable(String)",
    "int": "Nullable(Int32)",
    "bigint": "Nullable(Int64)",
    "boolean": "Nullable(Bool)",
    "double": "Nullable(Float64)",
    "timestamp": "Nullable(DateTime64(6, 'UTC'))",
    "binary": "Nullable(String)",
}


def spark_to_clickhouse(text):
    parsed, end = _parse(text, 0)
    if text[_skip(text, end) :]:
        raise ValueError(f"unparsed DDL: {text[end:]}")
    return parsed


def _skip(text, i):
    while i < len(text) and text[i].isspace():
        i += 1
    return i


def _parse(text, i):
    i = _skip(text, i)
    if text.startswith("struct<", i):
        i += len("struct<")
        fields = []
        while True:
            match = re.compile(r"\s*(\w+)\s*:\s*").match(text, i)
            inner, i = _parse(text, match.end())
            fields.append(f"{match.group(1)} {inner}")
            i = _skip(text, i)
            if text[i] == ",":
                i += 1
                continue
            if text[i] != ">":
                raise ValueError(f"expected > at {i} in {text}")
            return f"Tuple({', '.join(fields)})", i + 1
    if text.startswith("array<", i):
        inner, i = _parse(text, i + len("array<"))
        i = _skip(text, i)
        if text[i] != ">":
            raise ValueError(f"expected > at {i} in {text}")
        return f"Array({inner})", i + 1
    match = re.compile(r"\w+").match(text, i)
    return SPARK[match.group(0)], match.end()


def column_type(prop):
    logical = prop["logicalType"]
    if logical == "object":
        return spark_to_clickhouse(NESTED[prop["name"]])
    physical = prop.get("physicalType", "").rstrip(",")
    return PHYSICAL.get(physical) or LOGICAL[logical]


def contract_tables(product):
    for contract in sorted((CONTRACTS / product).glob("*.odcs.yaml")):
        yield from yaml.safe_load(contract.read_text())["schema"]


def quality_rules():
    for product in PRODUCTS:
        for table in contract_tables(product):
            for rule in table.get("quality", []):
                if rule.get("type") == "sql":
                    sql = rule["query"].replace("{object}", f"{DATABASE}.`{table['physicalName']}`")
                    yield f"{table['physicalName']}.{rule['id']}", sql.strip()


def script(extra_rules=()):
    statements = [f"CREATE DATABASE IF NOT EXISTS {DATABASE}"]
    for product in PRODUCTS:
        for table in contract_tables(product):
            columns = ", ".join(f"`{prop['name']}` {column_type(prop)}" for prop in table["properties"])
            statements.append(f"CREATE TABLE {DATABASE}.`{table['physicalName']}` ({columns}) ENGINE = Memory")
    statements += [f"EXPLAIN {sql}" for _, sql in list(quality_rules()) + list(extra_rules)]
    return "".join(f"{statement};\n" for statement in statements)


def analyze(sql_script):
    command = os.environ.get("CLICKHOUSE_LOCAL")
    command = [command] if command else ["docker", "run", "--rm", "-i", "--entrypoint", "clickhouse-local", IMAGE]
    result = subprocess.run(
        command + ["--multiquery", "--ignore-error"],
        input=sql_script,
        capture_output=True,
        text=True,
        check=False,
        timeout=900,
    )
    return [line[:300] for line in result.stderr.splitlines() if "Code:" in line or "(query:" in line]


def test_every_quality_rule_analyzes_on_the_pinned_clickhouse():
    rules = list(quality_rules())
    assert len(rules) >= 20

    errors = analyze(script())

    assert not errors, "\n".join(errors[:40])


def test_the_dialect_check_rejects_spark_functions():
    wrong = [
        (
            "probe.array_rule",
            f"SELECT count(*) FROM {DATABASE}.`bronze.payment_events` WHERE array_length(attempt_started.signals.payment_method.type) = 0",
        )
    ]

    assert analyze(script(wrong)), "the analyzer accepted array_length, so the check proves nothing"
