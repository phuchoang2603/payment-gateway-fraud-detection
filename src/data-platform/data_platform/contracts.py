import json
import re
from dataclasses import dataclass

import great_expectations as gx
import yaml
from datacontract.data_contract import DataContract
from great_expectations.expectations.registry import get_expectation_impl
from pyspark.sql import types as T

from data_platform.products import AUDIT, contract_path

TYPES = {
    "string": T.StringType,
    "long": T.LongType,
    "int": T.IntegerType,
    "double": T.DoubleType,
    "boolean": T.BooleanType,
    "timestamp": T.TimestampType,
}


def data_type(prop):
    if prop["logicalType"] == "object":
        return physical_schema(prop["properties"])
    if prop["logicalType"] == "array":
        return T.ArrayType(data_type(prop["items"]), containsNull=not prop["items"].get("required", False))
    try:
        return TYPES[prop["physicalType"]]()
    except KeyError as error:
        raise ValueError(f"unsupported physical type: {prop}") from error


def physical_schema(properties):
    return T.StructType([T.StructField(p["name"], data_type(p), not p.get("required", False)) for p in properties])


def required_predicates(properties, path="", present="true"):
    predicates = {}
    for p in properties:
        expr = f"{path}.`{p['name']}`" if path else f"`{p['name']}`"
        key = expr.replace("`", "")
        if p.get("required"):
            predicates[key] = f"({present}) AND {expr} IS NULL"
        if p["logicalType"] == "object":
            predicates.update(required_predicates(p["properties"], expr, f"({present}) AND {expr} IS NOT NULL"))
        if p["logicalType"] == "array":
            item = p["items"]
            if item.get("required"):
                predicates[key + "[]"] = f"exists({expr}, item -> item IS NULL)"
            if item["logicalType"] == "object":
                for child, pred in required_predicates(item["properties"], "item", "item IS NOT NULL").items():
                    predicates[key + "[]." + child.removeprefix("item.")] = f"exists({expr}, item -> {pred})"
    return predicates


@dataclass
class PreparedContract:
    document: dict
    schema: T.StructType
    suite: gx.ExpectationSuite
    coverage: dict

    @property
    def business_columns(self):
        return [f.name for f in self.schema if f.name not in AUDIT]


def prepare(table, path=None):
    path = path or contract_path(table)
    doc = yaml.safe_load(path.read_text())
    schema = doc["schema"][0]
    expected = physical_schema(schema["properties"])
    raw = json.loads(
        DataContract(data_contract_file=str(path)).export(
            "great-expectations", schema_name=schema["name"], engine="spark"
        )
    )
    exported = raw["expectations"]
    for entry in exported:
        if entry["type"] == "expect_table_columns_to_match_set":
            entry["kwargs"]["exact_match"] = False
    declared = {q["id"] for q in schema.get("quality", [])}
    if len(declared) != len(schema.get("quality", [])):
        raise ValueError("duplicate quality rule ID")
    covered = {e.get("meta", {}).get("id") for e in exported if e.get("meta", {}).get("id")}
    if covered != declared:
        raise ValueError(f"uncovered quality rules: {declared - covered}; unexpected rules: {covered - declared}")
    for q in schema.get("quality", []):
        if q.get("engine") != "great-expectations" or q.get("type") != "custom":
            raise ValueError(f"unsupported quality engine: {q['id']}")
        impl = q["implementation"]
        query = impl.get("kwargs", {}).get("unexpected_rows_query", "")
        if (
            impl.get("type") != "unexpected_rows_expectation"
            or not query
            or re.search(r"SELECT\s+count\s*\(", query, re.I)
        ):
            raise ValueError(f"rule must return unexpected rows: {q['id']}")
    requirements = required_predicates(schema["properties"])
    for key, predicate in requirements.items():
        if "." not in key and "[]" not in key:
            if not any(
                e["type"] == "expect_column_values_to_not_be_null" and e["kwargs"]["column"] == key for e in exported
            ):
                raise ValueError(f"uncovered required field: {key}")
            continue
        exported.append(
            {
                "type": "unexpected_rows_expectation",
                "kwargs": {"unexpected_rows_query": "SELECT * FROM {batch} WHERE " + predicate},
                "meta": {"id": "schema.required." + key},
            }
        )
    expectations = []
    for entry in exported:
        implementation = get_expectation_impl(entry["type"])
        if implementation is None:
            raise ValueError(f"unsupported expectation: {entry['type']}")
        expectations.append(implementation(**entry["kwargs"], meta=entry.get("meta", {})))
    suite = gx.ExpectationSuite(name=raw["name"], expectations=expectations, meta=raw.get("meta", {}))
    return PreparedContract(
        doc,
        expected,
        suite,
        {"quality": sorted(declared), "required": sorted(requirements), "physical_schema": expected.jsonValue()},
    )


def check_physical(actual, expected, path=""):
    if isinstance(expected, T.StructType):
        if not isinstance(actual, T.StructType) or actual.fieldNames() != expected.fieldNames():
            raise ValueError(f"schema columns differ at {path}")
        for field in expected:
            check_physical(actual[field.name].dataType, field.dataType, f"{path}.{field.name}")
    elif isinstance(expected, T.ArrayType):
        if not isinstance(actual, T.ArrayType):
            raise ValueError(f"schema array differs at {path}")
        check_physical(actual.elementType, expected.elementType, path + "[]")
    elif type(actual) is not type(expected):
        raise ValueError(f"schema type differs at {path}: {actual} != {expected}")
