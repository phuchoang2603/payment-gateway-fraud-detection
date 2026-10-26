import re
from dataclasses import dataclass

import yaml
from pyspark.sql import types as T

from data_platform.products import AUDIT, contract_path

SPARK_TYPES = {
    "string": T.StringType,
    "binary": T.BinaryType,
    "long": T.LongType,
    "int": T.IntegerType,
    "double": T.DoubleType,
    "boolean": T.BooleanType,
    "timestamp": T.TimestampType,
}
STARROCKS_TYPES = {
    "string": "varchar(1073741824)",
    "binary": "varbinary",
    "long": "bigint(20)",
    "int": "int(11)",
    "double": "double",
    "timestamp": "datetime",
}


def data_type(prop):
    if prop["logicalType"] == "object":
        return physical_schema(prop["properties"])
    if prop["logicalType"] == "array":
        return T.ArrayType(data_type(prop["items"]), containsNull=not prop["items"].get("required", False))
    try:
        return SPARK_TYPES[prop["physicalType"]]()
    except KeyError as error:
        raise ValueError(f"unsupported physical type: {prop}") from error


def physical_schema(properties):
    return T.StructType([T.StructField(p["name"], data_type(p), not p.get("required", False)) for p in properties])


def starrocks_type(prop, top=True):
    if prop["logicalType"] == "object":
        return "struct<" + ", ".join(f"`{p['name']}` {starrocks_type(p, False)}" for p in prop["properties"]) + ">"
    if prop["logicalType"] == "array":
        return "array<" + starrocks_type(prop["items"], False) + ">"
    if prop["physicalType"] == "boolean":
        return "tinyint(1)" if top else "boolean"
    try:
        return STARROCKS_TYPES[prop["physicalType"]]
    except KeyError as error:
        raise ValueError(f"unsupported physical type: {prop}") from error


def required_predicates(properties, expr="e", trail="", present="TRUE", depth=0):
    """Map each required field path to a StarRocks predicate that is true for violating rows."""
    predicates = {}
    for p in properties:
        child = f"{expr}.`{p['name']}`"
        key = f"{trail}{p['name']}"
        if p.get("required"):
            predicates[key] = f"({present}) AND {child} IS NULL"
        if p["logicalType"] == "object":
            predicates.update(
                required_predicates(p["properties"], child, key + ".", f"({present}) AND {child} IS NOT NULL", depth)
            )
        if p["logicalType"] == "array":
            item = p["items"]
            name = f"item{depth}"
            if item.get("required"):
                predicates[key + "[]"] = f"({present}) AND ANY_MATCH({name} -> {name} IS NULL, {child})"
            if item["logicalType"] == "object":
                inner = required_predicates(item["properties"], name, key + "[].", f"{name} IS NOT NULL", depth + 1)
                for path, predicate in inner.items():
                    predicates[path] = f"({present}) AND ANY_MATCH({name} -> {predicate}, {child})"
    return predicates


def semantic_name(identifier):
    return re.sub(r"[^A-Za-z0-9_]+", "__", identifier.replace("[]", "_items")).strip("_")


@dataclass
class Rule:
    id: str
    description: str
    sql: str


@dataclass
class PreparedContract:
    document: dict
    schema: T.StructType
    rules: list

    @property
    def business_columns(self):
        return [f.name for f in self.schema if f.name not in AUDIT]


def count_query(identifier, query):
    if not re.search(r"SELECT\s+COUNT\s*\(\s*\*\s*\)", query, re.I):
        raise ValueError(f"rule must return a violation count: {identifier}")
    return query


def prepare(table, path=None):
    path = path or contract_path(table)
    doc = yaml.safe_load(path.read_text())
    schema = doc["schema"][0]
    rules = []
    for column in schema["properties"]:
        rules.append(
            Rule(
                f"schema_type__{semantic_name(column['name'])}",
                f"Persisted type of {column['name']} equals the contract physical type.",
                "SELECT COUNT(*) FROM (SELECT 1) AS anchor WHERE NOT EXISTS (\n"
                "  SELECT 1 FROM information_schema.columns AS c\n"
                f"  WHERE c.table_schema = '{{schema}}' AND c.table_name = '{{table}}' AND c.column_name = '{column['name']}'\n"
                f"    AND c.column_type = '{starrocks_type(column)}'\n)",
            )
        )
    for key, predicate in required_predicates(schema["properties"]).items():
        rules.append(
            Rule(
                f"schema_required__{semantic_name(key)}",
                f"Required field {key} is present whenever its enclosing value is present.",
                "SELECT COUNT(*) FROM {relation} AS e WHERE " + predicate,
            )
        )
    declared = []
    for quality in schema.get("quality", []):
        if quality.get("type") != "sql" or quality.get("mustBe") != 0 or not quality.get("query"):
            raise ValueError(f"unsupported quality rule: {quality.get('id')}")
        declared.append(quality["id"])
        rules.append(Rule(quality["id"], quality["description"], count_query(quality["id"], quality["query"])))
    if len(declared) != len(set(declared)) or len({r.id for r in rules}) != len(rules):
        raise ValueError("duplicate quality rule ID")
    return PreparedContract(doc, physical_schema(schema["properties"]), rules)


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
