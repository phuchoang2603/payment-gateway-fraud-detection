import os

import yaml
from pyiceberg import types as T
from pyiceberg.catalog import load_catalog

from data_platform.products import PORTS, PRODUCTS

LAKE_SERVICE = os.environ.get("OPENMETADATA_LAKE_SERVICE", "fraud-lakehouse-prod")
LAKE_DATABASE = os.environ.get("OPENMETADATA_LAKE_DATABASE", "lake")
SOURCE_SERVICE = os.environ.get("OPENMETADATA_SOURCE_SERVICE", "synthetic-source")
SOURCE_DATABASE = os.environ.get("OPENMETADATA_SOURCE_DATABASE", "historical")
SCHEMA = "bronze"


def iceberg_config():
    access = os.environ.get("CATALOG_ACCESS_KEY_ID") or os.environ["LAKE_ACCESS_KEY_ID"]
    secret = os.environ.get("CATALOG_SECRET_ACCESS_KEY") or os.environ["LAKE_SECRET_ACCESS_KEY"]
    region = os.environ.get("AWS_REGION", "us-east-1")
    return {
        "type": "rest",
        "uri": os.environ["ICEBERG_REST_URI"],
        "warehouse": os.environ.get("ICEBERG_WAREHOUSE", "fraud-lakehouse-prod"),
        "client.access-key-id": access,
        "client.secret-access-key": secret,
        "client.region": region,
        "rest.sigv4-enabled": os.environ.get("ICEBERG_SIGV4_ENABLED", "true"),
        "rest.signing-name": os.environ.get("ICEBERG_SIGNING_NAME", "s3"),
        "rest.signing-region": region,
        "s3.endpoint": os.environ.get("S3_ENDPOINT", "https://minio.home.phuchoang.sbs"),
        "s3.region": region,
        "s3.access-key-id": access,
        "s3.secret-access-key": secret,
    }


SCALARS = {
    T.BooleanType: "BOOLEAN",
    T.IntegerType: "INT",
    T.LongType: "BIGINT",
    T.FloatType: "FLOAT",
    T.DoubleType: "DOUBLE",
    T.DateType: "DATE",
    T.TimeType: "TIME",
    T.TimestampType: "TIMESTAMP",
    T.TimestamptzType: "TIMESTAMP",
    T.StringType: "STRING",
    T.UUIDType: "UUID",
    T.BinaryType: "BINARY",
}


def om_type(kind):
    if isinstance(kind, T.StructType):
        return "STRUCT"
    if isinstance(kind, T.ListType):
        return "ARRAY"
    if isinstance(kind, T.MapType):
        return "MAP"
    if isinstance(kind, T.DecimalType):
        return "DECIMAL"
    if isinstance(kind, T.FixedType):
        return "BINARY"
    for scalar, name in SCALARS.items():
        if isinstance(kind, scalar):
            return name
    raise ValueError(f"unsupported Iceberg type {kind}")


def om_column(name, kind, doc=None):
    column = {"name": name, "dataType": om_type(kind), "dataTypeDisplay": str(kind)}
    if doc:
        column["description"] = doc
    if isinstance(kind, T.StructType):
        column["children"] = [om_column(f.name, f.field_type, f.doc) for f in kind.fields]
    elif isinstance(kind, T.ListType):
        column["arrayDataType"] = om_type(kind.element_type)
        if isinstance(kind.element_type, T.StructType):
            column["children"] = [om_column(f.name, f.field_type, f.doc) for f in kind.element_type.fields]
    return column


def lake_columns(schema):
    return [om_column(f.name, f.field_type, f.doc) for f in schema.fields]


CONTRACT_TYPES = {
    "string": "STRING",
    "integer": "BIGINT",
    "number": "DOUBLE",
    "boolean": "BOOLEAN",
    "timestamp": "TIMESTAMP",
    "date": "DATE",
    "object": "JSON",
    "array": "JSON",
}


def source_columns(contract):
    return [
        {
            "name": p["name"],
            "dataType": CONTRACT_TYPES[p["logicalType"]],
            "dataTypeDisplay": p.get("physicalType", p["logicalType"]).lower(),
            **({"description": p["description"]} if p.get("description") else {}),
        }
        for p in contract["schema"][0]["properties"]
    ]


def load_contract(table, product):
    return yaml.safe_load((PRODUCTS / product / f"{PORTS[table]}.odcs.yaml").read_text())


def put_table(client, service, database, schema, name, columns, description, owner=None):
    client.put("/v1/services/databaseServices", {"name": service, "serviceType": "CustomDatabase"})
    client.put("/v1/databases", {"name": database, "service": service})
    client.put("/v1/databaseSchemas", {"name": schema, "database": f"{service}.{database}"})
    body = {
        "name": name,
        "databaseSchema": f"{service}.{database}.{schema}",
        "tableType": "Regular",
        "columns": columns,
        "description": description,
    }
    if owner:
        body["owners"] = [{"type": "user", "name": owner}]
    table = client.put("/v1/tables", body)
    return {"id": table["id"], "fqn": table["fullyQualifiedName"], "columns": columns}


def discover(client, run_id, owner=None):
    catalog = load_catalog("lake", **iceberg_config())
    identities = {}
    for table in PORTS:
        iceberg = catalog.load_table(f"{SCHEMA}.raw_{table}")
        contract = load_contract(table, "bronze-payments")
        identities[table] = put_table(
            client,
            LAKE_SERVICE,
            LAKE_DATABASE,
            SCHEMA,
            f"raw_{table}",
            lake_columns(iceberg.schema()),
            contract["schema"][0]["description"],
            owner,
        )
    if set(identities) != set(PORTS):
        raise RuntimeError("Iceberg discovery did not find both canonical tables")
    return identities


def register_source(client, run, owner=None):
    manifest = f"{run['source_root']}/manifest.json"
    return {
        table: put_table(
            client,
            SOURCE_SERVICE,
            SOURCE_DATABASE,
            run["dataset_version"],
            table,
            source_columns(contract := load_contract(table, "historical-payments")),
            f"{contract['schema'][0]['description']} Immutable manifest: {manifest} (sha256 {run['manifest_sha256']}).",
            owner,
        )
        for table in PORTS
    }
