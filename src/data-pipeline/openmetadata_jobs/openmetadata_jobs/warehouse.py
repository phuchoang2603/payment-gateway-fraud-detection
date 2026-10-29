import os

SERVICE = "clickhouse"
OPENMETADATA_DATABASE = "default"


def schema():
    return os.environ.get("CLICKHOUSE_DATABASE", "lakehouse")


def table_fqn(table):
    return f'{SERVICE}.{OPENMETADATA_DATABASE}.{schema()}."{table}"'


def sql_table(table):
    return f"{schema()}.`{table}`"
