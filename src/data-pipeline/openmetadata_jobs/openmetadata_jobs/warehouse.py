import os

SERVICE = "clickhouse"


def database():
    return os.environ.get("CLICKHOUSE_DATABASE", "lakehouse")


def table_fqn(table):
    return f'{SERVICE}.{database()}."{table}"'


def sql_table(table):
    return f"{database()}.`{table}`"
