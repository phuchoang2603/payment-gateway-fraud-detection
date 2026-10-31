SERVICE = "clickhouse"
OPENMETADATA_DATABASE = "default"
LAKEHOUSE = "lakehouse"
MARTS = "marts"


def table_fqn(table, database=LAKEHOUSE):
    """The OpenMetadata FQN; lakehouse table names hold a dot and are quoted."""
    name = f'"{table}"' if "." in table else table
    return f"{SERVICE}.{OPENMETADATA_DATABASE}.{database}.{name}"


def sql_table(table, database=LAKEHOUSE):
    return f"{database}.`{table}`"
