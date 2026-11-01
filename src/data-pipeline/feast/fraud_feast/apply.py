from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import DayTransform
from pyiceberg.types import LongType, NestedField, StringType, TimestamptzType

from fraud_feast.lake import catalog
from fraud_feast.repo import KEYS, STREAM_FIELDS, TABLES, definitions
from fraud_feast.runtime import store


def stream_schema(scope):
    columns = [
        ("integration_id", StringType()),
        (KEYS[scope], StringType()),
        ("event_timestamp", TimestamptzType()),
        ("created", TimestamptzType()),
        ("epoch", StringType()),
        ("revision", LongType()),
        ("_kafka_partition", LongType()),
        ("_kafka_offset", LongType()),
        *((field, LongType()) for field in STREAM_FIELDS[scope]),
    ]
    return Schema(
        *[
            NestedField(index, name, kind, required=False)
            for index, (name, kind) in enumerate(columns, 1)
        ]
    )


def ensure_tables(lake):
    lake.create_namespace_if_not_exists("gold")
    for scope, name in TABLES.items():
        lake.create_table_if_not_exists(
            name,
            schema=stream_schema(scope),
            partition_spec=PartitionSpec(
                PartitionField(
                    source_id=3,
                    field_id=1000,
                    transform=DayTransform(),
                    name="event_day",
                )
            ),
            properties={"format-version": "2", "write.target-file-size-bytes": "268435456"},
        )


def main():
    ensure_tables(catalog())
    store().apply(definitions())


if __name__ == "__main__":
    main()
