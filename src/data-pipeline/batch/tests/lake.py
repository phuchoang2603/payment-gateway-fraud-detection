from pyspark.sql import functions as F

from batch.bronze import ingest as bronze
from batch.bronze.schema import EVENT_COLUMNS, LABEL_COLUMNS, business_schema

TOPICS = {bronze.EVENTS: "payments.events.v1", bronze.LABELS: "fraud.labels.v1"}
COLUMNS = {bronze.EVENTS: EVENT_COLUMNS, bronze.LABELS: LABEL_COLUMNS}


def provenance(frame, source):
    return frame.withColumns(
        {
            "_source": F.lit(source),
            "_dataset_version": F.lit("v-test" if source == "historical" else None).cast("string"),
            "_source_file": F.lit("payment_events/part.parquet" if source == "historical" else None).cast("string"),
            "_kafka_topic": F.lit(None if source == "historical" else "payments.events.v1").cast("string"),
            "_kafka_partition": F.lit(None).cast("int"),
            "_kafka_offset": F.lit(None).cast("bigint"),
            "_kafka_timestamp": F.lit(None).cast("timestamp"),
            "_ingested_at": F.current_timestamp(),
            "_run_id": F.lit("bronze-test"),
        }
    )


def load_bronze(spark, events, labels, source="historical"):
    """Replace Bronze with these rows, as one historical or Kafka load."""
    bronze.ensure_tables(spark)
    spark.createDataFrame(events, business_schema(spark, EVENT_COLUMNS)).transform(
        lambda frame: provenance(frame, source)
    ).writeTo(bronze.EVENTS).overwrite(F.lit(True))
    if labels is not None:
        spark.createDataFrame(labels, business_schema(spark, LABEL_COLUMNS)).transform(
            lambda frame: provenance(frame, source)
        ).writeTo(bronze.LABELS).overwrite(F.lit(True))


def deliver(spark, lake, table, rows):
    """Append rows to Bronze as Kafka deliveries on partition 0 with consecutive offsets."""
    for values in rows:
        offset = lake["offsets"][table]
        lake["offsets"][table] += 1
        spark.createDataFrame([values], business_schema(spark, COLUMNS[table])).withColumns(
            {
                "_source": F.lit("kafka"),
                "_dataset_version": F.lit(None).cast("string"),
                "_source_file": F.lit(None).cast("string"),
                "_kafka_topic": F.lit(TOPICS[table]),
                "_kafka_partition": F.lit(0),
                "_kafka_offset": F.lit(offset).cast("bigint"),
                "_kafka_timestamp": F.current_timestamp(),
                "_ingested_at": F.current_timestamp(),
                "_run_id": F.lit("bronze-kafka-test"),
            }
        ).writeTo(table).append()
