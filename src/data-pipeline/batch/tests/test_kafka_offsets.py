from batch import bronze, lakehouse
from batch.schema import event_ddl
from tests.events import created

TOPIC = "payments.events.compaction-test"


def kafka_row(offset):
    values = created(f"pay-{offset}", f"evt-{offset}")
    values.update(
        _source="kafka",
        _dataset_version=None,
        _source_file=None,
        _kafka_topic=TOPIC,
        _kafka_partition=0,
        _kafka_offset=offset,
        _kafka_timestamp=None,
        _ingested_at=None,
        _run_id="run-compaction",
    )
    return values


def test_compaction_between_runs_does_not_replay_kafka_offsets(spark):
    bronze.ensure_tables(spark)
    for start in range(0, 20, 2):
        rows = [kafka_row(offset) for offset in (start, start + 1)]
        spark.createDataFrame(rows, schema=event_ddl()).writeTo(bronze.EVENTS).append()
    before = bronze.next_starts(spark, bronze.EVENTS, TOPIC, {0: 0}, kafka_rows=True)
    files_before = lakehouse.file_stats(spark, bronze.EVENTS)["files"]

    spark.sql(
        f"CALL {bronze.ICEBERG_CATALOG}.system.rewrite_data_files(table => '{bronze.EVENTS}', strategy => 'binpack')"
    )

    after = bronze.next_starts(spark, bronze.EVENTS, TOPIC, {0: 0}, kafka_rows=True)
    assert lakehouse.file_stats(spark, bronze.EVENTS)["files"] < files_before
    assert before == after == {0: 20}
