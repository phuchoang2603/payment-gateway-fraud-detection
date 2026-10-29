import json
import os

from pyflink.common import Configuration, Types, WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment

from fraud_streaming.operators import REJECTS, Aggregate, Clock, Deduplicate, ObserveSource


def sql_literal(value):
    return "'" + value.replace("'", "''") + "'"


def build_updates(packets, *, epoch, logical, max_entries):
    # Every operator that yields to REJECTS must have its side output read, or rejects leak
    # into its main output.
    rejects = [packets.get_side_output(REJECTS)]
    if logical:
        packets = packets.assign_timestamps_and_watermarks(
            WatermarkStrategy.for_monotonous_timestamps().with_timestamp_assigner(Clock())
        )
    events = (
        packets.filter(lambda packet: packet.fact is not None)
        .key_by(lambda packet: packet.fact.event_id)
        .process(Deduplicate(logical, "event"))
        .uid("event-deduplication-v3")
        .name("Deduplicate events")
    )
    starts = (
        events.key_by(lambda packet: json.dumps(packet.fact.identity))
        .process(Deduplicate(logical, "attempt"))
        .uid("attempt-deduplication-v1")
        .name("Deduplicate attempts")
    )
    rejects += [events.get_side_output(REJECTS), starts.get_side_output(REJECTS)]
    row_type = Types.ROW_NAMED(
        ["key", "payload"],
        [Types.PRIMITIVE_ARRAY(Types.BYTE()), Types.PRIMITIVE_ARRAY(Types.BYTE())],
    )
    branches = []
    for view in ("customer", "method"):
        branch = (
            starts.key_by(
                lambda packet, view=view: json.dumps(
                    [packet.fact.integration, getattr(packet.fact, view)]
                )
            )
            .process(
                Aggregate(
                    view=view,
                    epoch=epoch,
                    logical=logical,
                    max_entries=max_entries,
                ),
                output_type=row_type,
            )
            .uid(f"{view}-aggregation-v3")
            .name(f"Aggregate {view} windows")
        )
        branches.append(branch)
    return branches[0].union(branches[1]), rejects[0].union(*rejects[1:])


def main():
    epoch = os.environ["FEATURE_EPOCH"]
    brokers = os.environ["KAFKA_BROKERS"]
    source = os.environ.get("SOURCE_TOPIC", "payments.events.v1")
    output = os.environ["FEATURE_TOPIC"]
    logical = os.environ.get("CLOCK_MODE", "live") == "logical"
    clock_topic = os.environ.get("CLOCK_TOPIC")
    if logical and (not clock_topic or epoch.startswith("live") or source == "payments.events.v1"):
        raise ValueError("logical replay requires isolated input, output epoch and clock topic")
    if output != f"fraud.feature-updates.v2.{epoch}":
        raise ValueError("feature journal must be bound to its epoch")
    rejects_topic = f"fraud.feature-rejects.v1.{epoch}"
    configuration = Configuration()
    configuration.set_string("pipeline.name", f"fraud-features-{epoch}")
    configuration.set_string("python.execution-mode", "process")
    env = StreamExecutionEnvironment.get_execution_environment(configuration)
    env.set_parallelism(1)
    env.enable_checkpointing(10_000)
    table = StreamTableEnvironment.create(env)
    topics = ";".join([source, clock_topic] if logical else [source])
    table.execute_sql(
        f"""CREATE TABLE payment_source (
            payload BYTES,
            topic STRING METADATA FROM 'topic' VIRTUAL,
            `partition` INT METADATA FROM 'partition' VIRTUAL,
            `offset` BIGINT METADATA FROM 'offset' VIRTUAL,
            `timestamp` TIMESTAMP_LTZ(3) METADATA FROM 'timestamp' VIRTUAL
        ) WITH (
            'connector' = 'kafka',
            'topic' = {sql_literal(topics)},
            'properties.bootstrap.servers' = {sql_literal(brokers)},
            'properties.group.id' = {sql_literal("fraud-features-" + epoch)},
            'properties.isolation.level' = 'read_committed',
            'properties.auto.offset.reset' = 'none',
            'scan.startup.mode' = {sql_literal("earliest-offset" if logical else "latest-offset")},
            'format' = 'raw'
        )"""
    )
    packets = (
        table.to_data_stream(table.from_path("payment_source"))
        .key_by(lambda _: "source-progress")
        .process(
            ObserveSource(
                brokers=brokers,
                topic=source,
                partitions=int(os.environ.get("SOURCE_PARTITIONS", "3")),
                logical=logical,
                clock_topic=clock_topic,
            )
        )
        .uid("observed-source-progress-v1")
        .name("Observe source progress")
    )
    updates, rejects = build_updates(
        packets,
        epoch=epoch,
        logical=logical,
        max_entries=int(os.environ.get("MAX_ENTITY_ENTRIES", "50000")),
    )
    table.execute_sql(
        f"""CREATE TABLE feature_journal (`key` BYTES, payload BYTES) WITH (
            'connector' = 'kafka',
            'topic' = {sql_literal(output)},
            'properties.bootstrap.servers' = {sql_literal(brokers)},
            'key.format' = 'raw',
            'key.fields' = 'key',
            'value.format' = 'raw',
            'value.fields-include' = 'EXCEPT_KEY',
            'sink.delivery-guarantee' = 'exactly-once',
            'properties.transaction.timeout.ms' = '900000',
            'sink.transactional-id-prefix' = {sql_literal("fraud-features-" + epoch + "-")}
        )"""
    )
    table.execute_sql(
        f"""CREATE TABLE feature_rejects (
            `key` BYTES,
            payload BYTES,
            headers MAP<STRING, BYTES> METADATA
        ) WITH (
            'connector' = 'kafka',
            'topic' = {sql_literal(rejects_topic)},
            'properties.bootstrap.servers' = {sql_literal(brokers)},
            'key.format' = 'raw',
            'key.fields' = 'key',
            'value.format' = 'raw',
            'value.fields-include' = 'EXCEPT_KEY',
            'sink.delivery-guarantee' = 'exactly-once',
            'properties.transaction.timeout.ms' = '900000',
            'sink.transactional-id-prefix' = {sql_literal("fraud-rejects-" + epoch + "-")}
        )"""
    )
    statements = table.create_statement_set()
    statements.add_insert("feature_journal", table.from_data_stream(updates))
    statements.add_insert("feature_rejects", table.from_data_stream(rejects))
    statements.execute().wait()


if __name__ == "__main__":
    main()
