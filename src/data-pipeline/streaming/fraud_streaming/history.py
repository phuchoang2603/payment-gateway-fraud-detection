import argparse
import json
import os
import time
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from urllib.parse import urlsplit

from confluent_kafka import ConsumerGroupTopicPartitions, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic
from google.protobuf.json_format import ParseDict

from fraud_streaming.wire import PaymentEvent


def encode(row):
    def clean(value):
        if isinstance(value, dict):
            return {
                key: clean(item)
                for key, item in value.items()
                if item is not None and key != "_present"
            }
        if isinstance(value, list):
            return [clean(item) for item in value]
        if isinstance(value, datetime):
            return value.replace(tzinfo=UTC).isoformat()
        return value

    fields = PaymentEvent.DESCRIPTOR.fields_by_name
    document = clean({key: value for key, value in row.items() if key in fields})
    if document["schema_version"] not in (2, 3):
        raise ValueError("unsupported Bronze payment schema")
    document["schema_version"] = 3
    return ParseDict(document, PaymentEvent()).SerializeToString(deterministic=True)


def lake():
    from pyiceberg.catalog import load_catalog

    return load_catalog(
        "lake",
        type="rest",
        uri=os.environ["ICEBERG_REST_URI"],
        warehouse=os.environ["ICEBERG_WAREHOUSE"],
        credential=f"{os.environ['ICEBERG_CLIENT_ID']}:{os.environ['ICEBERG_CLIENT_SECRET']}",
        **{
            "oauth2-server-uri": os.environ["ICEBERG_OAUTH2_URI"],
            "scope": "PRINCIPAL_ROLE:ALL",
            "s3.endpoint": os.environ["S3_ENDPOINT"],
            "s3.access-key-id": os.environ["AWS_ACCESS_KEY_ID"],
            "s3.secret-access-key": os.environ["AWS_SECRET_ACCESS_KEY"],
            "s3.region": os.environ.get("AWS_REGION", "us-east-1"),
        },
    )


def topics(epoch):
    if not epoch.startswith("history-"):
        raise ValueError("history topics require a history epoch")
    return [
        f"payments.events.{epoch}",
        f"fraud.clock.{epoch}",
        f"fraud.feature-updates.v2.{epoch}",
        f"fraud.feature-rejects.v1.{epoch}",
    ]


def write_report(report):
    location = urlsplit(os.environ["HISTORY_SEED_REPORT"])
    body = json.dumps(report, sort_keys=True).encode()
    if location.scheme == "s3":
        import boto3
        from botocore.config import Config

        client = boto3.client(
            "s3",
            endpoint_url=os.environ["S3_ENDPOINT"],
            config=Config(
                s3={
                    "addressing_style": "path"
                    if os.environ.get("S3_PATH_STYLE") == "true"
                    else "virtual"
                }
            ),
        )
        client.put_object(Bucket=location.netloc, Key=location.path.lstrip("/"), Body=body)
    else:
        Path(os.environ["HISTORY_SEED_REPORT"]).write_bytes(body)


def wait_for_lag(admin, epoch, produced, bound, timeout=600):
    deadline = time.monotonic() + timeout
    while produced > bound:
        group = f"fraud-features-{epoch}"
        offsets = (
            admin.list_consumer_group_offsets(
                [ConsumerGroupTopicPartitions(group, [TopicPartition(topics(epoch)[0], 0)])],
                request_timeout=30,
            )[group]
            .result()
            .topic_partitions
        )
        consumed = max(offsets[0].offset, 0)
        if produced - consumed <= bound:
            return
        if time.monotonic() > deadline:
            raise TimeoutError("history replay stopped advancing")
        time.sleep(2)


def seed(start, end):
    import duckdb
    from pyiceberg.expressions import And, EqualTo, GreaterThanOrEqual, LessThan

    first, last = date.fromisoformat(start), date.fromisoformat(end)
    if first >= last:
        raise ValueError("history start must precede end")
    epoch = os.environ["FEATURE_EPOCH"]
    input_topic, clock_topic, *_ = topics(epoch)
    bootstrap = datetime.combine(first - timedelta(days=1), datetime.min.time(), UTC)
    until = datetime.combine(last, datetime.min.time(), UTC)
    table = lake().load_table("bronze.payment_events")
    snapshot = table.current_snapshot().snapshot_id
    predicate = And(
        EqualTo("_dataset_version", os.environ["DATASET_VERSION"]),
        And(GreaterThanOrEqual("recorded_at", bootstrap), LessThan("recorded_at", until)),
    )
    scan = table.scan(row_filter=predicate, snapshot_id=snapshot).to_arrow_batch_reader()
    db = duckdb.connect()
    db.execute("SET memory_limit = '1GB'")
    db.execute("SET temp_directory = '/tmp/history-sort'")
    db.register("bronze", scan)
    reader = db.sql(
        "SELECT * FROM bronze ORDER BY recorded_at, integration_id, payment_id, aggregate_version, event_id, _source_file, _kafka_offset"
    ).to_arrow_reader(1000)
    brokers = os.environ["KAFKA_BROKERS"]
    admin = AdminClient({"bootstrap.servers": brokers})
    producer = Producer(
        {
            "bootstrap.servers": brokers,
            "enable.idempotence": True,
            "acks": "all",
            "queue.buffering.max.messages": 20000,
        }
    )
    offset, sequence = 0, 0
    failures = []

    def delivered(error, message):
        if error:
            failures.append(str(error))

    def flush():
        if producer.flush(60) or failures:
            raise RuntimeError("history Kafka delivery failed")

    def clock(instant):
        nonlocal sequence
        flush()
        producer.produce(
            clock_topic,
            partition=0,
            value=json.dumps(
                {
                    "sequence": sequence,
                    "clock_us": instant,
                    "input_offsets": {"0": offset},
                }
            ).encode(),
            on_delivery=delivered,
        )
        flush()
        sequence += 1

    began = time.monotonic()
    clock(int(bootstrap.timestamp() * 1_000_000))
    try:
        for batch in reader:
            for row in batch.to_pylist():
                raw = encode(row)
                producer.produce(
                    input_topic,
                    partition=0,
                    key=row["payment_id"].encode(),
                    value=raw,
                    timestamp=int(row["recorded_at"].replace(tzinfo=UTC).timestamp() * 1000),
                    on_delivery=delivered,
                )
                offset += 1
            clock(int(row["recorded_at"].replace(tzinfo=UTC).timestamp() * 1_000_000))
            wait_for_lag(admin, epoch, offset, 1000)
        clock(int((until + timedelta(days=1, milliseconds=1)).timestamp() * 1_000_000))
        report = {
            "start": start,
            "end": end,
            "epoch": epoch,
            "snapshot": snapshot,
            "events": offset,
            "input_end": offset,
            "clock_end": sequence,
            "events_per_second": offset / max(time.monotonic() - began, 1),
            "done": True,
        }
        write_report(report)
    finally:
        db.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=("seed", "topics", "months"))
    parser.add_argument("--start", default=os.environ.get("START_DAY"))
    parser.add_argument("--end", default=os.environ.get("END_DAY"))
    parser.add_argument("--delete", action="store_true")
    args = parser.parse_args()
    if args.mode == "seed":
        try:
            seed(args.start, args.end)
        except Exception as error:
            write_report(
                {
                    "failed": True,
                    "error": type(error).__name__,
                    "start": args.start,
                    "end": args.end,
                }
            )
            raise
    elif args.mode == "topics":
        epoch = os.environ["FEATURE_EPOCH"]
        admin = AdminClient({"bootstrap.servers": os.environ["KAFKA_BROKERS"]})
        existing = set(admin.list_topics(timeout=30).topics)
        names = topics(epoch)
        futures = admin.delete_topics(
            [name for name in names if name in existing], operation_timeout=30
        )
        for future in futures.values():
            future.result()
        if not args.delete:
            deadline = time.monotonic() + 120
            while existing.intersection(names):
                if time.monotonic() > deadline:
                    raise TimeoutError("history topics were not deleted")
                time.sleep(1)
                existing = set(admin.list_topics(timeout=30).topics)
            configs = {"retention.ms": "604800000", "cleanup.policy": "delete"}
            for name, future in admin.create_topics(
                [
                    NewTopic(
                        name,
                        num_partitions=1,
                        replication_factor=int(os.environ.get("HISTORY_REPLICATION_FACTOR", "1")),
                        config={
                            **configs,
                            **({"retention.bytes": "1073741824"} if name == names[0] else {}),
                        },
                    )
                    for name in names
                ]
            ).items():
                future.result()
    else:
        first, last = date.fromisoformat(args.start), date.fromisoformat(args.end)
        if first >= last:
            raise ValueError("history start must precede end")
        end = min((first.replace(day=28) + timedelta(days=4)).replace(day=1), last)
        Path("/tmp/unit-end").write_text(end.isoformat())
        Path("/tmp/more").write_text("true" if end < last else "false")


if __name__ == "__main__":
    main()
