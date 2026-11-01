import argparse
import json
import os
import time
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from urllib.parse import urlsplit

from confluent_kafka import Producer
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

    catalog = load_catalog(
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
            "s3.force-virtual-addressing": "false"
            if os.environ.get("S3_PATH_STYLE") == "true"
            else "true",
        },
    )
    # Polaris runs with STS disabled and rejects PyIceberg's default request for vended credentials.
    catalog._session.headers.pop("X-Iceberg-Access-Delegation", None)
    return catalog


def topics(run):
    if not run.startswith("history-"):
        raise ValueError("history topics require a history epoch")
    return [
        f"payments.events.{run}",
        f"fraud.clock.{run}",
        f"fraud.feature-updates.v2.{run}",
        f"fraud.feature-rejects.v1.{run}",
    ]


def units(first, last, days):
    if first >= last or days < 1:
        raise ValueError("history needs a nonempty range and a positive unit length")
    result = []
    while first < last:
        end = min(first + timedelta(days=days), last)
        result.append(
            {"start": first.isoformat(), "end": end.isoformat(), "id": first.strftime("%Y%m%d")}
        )
        first = end
    return result


def run_name():
    return os.environ.get("STREAM_RUN") or os.environ["FEATURE_EPOCH"]


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


def seed(start, end):
    import duckdb
    from pyiceberg.expressions import And, EqualTo, GreaterThanOrEqual, LessThan

    first, last = date.fromisoformat(start), date.fromisoformat(end)
    if first >= last:
        raise ValueError("history start must precede end")
    epoch = os.environ["FEATURE_EPOCH"]
    input_topic, clock_topic, *_ = topics(run_name())
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
    db.execute(f"SET memory_limit = '{os.environ.get('HISTORY_SORT_MEMORY', '1GB')}'")
    db.execute("SET temp_directory = '/tmp/history-sort'")
    db.register("bronze", scan)
    reader = db.sql(
        "SELECT * FROM bronze ORDER BY recorded_at, integration_id, payment_id, aggregate_version, event_id, _source_file, _kafka_offset"
    ).to_arrow_reader(10000)
    brokers = os.environ["KAFKA_BROKERS"]
    producer = Producer(
        {
            "bootstrap.servers": brokers,
            "enable.idempotence": True,
            "acks": "all",
            "queue.buffering.max.messages": 500000,
            "linger.ms": 50,
            "batch.size": 1048576,
            "compression.type": "zstd",
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
                record = {
                    "partition": 0,
                    "key": row["payment_id"].encode(),
                    "value": encode(row),
                    "timestamp": int(row["recorded_at"].replace(tzinfo=UTC).timestamp() * 1000),
                    "on_delivery": delivered,
                }
                while True:
                    try:
                        producer.produce(input_topic, **record)
                        break
                    except BufferError:
                        producer.poll(1)
                offset += 1
            producer.poll(0)
            if failures:
                raise RuntimeError("history Kafka delivery failed")
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
    parser.add_argument("mode", choices=("seed", "topics", "units"))
    parser.add_argument("--start", default=os.environ.get("START_DAY"))
    parser.add_argument("--end", default=os.environ.get("END_DAY"))
    parser.add_argument("--delete", action="store_true")
    parser.add_argument("--days", type=int, default=int(os.environ.get("HISTORY_UNIT_DAYS", "7")))
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
        admin = AdminClient({"bootstrap.servers": os.environ["KAFKA_BROKERS"]})
        existing = set(admin.list_topics(timeout=30).topics)
        names = topics(run_name())
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
            configs = {
                "retention.ms": "-1",
                "retention.bytes": "-1",
                "cleanup.policy": "delete",
                "compression.type": "zstd",
            }
            for name, future in admin.create_topics(
                [
                    NewTopic(
                        name,
                        num_partitions=1,
                        replication_factor=int(os.environ.get("HISTORY_REPLICATION_FACTOR", "1")),
                        config=configs,
                    )
                    for name in names
                ]
            ).items():
                future.result()
    else:
        planned = units(date.fromisoformat(args.start), date.fromisoformat(args.end), args.days)
        # The Flink Role names each unit's Deployment from the chart's history range and unit length.
        allowed = {
            unit["id"]
            for unit in units(
                date.fromisoformat(os.environ["HISTORY_RANGE_START"]),
                date.fromisoformat(os.environ["HISTORY_RANGE_END"]),
                args.days,
            )
        }
        if any(unit["id"] not in allowed for unit in planned):
            raise ValueError("history units must align with the configured history range")
        Path("/tmp/units").write_text(json.dumps(planned))


if __name__ == "__main__":
    main()
