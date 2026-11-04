"""Plan and seed bounded historical replay units with isolated topics and logical clocks."""

import argparse
import json
import os
import time
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from urllib.parse import urlsplit

from confluent_kafka import KafkaException, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic, OffsetSpec
from google.protobuf.json_format import ParseDict

from fraud_streaming.contracts import PaymentEvent


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


def report_store():
    location = urlsplit(os.environ["HISTORY_SEED_REPORT"])
    if location.scheme != "s3":
        return None, location
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
            },
            request_checksum_calculation="when_required",
        ),
    )
    return client, location


def write_report(report):
    client, location = report_store()
    body = json.dumps(report, sort_keys=True).encode()
    if client:
        client.put_object(Bucket=location.netloc, Key=location.path.lstrip("/"), Body=body)
    else:
        Path(os.environ["HISTORY_SEED_REPORT"]).write_bytes(body)


def read_report():
    client, location = report_store()
    try:
        if client:
            body = client.get_object(Bucket=location.netloc, Key=location.path.lstrip("/"))[
                "Body"
            ].read()
        else:
            body = Path(os.environ["HISTORY_SEED_REPORT"]).read_bytes()
    except FileNotFoundError:
        return None
    except Exception as error:
        if getattr(error, "response", {}).get("Error", {}).get("Code") in ("NoSuchKey", "404"):
            return None
        raise
    return json.loads(body)


# Bronze ordering for replay: event time, then the keys that make it total.
ORDER = "recorded_at, integration_id, payment_id, aggregate_version, event_id, _source_file, _kafka_offset"
# Columns the seed reads, where Bronze has them: the event fields it encodes and the provenance that orders duplicates.
PROVENANCE = ("_source_file", "_kafka_offset")


def days(first, last):
    while first < last:
        yield first, min(first + timedelta(days=1), last)
        first += timedelta(days=1)


def ordered_rows(db, table, snapshot, columns, start, end):
    """Bronze rows recorded in [start, end), in replay order. One day at a time keeps the sort and the decoded files
    to one day's events, so memory does not grow with the unit."""
    from pyiceberg.expressions import And, EqualTo, GreaterThanOrEqual, LessThan

    predicate = And(
        EqualTo("_dataset_version", os.environ["DATASET_VERSION"]),
        And(GreaterThanOrEqual("recorded_at", start), LessThan("recorded_at", end)),
    )
    scan = table.scan(row_filter=predicate, selected_fields=columns, snapshot_id=snapshot)
    db.register("bronze", scan.to_arrow_batch_reader())
    try:
        for batch in db.sql(f"SELECT * FROM bronze ORDER BY {ORDER}").to_arrow_reader(10000):
            yield from batch.to_pylist()
    finally:
        db.unregister("bronze")


def reset_topics(*, delete_only=False, keep_inputs=False):
    """Delete the run's topics, then recreate them empty unless delete_only. The seed calls it first, so a retried
    seed starts from empty topics and never appends a second copy of the unit. keep_inputs leaves the seeded input
    and clock topics and resets only what a Flink run writes."""
    admin = AdminClient({"bootstrap.servers": os.environ["KAFKA_BROKERS"]})
    existing = set(admin.list_topics(timeout=30).topics)
    names = topics(run_name())[2 if keep_inputs else 0 :]
    stale = [name for name in names if name in existing]
    if stale:
        for future in admin.delete_topics(stale, operation_timeout=30).values():
            future.result()
    if delete_only:
        return
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
    for future in admin.create_topics(
        [
            NewTopic(
                name,
                num_partitions=1,
                replication_factor=int(os.environ.get("HISTORY_REPLICATION_FACTOR", "1")),
                config=configs,
            )
            for name in names
        ]
    ).values():
        future.result()


def seeded(start, end, snapshot):
    """Whether a finished seed of this unit and Bronze snapshot is still in Kafka, untouched: the input and clock
    topics end at the offsets its report recorded."""
    report = read_report()
    if not report or not report.get("done"):
        return False
    if (report["start"], report["end"], report["epoch"], report["snapshot"]) != (
        start,
        end,
        os.environ["FEATURE_EPOCH"],
        snapshot,
    ):
        return False
    input_topic, clock_topic, *_ = topics(run_name())
    admin = AdminClient({"bootstrap.servers": os.environ["KAFKA_BROKERS"]})
    try:
        latest = admin.list_offsets(
            {TopicPartition(name, 0): OffsetSpec.latest() for name in (input_topic, clock_topic)},
            request_timeout=30,
        )
        ends = {partition.topic: future.result().offset for partition, future in latest.items()}
    except KafkaException:
        return False
    return ends[input_topic] == report["input_end"] and ends[clock_topic] == report["clock_end"]


def seed(start, end):
    import duckdb

    first, last = date.fromisoformat(start), date.fromisoformat(end)
    if first >= last:
        raise ValueError("history start must precede end")
    epoch = os.environ["FEATURE_EPOCH"]
    input_topic, clock_topic, *_ = topics(run_name())
    table = lake().load_table("bronze.payment_events")
    snapshot = table.current_snapshot().snapshot_id
    if seeded(start, end, snapshot):
        reset_topics(keep_inputs=True)
        return
    reset_topics()
    bootstrap = datetime.combine(first - timedelta(days=1), datetime.min.time(), UTC)
    until = datetime.combine(last, datetime.min.time(), UTC)
    wanted = {*PaymentEvent.DESCRIPTOR.fields_by_name, *PROVENANCE}
    columns = tuple(field.name for field in table.schema().fields if field.name in wanted)
    db = duckdb.connect()
    db.execute(f"SET memory_limit = '{os.environ.get('HISTORY_SORT_MEMORY', '1GB')}'")
    db.execute("SET temp_directory = '/tmp/history-sort'")
    db.execute("SET preserve_insertion_order = false")
    brokers = os.environ["KAFKA_BROKERS"]
    producer = Producer(
        {
            "bootstrap.servers": brokers,
            "enable.idempotence": True,
            "acks": "all",
            # Bounds the producer's own buffer; produce() blocks on BufferError until it drains.
            "queue.buffering.max.kbytes": 262144,
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
        if producer.flush(120) or failures:
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
        for day_start, day_end in days(bootstrap, until):
            for row in ordered_rows(db, table, snapshot, columns, day_start, day_end):
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
        reset_topics(delete_only=args.delete)
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
