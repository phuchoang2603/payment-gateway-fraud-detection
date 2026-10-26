import json
import os
import re
import signal
import time
from threading import Event

import boto3
from botocore.config import Config
from confluent_kafka import Consumer, TopicPartition

from simulator.landing.archive import LandingStore, publish
from simulator.landing.parquet import Record


def run_archiver(
    consumer, store, topics, label_topic, stop, *, max_records=10000, max_bytes=32 * 1024**2, flush_seconds=10
):
    records = []
    size = 0
    closing = False
    deadline = time.monotonic() + flush_seconds

    def revoked(client, assignment):
        if records and not closing:
            raise RuntimeError("landing assignment revoked with unfinished work; replay required")

    def flush():
        nonlocal size, deadline
        if records:
            manifests = publish(store, records, label_topic)
            offsets = {}
            for record in records:
                offsets[record.topic, record.partition] = record.offset + 1
            committed = consumer.commit(
                offsets=[
                    TopicPartition(topic, partition, offset) for (topic, partition), offset in sorted(offsets.items())
                ],
                asynchronous=False,
            )
            if any(partition.error for partition in committed):
                raise RuntimeError("landing offset commit failed after publication; replay possible")
            print(
                json.dumps(
                    {
                        "event": "landing_batch_committed",
                        "records": len(records),
                        "source_bytes": size,
                        "completed_manifests": manifests,
                        "recovery": "at-least-once",
                    }
                ),
                flush=True,
            )
            records.clear()
            size = 0
        deadline = time.monotonic() + flush_seconds

    try:
        consumer.subscribe(topics, on_revoke=revoked, on_lost=revoked)
        while not stop.is_set():
            if time.monotonic() >= deadline:
                flush()
            message = consumer.poll(min(0.25, max(0, deadline - time.monotonic())))
            if message is None:
                continue
            if message.error():
                raise RuntimeError("landing consumer failed")
            record = Record.from_message(message)
            if record.size > max_bytes:
                raise ValueError("record exceeds landing byte capacity; no offsets committed")
            if size + record.size > max_bytes:
                flush()
            records.append(record)
            size += record.size
            if len(records) >= max_records or size >= max_bytes:
                flush()
        flush()
    finally:
        closing = True
        consumer.close()
        print(
            json.dumps(
                {
                    "event": "landing_stop",
                    "pending_records": len(records),
                    "pending_bytes": size,
                    "recovery": "uncommitted records replay; overlapping batches possible",
                }
            ),
            flush=True,
        )


def main():
    max_records = int(os.environ.get("LANDING_MAX_RECORDS", "10000"))
    max_bytes = int(os.environ.get("LANDING_MAX_BYTES", str(32 * 1024**2)))
    interval = os.environ.get("LANDING_FLUSH_INTERVAL", "10s")
    match = re.fullmatch(r"([0-9]+(?:\.[0-9]+)?)(ms|s|m)", interval)
    if not match:
        raise ValueError("invalid landing flush interval")
    flush_seconds = float(match[1]) * {"ms": 0.001, "s": 1, "m": 60}[match[2]]
    if not 0 < max_records <= 1_000_000 or not 0 < max_bytes <= 256 * 1024**2 or not 0 < flush_seconds <= 60:
        raise ValueError("invalid landing capacity")
    topics = os.environ.get("LANDING_TOPICS", "payments.events.v1,fraud.labels.v1").split(",")
    if any(not re.fullmatch(r"[A-Za-z0-9_.-]+", topic) or topic in (".", "..") for topic in topics):
        raise ValueError("invalid landing topic")
    if not os.environ.get("AWS_ACCESS_KEY_ID") or not os.environ.get("AWS_SECRET_ACCESS_KEY"):
        raise ValueError("missing object store credentials")
    client = boto3.client(
        "s3",
        endpoint_url=os.environ.get("S3_ENDPOINT", "https://minio.home.phuchoang.sbs"),
        region_name=os.environ.get("AWS_REGION", "us-east-1"),
        config=Config(
            s3={"addressing_style": "path"},
            connect_timeout=15,
            read_timeout=120,
            retries={"max_attempts": 3},
            request_checksum_calculation="when_required",
        ),
    )
    consumer = Consumer(
        {
            "bootstrap.servers": os.environ.get(
                "KAFKA_BROKERS", "payment-gateway-kafka-kafka-bootstrap.payment-gateway.svc:9092"
            ),
            "group.id": os.environ.get("LANDING_GROUP", "payments-parquet-landing-v1"),
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "queued.max.messages.kbytes": max(1, max_bytes // 1024),
            "fetch.message.max.bytes": max_bytes,
        }
    )
    stop = Event()
    previous = {sig: signal.signal(sig, lambda *_: stop.set()) for sig in (signal.SIGINT, signal.SIGTERM)}
    try:
        run_archiver(
            consumer,
            LandingStore(
                client, os.environ.get("LANDING_BUCKET", "synthetic-source"), os.environ["LANDING_SOURCE_EPOCH"]
            ),
            topics,
            os.environ.get("LABEL_TOPIC", "fraud.labels.v1"),
            stop,
            max_records=max_records,
            max_bytes=max_bytes,
            flush_seconds=flush_seconds,
        )
    finally:
        for sig, handler in previous.items():
            signal.signal(sig, handler)
