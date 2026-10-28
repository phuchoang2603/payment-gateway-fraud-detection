import json
from dataclasses import replace
from threading import Event
from unittest.mock import Mock

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from botocore.exceptions import ClientError

from simulator.contracts.payments.v1.events_pb2 import PaymentEvent
from simulator.landing.archive import FORMAT, LandingStore, digest, publish
from simulator.landing.parquet import Record, decode
from simulator.landing.runtime import run_archiver


class MemoryStore:
    source_epoch = "smoke-run"

    def __init__(self):
        self.objects = {}
        self.order = []
        self.fail_manifest = False

    def create(self, key, body):
        if self.fail_manifest and key.endswith(".json"):
            raise OSError("interrupted manifest publication")
        if key in self.objects:
            assert self.objects[key] == body
        self.objects[key] = body
        self.order.append(key)


def record(offset=1):
    event = PaymentEvent(
        event_id="same-event", payment_id="p", integration_id="i", aggregate_version=1, schema_version=3
    )
    event.occurred_at.FromJsonString("2026-07-01T00:00:00Z")
    event.recorded_at.CopyFrom(event.occurred_at)
    event.payment_expired.present = True
    return Record("events", 0, offset, 1234, b"", event.SerializeToString(), [("h", None), ("h", b"")])


def test_manifest_last_preserves_deliveries_and_replay_identity():
    store = MemoryStore()
    records = [record(1), record(2)]
    manifests = publish(store, records, "labels")
    assert store.order[-1] == manifests[0]
    manifest = json.loads(store.objects[manifests[0]])
    assert (manifest["rows"], manifest["first_offset"], manifest["last_offset"]) == (2, 1, 2)
    assert manifest["source_epoch"] == "smoke-run"
    assert "/epoch=smoke-run/" in manifest["files"][0]["key"]
    body = store.objects[manifest["files"][0]["key"]]
    rows = pq.read_table(pa.BufferReader(body)).to_pylist()
    assert [row["event_id"] for row in rows] == ["same-event", "same-event"]
    assert [row["_transport"]["offset"] for row in rows] == [1, 2]
    original = store.objects.copy()
    assert publish(store, records, "labels") == manifests
    assert store.objects == original


@pytest.mark.parametrize("raw", [None, b"", b"\xff"])
def test_malformed_record_cannot_publish(raw):
    store = MemoryStore()
    with pytest.raises(Exception):
        publish(store, [replace(record(), value=raw)], "labels")
    assert not store.objects


def test_unsupported_schema_and_missing_context_refused():
    source = record()
    event = PaymentEvent.FromString(source.value)
    event.schema_version = 2
    with pytest.raises(ValueError, match="unsupported"):
        decode(replace(source, value=event.SerializeToString()), "labels")
    event.schema_version = 3
    event.ClearField("payment_expired")
    event.attempt_started.attempt_id = "a"
    event.attempt_started.signals.user_agent_family = "browser"
    with pytest.raises(ValueError, match="scoring context"):
        decode(replace(source, value=event.SerializeToString()), "labels")


def test_conditional_create_replay_requires_matching_metadata():
    client = Mock()
    body = b"immutable"
    client.head_object.return_value = {
        "ContentLength": len(body),
        "Metadata": {"Sha256": digest(body), "Landing-Format": FORMAT},
    }
    store = LandingStore(client, "s3://platform-dev/datasets/payment-gateway", "smoke-run")
    store.create("streaming/file", body)
    assert client.put_object.call_args.kwargs["IfNoneMatch"] == "*"
    assert client.put_object.call_args.kwargs["Bucket"] == "platform-dev"
    assert client.put_object.call_args.kwargs["Key"] == "datasets/payment-gateway/streaming/file"
    client.put_object.side_effect = ClientError({"Error": {"Code": "PreconditionFailed"}}, "PutObject")
    store.create("streaming/file", body)
    client.head_object.return_value["Metadata"]["Sha256"] = "different"
    with pytest.raises(RuntimeError, match="collision"):
        store.create("streaming/file", body)
    with pytest.raises(ValueError, match="outside"):
        store.create("historical/file", body)


def consumer_for(source, stop):
    message = Mock()
    message.error.return_value = None
    message.topic.return_value = source.topic
    message.partition.return_value = source.partition
    message.offset.return_value = source.offset
    message.timestamp.return_value = (1, source.timestamp_ms)
    message.key.return_value = source.key
    message.value.return_value = source.value
    message.headers.return_value = source.headers
    consumer = Mock()
    consumer.poll.side_effect = [message, None]

    def commit(**kwargs):
        stop.set()
        return kwargs["offsets"]

    consumer.commit.side_effect = commit
    return consumer


def test_publication_failure_does_not_commit_and_restart_reuses_files():
    store = MemoryStore()
    store.fail_manifest = True
    stop = Event()
    consumer = consumer_for(record(), stop)
    with pytest.raises(OSError, match="manifest"):
        run_archiver(consumer, store, ["events"], "labels", stop, max_records=1)
    consumer.commit.assert_not_called()
    consumer.close.assert_called_once()
    partial = store.objects.copy()
    store.fail_manifest = False
    restarted = consumer_for(record(), stop)
    run_archiver(restarted, store, ["events"], "labels", stop, max_records=1)
    assert all(store.objects[key] == body for key, body in partial.items())
    offset = restarted.commit.call_args.kwargs["offsets"][0]
    assert (offset.topic, offset.partition, offset.offset) == ("events", 0, 2)
    assert store.order[-1].endswith(".json")


def test_low_volume_flushes_on_deadline(monkeypatch):
    clock = [0.0]
    monkeypatch.setattr("simulator.landing.runtime.time.monotonic", lambda: clock[0])
    stop = Event()
    consumer = consumer_for(record(), stop)
    message = next(consumer.poll.side_effect)
    deliveries = [message]

    def poll(timeout):
        clock[0] += timeout
        return deliveries.pop() if deliveries else None

    consumer.poll.side_effect = poll
    run_archiver(consumer, MemoryStore(), ["events"], "labels", stop, flush_seconds=1)
    consumer.commit.assert_called_once()
    assert clock[0] >= 1


def test_rebalance_with_buffered_record_aborts_without_commit():
    stop = Event()
    consumer = consumer_for(record(), stop)
    message = next(consumer.poll.side_effect)
    pending = [message]

    def poll(timeout):
        if pending:
            return pending.pop()
        consumer.subscribe.call_args.kwargs["on_revoke"](consumer, [])

    consumer.poll.side_effect = poll
    with pytest.raises(RuntimeError, match="revoked"):
        run_archiver(consumer, MemoryStore(), ["events"], "labels", stop)
    consumer.commit.assert_not_called()
    consumer.close.assert_called_once()
