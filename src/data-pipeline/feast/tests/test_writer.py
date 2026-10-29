import base64
import secrets
import threading
from datetime import UTC, datetime, timedelta

import pytest
from feast import FeatureStore
from feast.feature_server_utils import convert_response_to_dict
from feast.repo_config import RepoConfig
from testcontainers.community.redis import RedisContainer

from fraud_feast import schema
from fraud_feast.contracts.features.v1 import features_pb2 as wire
from fraud_feast.writer import Writer, consume

EPOCH = "ci-replay"
DEFINITION_HASH = "8ca9a423f452f8ed7a8d1e8aa082dadb72590498cfd1e8da48f94f6c06b0e50d"


@pytest.fixture(scope="session")
def redis_address():
    password = secrets.token_hex(16)
    container = (
        RedisContainer("quay.io/opstree/redis:v7.2.16", password=password)
        .with_kwargs(entrypoint="redis-server")
        .with_command(
            [
                "--save",
                "",
                "--appendonly",
                "no",
                "--requirepass",
                password,
            ]
        )
    )
    with container:
        host = container.get_container_host_ip()
        port = container.get_exposed_port(6379)
        yield f"{host}:{port},password={password}"


@pytest.fixture
def store(redis_address, tmp_path):
    result = FeatureStore(
        config=RepoConfig(
            project="fraud_snapshots",
            provider="local",
            registry=str(tmp_path / "registry.db"),
            online_store={"type": "redis", "connection_string": redis_address},
            entity_key_serialization_version=3,
        )
    )
    result.apply(schema.definitions(), partial=True)
    return result


def update(*, view=wire.ENTITY_VIEW_CUSTOMER, revision=1, write_delta=0, unknown=False):
    anchor = datetime(2025, 9, 28, tzinfo=UTC)
    value = wire.SnapshotUpdate(
        schema_version=1,
        definition=wire.DefinitionIdentity(version="fraud-snapshot-v1", sha256=DEFINITION_HASH),
        epoch=EPOCH,
        view=view,
        integration_id="integration-π",
        entity_id=secrets.token_hex(8),
        revision=revision,
        coverage=wire.WindowCoverage(
            one_hour=wire.COVERAGE_UNKNOWN if unknown else wire.COVERAGE_COMPLETE,
            twenty_four_hours=wire.COVERAGE_COMPLETE,
            seven_days=wire.COVERAGE_COMPLETE,
        ),
        values=wire.SnapshotValues(assessed_count_24h=4),
    )
    if not unknown:
        value.values.assessed_count_1h = 0
    if view == wire.ENTITY_VIEW_CUSTOMER:
        value.values.assessed_amount_minor_24h = 2**53 + 1
        value.values.distinct_devices_7d = 2
        value.values.last_assessed_start_at.FromDatetime(anchor - timedelta(hours=1))
    for name in ("anchor", "computed_at", "published_at", "source_available_at"):
        getattr(value, name).FromDatetime(anchor)
    value.online_write_at.FromDatetime(anchor + timedelta(microseconds=write_delta))
    progress = value.source_progress.add(
        topic="payments.events.v1", partition=0, consumed_offset=3, observed_high_watermark=4
    )
    progress.observed_at.FromDatetime(anchor)
    empty = value.source_progress.add(
        topic="payments.events.v1", partition=1, consumed_offset=-1, observed_high_watermark=0
    )
    empty.observed_at.FromDatetime(anchor)
    value.snapshot_id = schema.snapshot_id(value)
    return value


def write(writer, value):
    view = schema.VIEWS[value.view]
    key = schema.entity_key(value.epoch, view, value.integration_id, value.entity_id).encode()
    writer.write(value.SerializeToString(deterministic=True), key)


def test_sdk_readback_replay_and_monotonic_timestamps(store):
    writer = Writer(store, epoch=EPOCH, definition_hash=DEFINITION_HASH)
    original = update(unknown=True)
    write(writer, original)
    assert writer.current(original, "customer") == original
    response = store.get_online_features(
        features=[
            f"customer_snapshot:{name}"
            for name in (*schema.FIELDS["customer"], "snapshot_metadata")
        ],
        entity_rows=[
            {
                "snapshot_key": schema.entity_key(
                    EPOCH, "customer", original.integration_id, original.entity_id
                )
            }
        ],
        full_feature_names=True,
    )
    served = convert_response_to_dict(response.proto)
    columns = dict(zip(served["metadata"]["feature_names"], served["results"], strict=True))
    assert columns["customer_snapshot__assessed_count_1h"]["values"] == [None]
    assert columns["customer_snapshot__assessed_amount_minor_24h"]["values"] == [2**53 + 1]
    packet = base64.b64decode(columns["customer_snapshot__snapshot_metadata"]["values"][0])
    metadata = wire.SnapshotUpdate.FromString(packet)
    assert metadata.snapshot_id == original.snapshot_id and not metadata.HasField("values")
    corrected = wire.SnapshotUpdate()
    corrected.CopyFrom(original)
    corrected.revision = 2
    corrected.online_write_at.nanos += 1000
    corrected.values.assessed_count_24h = 5
    corrected.snapshot_id = schema.snapshot_id(corrected)
    write(writer, corrected)
    restarted = Writer(store, epoch=EPOCH, definition_hash=DEFINITION_HASH)
    write(restarted, original)
    write(restarted, corrected)
    assert restarted.current(original, "customer") == corrected
    store.write_to_online_store("customer_snapshot", schema.row(original))
    equal_time = wire.SnapshotUpdate()
    equal_time.CopyFrom(corrected)
    equal_time.values.assessed_count_24h = 99
    equal_time.snapshot_id = schema.snapshot_id(equal_time)
    store.write_to_online_store("customer_snapshot", schema.row(equal_time))
    assert restarted.current(original, "customer") == corrected
    with pytest.raises(ValueError, match="conflicting snapshot revision"):
        write(restarted, equal_time)
    regressed = wire.SnapshotUpdate()
    regressed.CopyFrom(corrected)
    regressed.revision = 3
    regressed.online_write_at.CopyFrom(original.online_write_at)
    regressed.snapshot_id = schema.snapshot_id(regressed)
    with pytest.raises(ValueError, match="must increase"):
        write(restarted, regressed)


def test_sdk_entity_and_epoch_isolation(store):
    writer = Writer(store, epoch=EPOCH, definition_hash=DEFINITION_HASH)
    customer = update()
    method = update(view=wire.ENTITY_VIEW_METHOD)
    method.entity_id = customer.entity_id
    method.snapshot_id = schema.snapshot_id(method)
    write(writer, customer)
    write(writer, method)
    other_integration = wire.SnapshotUpdate()
    other_integration.CopyFrom(customer)
    other_integration.integration_id = "other"
    other_integration.values.assessed_count_24h = 8
    other_integration.snapshot_id = schema.snapshot_id(other_integration)
    write(writer, other_integration)
    other_epoch = wire.SnapshotUpdate()
    other_epoch.CopyFrom(customer)
    other_epoch.epoch = "live-v1"
    other_epoch.snapshot_id = schema.snapshot_id(other_epoch)
    live_writer = Writer(store, epoch="live-v1", definition_hash=DEFINITION_HASH)
    write(live_writer, other_epoch)
    assert writer.current(customer, "customer") == customer
    assert writer.current(method, "method") == method
    assert writer.current(other_integration, "customer") == other_integration
    assert live_writer.current(other_epoch, "customer") == other_epoch
    with pytest.raises(ValueError, match="incompatible snapshot identity"):
        write(writer, other_epoch)
    incompatible = wire.SnapshotUpdate()
    incompatible.CopyFrom(customer)
    incompatible.definition.sha256 = "0" * 64
    incompatible.revision += 1
    incompatible.online_write_at.nanos += 1000
    incompatible.snapshot_id = schema.snapshot_id(incompatible)
    with pytest.raises(ValueError, match="incompatible snapshot identity"):
        write(writer, incompatible)
    incompatible.CopyFrom(customer)
    incompatible.schema_version = 2
    incompatible.snapshot_id = schema.snapshot_id(incompatible)
    with pytest.raises(ValueError, match="incompatible snapshot identity"):
        write(writer, incompatible)
    assert writer.current(customer, "customer") == customer


def test_response_loss_replays_before_committing_kafka(store):
    value = update()
    stopped = threading.Event()

    class Message:
        def value(self):
            return value.SerializeToString(deterministic=True)

        def key(self):
            return schema.entity_key(
                EPOCH, "customer", value.integration_id, value.entity_id
            ).encode()

        def error(self):
            return None

    class Consumer:
        closed = False
        committed = False

        def poll(self, _):
            return Message()

        def commit(self, *, message, asynchronous):
            assert not asynchronous
            assert writer.current(value, "customer") == value
            if not response_lost.is_set():
                response_lost.set()
                raise RuntimeError("lost commit response")
            self.committed = True
            stopped.set()

        def close(self):
            self.closed = True

    response_lost = threading.Event()
    writer = Writer(store, epoch=EPOCH, definition_hash=DEFINITION_HASH)
    first = Consumer()
    with pytest.raises(RuntimeError, match="lost commit response"):
        consume(first, writer, stopped)
    assert first.closed and not first.committed
    second = Consumer()
    consume(second, Writer(store, epoch=EPOCH, definition_hash=DEFINITION_HASH), stopped)
    assert second.closed and second.committed
