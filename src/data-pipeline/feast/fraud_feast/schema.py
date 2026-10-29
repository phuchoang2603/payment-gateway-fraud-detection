import hashlib
from datetime import UTC, timedelta

import pandas as pd
from feast import Entity, FeatureView, Field, FileSource, ValueType
from feast.types import Bytes, Int64

from fraud_feast.contracts.features.v1 import features_pb2 as wire

COUNTS = ("attempt_count_5m", "attempt_count_1h", "attempt_count_24h")
FIELDS = {"customer": (*COUNTS, "distinct_devices_24h"), "method": COUNTS}
VIEWS = {wire.ENTITY_VIEW_CUSTOMER: "customer", wire.ENTITY_VIEW_METHOD: "method"}


def entity_key(epoch, view, integration, entity):
    digest = hashlib.sha256()
    for value in (epoch, view, integration, entity):
        encoded = value.encode()
        digest.update(str(len(encoded)).encode() + b":" + encoded)
    return digest.hexdigest()


def snapshot_id(update):
    identity = wire.SnapshotUpdate()
    identity.CopyFrom(update)
    identity.ClearField("snapshot_id")
    return hashlib.sha256(identity.SerializeToString(deterministic=True)).hexdigest()


def definitions():
    entity = Entity(name="snapshot", join_keys=["snapshot_key"], value_type=ValueType.STRING)
    views = [
        FeatureView(
            name=f"{view}_snapshot",
            entities=[entity],
            ttl=timedelta(0),
            schema=[Field(name=name, dtype=Int64) for name in fields]
            + [Field(name="snapshot_metadata", dtype=Bytes)],
            source=FileSource(path=f"{view}.parquet", timestamp_field="online_write_at"),
            online=True,
        )
        for view, fields in FIELDS.items()
    ]
    return [entity, *views]


def validate(update, *, epoch, definition_hash):
    if (
        update.schema_version != 1
        or update.definition.version != "fraud-snapshot-v2"
        or update.definition.sha256 != definition_hash
        or update.epoch != epoch
        or update.view not in VIEWS
        or not update.integration_id
        or not update.entity_id
        or not update.revision
        or not update.HasField("coverage")
        or not update.HasField("values")
        or update.snapshot_id != snapshot_id(update)
    ):
        raise ValueError("incompatible snapshot identity")
    view = VIEWS[update.view]
    times = {}
    for name in ("anchor", "source_available_at", "online_write_at"):
        timestamp = getattr(update, name)
        if not update.HasField(name) or timestamp.nanos % 1000:
            raise ValueError("snapshot requires UTC microsecond timestamps")
        times[name] = timestamp.ToDatetime(tzinfo=UTC)
    if times["source_available_at"] > times["anchor"]:
        raise ValueError("inconsistent snapshot timestamps")
    partitions = set()
    if not update.source_progress:
        raise ValueError("missing measured source progress")
    for progress in update.source_progress:
        identity = (progress.topic, progress.partition)
        if (
            not progress.topic
            or progress.partition < 0
            or progress.consumed_offset < -1
            or progress.observed_high_watermark <= progress.consumed_offset
            or not progress.HasField("observed_at")
            or progress.observed_at.ToDatetime(tzinfo=UTC) > times["anchor"]
            or identity in partitions
            or (
                progress.HasField("heartbeat_at")
                and progress.heartbeat_at.ToMicroseconds() > progress.observed_at.ToMicroseconds()
            )
        ):
            raise ValueError("invalid source progress")
        partitions.add(identity)
    windows = {
        "five_minutes": ["attempt_count_5m"],
        "one_hour": ["attempt_count_1h"],
        "twenty_four_hours": ["attempt_count_24h"],
    }
    if view == "customer":
        windows["twenty_four_hours"].append("distinct_devices_24h")
    elif update.values.HasField("distinct_devices_24h"):
        raise ValueError("method view contains customer quantities")
    for window, fields in windows.items():
        coverage = getattr(update.coverage, window)
        if coverage not in (wire.COVERAGE_UNKNOWN, wire.COVERAGE_COMPLETE):
            raise ValueError("unknown coverage code")
        for name in fields:
            present = update.values.HasField(name)
            if present != (coverage == wire.COVERAGE_COMPLETE):
                raise ValueError("snapshot value conflicts with coverage")
            if present and getattr(update.values, name) < 0:
                raise ValueError("negative snapshot quantity")
    return view


def row(update):
    view = VIEWS[update.view]
    metadata = wire.SnapshotUpdate()
    metadata.CopyFrom(update)
    metadata.ClearField("values")
    values = {}
    for name in FIELDS[view]:
        value = getattr(update.values, name) if update.values.HasField(name) else None
        values[name] = pd.Series([value], dtype="Int64")
    return pd.DataFrame(
        {
            "snapshot_key": [
                entity_key(update.epoch, view, update.integration_id, update.entity_id)
            ],
            "online_write_at": [update.online_write_at.ToDatetime(tzinfo=UTC)],
            "snapshot_metadata": [metadata.SerializeToString(deterministic=True)],
            **values,
        }
    )


def restore(metadata, values):
    update = wire.SnapshotUpdate.FromString(metadata)
    if update.HasField("values"):
        raise ValueError("metadata duplicates typed quantities")
    update.values.SetInParent()
    for name in FIELDS[VIEWS[update.view]]:
        value = values[name]
        if value is not None:
            setattr(update.values, name, int(value))
    return update
