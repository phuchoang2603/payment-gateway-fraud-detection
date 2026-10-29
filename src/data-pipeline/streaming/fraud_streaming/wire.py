import hashlib
from pathlib import Path

from google.protobuf import descriptor_pb2, descriptor_pool, message_factory

from fraud_streaming.model import DAY, HOUR, MINUTE, Fact

pool = descriptor_pool.DescriptorPool()
descriptors = descriptor_pb2.FileDescriptorSet.FromString(
    Path(__file__).with_name("contracts.pb").read_bytes()
)
for descriptor in descriptors.file:
    pool.Add(descriptor)
PaymentEvent = message_factory.GetMessageClass(
    pool.FindMessageTypeByName("payments.v1.PaymentEvent")
)
SnapshotUpdate = message_factory.GetMessageClass(
    pool.FindMessageTypeByName("features.v1.SnapshotUpdate")
)
DEFINITION_HASH = "958f9b0fd7e50ba33a22526271ff2e62eafcf827c9f9e0bce1c35c4a016f85a3"


def decode(raw):
    event = PaymentEvent.FromString(bytes(raw))
    if (
        event.schema_version != 3
        or not event.event_id
        or not event.integration_id
        or not event.payment_id
        or not event.aggregate_version
        or not event.HasField("occurred_at")
        or not event.HasField("recorded_at")
        or event.WhichOneof("payload") is None
    ):
        raise ValueError("invalid or unsupported payment event")
    for timestamp in (event.occurred_at, event.recorded_at):
        timestamp.ToDatetime()
    recorded_at = event.recorded_at.ToMicroseconds()
    if event.WhichOneof("payload") != "attempt_started":
        return None, recorded_at
    body = event.attempt_started
    if (
        not body.attempt_id
        or not body.context.customer.customer_id
        or not body.signals.payment_method.fingerprint
        or not body.signals.HasField("attempted_at")
        or (body.signals.HasField("device_fingerprint") and not body.signals.device_fingerprint)
    ):
        raise ValueError("invalid self-contained attempt context")
    body.signals.attempted_at.ToDatetime()
    return Fact(
        event_id=event.event_id,
        digest=hashlib.sha256(event.SerializeToString(deterministic=True)).hexdigest(),
        integration=event.integration_id,
        payment=event.payment_id,
        attempt=body.attempt_id,
        started_at=body.signals.attempted_at.ToMicroseconds(),
        recorded_at=recorded_at,
        customer=body.context.customer.customer_id,
        method=body.signals.payment_method.fingerprint,
        device=body.signals.device_fingerprint
        if body.signals.HasField("device_fingerprint")
        else None,
    ), recorded_at


def snapshot(entity, quantities, *, epoch, now):
    coverage, values = quantities
    update = SnapshotUpdate(
        schema_version=1,
        definition={"version": "fraud-snapshot-v2", "sha256": DEFINITION_HASH},
        epoch=epoch,
        view=1 if entity.view == "customer" else 2,
        integration_id=entity.integration,
        entity_id=entity.entity,
        revision=entity.revision,
    )
    for field, instant in (
        ("anchor", now),
        ("source_available_at", entity.available_at),
        ("online_write_at", entity.write_at),
    ):
        getattr(update, field).FromMicroseconds(instant)
    for window, name in (
        (5 * MINUTE, "five_minutes"),
        (HOUR, "one_hour"),
        (DAY, "twenty_four_hours"),
    ):
        setattr(update.coverage, name, 2 if coverage[window] else 1)
    update.values.SetInParent()
    for name, value in values.items():
        setattr(update.values, name, value)
    for progress in entity.progress:
        row = update.source_progress.add(
            topic=progress.topic,
            partition=progress.partition,
            consumed_offset=progress.consumed,
            observed_high_watermark=progress.high_watermark,
        )
        row.observed_at.FromMicroseconds(progress.observed_at)
    update.snapshot_id = hashlib.sha256(update.SerializeToString(deterministic=True)).hexdigest()
    return update.SerializeToString(deterministic=True)
