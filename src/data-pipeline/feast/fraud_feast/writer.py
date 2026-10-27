from fraud_feast import schema
from fraud_feast.contracts.features.v1 import features_pb2 as wire


class Writer:
    def __init__(self, store, *, epoch, definition_hash):
        self.store = store
        self.epoch = epoch
        self.definition_hash = definition_hash

    def current(self, update, view):
        fields = (*schema.FIELDS[view], "snapshot_metadata")
        values = self.store.get_online_features(
            features=[f"{view}_snapshot:{name}" for name in fields],
            entity_rows=[
                {
                    "snapshot_key": schema.entity_key(
                        update.epoch, view, update.integration_id, update.entity_id
                    )
                }
            ],
            full_feature_names=True,
        ).to_dict()
        current = {name: values[f"{view}_snapshot__{name}"][0] for name in fields}
        if current["snapshot_metadata"] is None:
            if any(current[name] is not None for name in schema.FIELDS[view]):
                raise ValueError("incomplete stored snapshot")
            return None
        observed = schema.restore(current.pop("snapshot_metadata"), current)
        schema.validate(observed, epoch=self.epoch, definition_hash=self.definition_hash)
        if (
            observed.view != update.view
            or observed.entity_id != update.entity_id
            or observed.integration_id != update.integration_id
        ):
            raise ValueError("stored snapshot identity mismatch")
        return observed

    def write(self, content, key):
        update = wire.SnapshotUpdate.FromString(content)
        view = schema.validate(update, epoch=self.epoch, definition_hash=self.definition_hash)
        if (
            key
            != schema.entity_key(
                update.epoch, view, update.integration_id, update.entity_id
            ).encode()
        ):
            raise ValueError("feature journal key does not match snapshot")
        previous = self.current(update, view)
        if previous is not None:
            if previous.revision == update.revision:
                if previous.snapshot_id != update.snapshot_id:
                    raise ValueError("conflicting snapshot revision")
                return
            if previous.revision > update.revision:
                if (
                    previous.online_write_at.ToMicroseconds()
                    <= update.online_write_at.ToMicroseconds()
                ):
                    raise ValueError("snapshot revision/write timestamp regression")
                return
            if previous.online_write_at.ToMicroseconds() >= update.online_write_at.ToMicroseconds():
                raise ValueError("snapshot write timestamp must increase")
        self.store.write_to_online_store(f"{view}_snapshot", schema.row(update))
        stored = self.current(update, view)
        if stored is None or stored.snapshot_id != update.snapshot_id:
            raise RuntimeError("Feast did not retain the submitted snapshot")


def consume(consumer, writer, stopped):
    try:
        while not stopped.is_set():
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                raise RuntimeError("feature journal consumer failed")
            if message.value() is None or len(message.value()) > 128 * 1024:
                raise ValueError("invalid feature journal payload size")
            writer.write(message.value(), message.key())
            consumer.commit(message=message, asynchronous=False)
    finally:
        consumer.close()
