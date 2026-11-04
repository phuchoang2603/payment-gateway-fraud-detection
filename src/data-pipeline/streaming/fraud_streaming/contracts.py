from pathlib import Path

from google.protobuf import descriptor_pb2, descriptor_pool, message_factory

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
    pool.FindMessageTypeByName("features.v2.SnapshotUpdate")
)
