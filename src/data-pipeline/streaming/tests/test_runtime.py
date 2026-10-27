import sys
from dataclasses import replace

from pyflink.common import Configuration, Types
from pyflink.datastream import StreamExecutionEnvironment

from fraud_streaming.job import build_updates
from fraud_streaming.model import DAY, HOUR, Fact, Packet, Progress, entity_key
from fraud_streaming.wire import SnapshotUpdate


def test_keyed_runtime_matching_shared_method_and_idle_expiration():
    anchor = 1_760_000_000_000_000
    configuration = Configuration()
    configuration.set_string("python.executable", sys.executable)
    configuration.set_string("python.client.executable", sys.executable)
    configuration.set_string("taskmanager.memory.process.size", "2048m")
    env = StreamExecutionEnvironment.get_execution_environment(configuration)
    env.set_parallelism(1)
    first = Fact(
        "start-one",
        "one",
        "integration",
        "payment-one",
        "one",
        "start",
        anchor - HOUR,
        anchor,
        "customer-one",
        "shared-method",
        100,
        "device",
    )
    second = replace(
        first,
        event_id="start-two",
        digest="two",
        payment="payment-two",
        attempt="two",
        customer="customer-two",
        started_at=anchor - HOUR + 1000,
    )
    progress = (Progress("isolated-source", 0, 3, 4, anchor),)
    packets = []
    for start in (first, second):
        decision = replace(
            start,
            event_id=f"decision-{start.attempt}",
            digest=f"decision-{start.attempt}",
            kind="decision",
            started_at=anchor,
        )
        for fact in (decision, start, start):
            packets.append(Packet(fact, anchor, anchor - 8 * DAY, progress))
    updates = build_updates(
        env.from_collection(packets, type_info=Types.PICKLED_BYTE_ARRAY()),
        epoch="runtime-ci",
        logical=True,
        max_entries=10,
    )
    with updates.execute_and_collect() as results:
        rows = list(results)
    snapshots = [SnapshotUpdate.FromString(bytes(row.payload)) for row in rows]
    assert {value.entity_id for value in snapshots} == {
        "customer-one",
        "customer-two",
        "shared-method",
    }
    for key, payload in rows:
        value = SnapshotUpdate.FromString(bytes(payload))
        view = "customer" if value.view == 1 else "method"
        assert bytes(key).decode() == entity_key(
            "runtime-ci",
            view,
            value.integration_id,
            value.entity_id,
        )
    for entity in ("customer-one", "customer-two", "shared-method"):
        history = [value for value in snapshots if value.entity_id == entity]
        assert [value.revision for value in history] == list(range(1, len(history) + 1))
        assert all(value.coverage.one_hour == 2 for value in history)
        assert history[0].values.assessed_count_1h == 1
        assert max(value.values.assessed_count_24h for value in history) == (
            2 if entity == "shared-method" else 1
        )
        assert history[-1].values.assessed_count_24h == 0
        if entity.startswith("customer"):
            assert history[0].values.distinct_devices_7d == 1
            assert history[-1].values.distinct_devices_7d == 0
            assert not history[-1].values.HasField("last_assessed_start_at")
