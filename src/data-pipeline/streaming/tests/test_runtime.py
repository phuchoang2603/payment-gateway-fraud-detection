import sys
from concurrent.futures import Future
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from confluent_kafka import KafkaError, KafkaException
from pyflink.common import Configuration
from pyflink.datastream import StreamExecutionEnvironment

from fraud_streaming import operators
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
        env.from_collection(packets),
        epoch="runtime-ci",
        logical=True,
        max_entries=10,
    )
    with updates.execute_and_collect() as results:
        # The collection bridge exposes Java byte arrays as signed integers.
        rows = [tuple(bytes(value & 0xFF for value in field) for field in row) for row in results]
    snapshots = []
    for key, payload in rows:
        value = SnapshotUpdate.FromString(payload)
        snapshots.append(value)
        view = "customer" if value.view == 1 else "method"
        assert key.decode() == entity_key(
            "runtime-ci",
            view,
            value.integration_id,
            value.entity_id,
        )
    assert {value.entity_id for value in snapshots} == {
        "customer-one",
        "customer-two",
        "shared-method",
    }
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


def test_source_progress_refreshes_crossed_offsets_and_rejects_partial_broker_results(monkeypatch):
    anchor = 1_760_000_000_000_000
    state = Mock()
    state.value.return_value = None
    state.update.side_effect = lambda value: setattr(state.value, "return_value", value)
    admin = Mock()
    admin.list_topics.return_value.topics = {
        "source": SimpleNamespace(error=None, partitions={0: None, 1: None, 2: None})
    }
    high = [4, 8, 12]
    leader_error = False

    def offsets(partitions, **kwargs):
        nonlocal leader_error
        results = {}
        for partition in partitions:
            result = Future()
            if leader_error and partition.partition == 0:
                result.set_exception(KafkaException(KafkaError(KafkaError.FENCED_LEADER_EPOCH)))
                leader_error = False
            elif high[partition.partition] is None:
                result.set_exception(RuntimeError("broker unavailable"))
            else:
                result.set_result(SimpleNamespace(offset=high[partition.partition]))
            results[partition] = result
        return results

    admin.list_offsets.side_effect = offsets
    monkeypatch.setattr(operators, "AdminClient", lambda config: admin)
    monkeypatch.setattr(operators, "decode", lambda raw: (None, anchor))
    monkeypatch.setattr(operators.time, "monotonic", lambda: 1)
    observer = operators.ObserveSource(brokers="broker", topic="source", partitions=3, logical=True)
    observer.open(SimpleNamespace(get_state=lambda descriptor: state))
    first = next(observer.process_element((b"", "source", 0, 3), None))
    assert [(p.partition, p.consumed, p.high_watermark) for p in first.progress] == [
        (0, 3, 4),
        (1, -1, 8),
        (2, -1, 12),
    ]
    assert first.bootstrap == first.now == anchor
    high[0] = 6
    next(observer.process_element((b"", "source", 0, 4), None))
    cached = next(observer.process_element((b"", "source", 0, 5), None))
    assert cached.progress[0].high_watermark == 6
    assert admin.list_offsets.call_count == 2
    high[0], leader_error = 9, True
    refreshed = next(observer.process_element((b"", "source", 0, 6), None))
    assert refreshed.progress[0].high_watermark == 9
    assert admin.list_topics.call_count == 2 and admin.list_offsets.call_count == 4
    high[0], high[1] = 12, None
    with pytest.raises(RuntimeError, match="broker unavailable"):
        next(observer.process_element((b"", "source", 0, 9), None))
    assert observer.watermarks == {0: 9, 1: 8, 2: 12}
