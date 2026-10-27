import json
import os
import time
from http.client import RemoteDisconnected
from urllib.request import urlopen

from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from fraud_feast import schema
from fraud_feast.contracts.features.v1 import features_pb2
from testcontainers.community.kafka import KafkaContainer
from testcontainers.core.container import DockerContainer
from testcontainers.core.network import Network
from testcontainers.core.wait_strategies import HttpWaitStrategy

from fraud_streaming.model import HOUR
from fraud_streaming.wire import DEFINITION_HASH, PaymentEvent


def await_result(read, *, timeout=120):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = read()
        if result:
            return result
        time.sleep(0.2)
    raise AssertionError("Flink integration condition did not complete before its deadline")


def lifecycle(attempt, started_at, *, customer="customer"):
    event = PaymentEvent(
        event_id=f"start-{attempt}",
        integration_id="integration",
        payment_id=attempt,
        aggregate_version=1,
        schema_version=3,
    )
    event.occurred_at.FromMicroseconds(started_at)
    event.recorded_at.CopyFrom(event.occurred_at)
    event.attempt_started.attempt_id = attempt
    event.attempt_started.context.customer.customer_id = customer
    event.attempt_started.context.snapshot.total.currency = "USD"
    event.attempt_started.context.snapshot.total.minor_units = 100
    event.attempt_started.signals.payment_method.fingerprint = "shared-method"
    event.attempt_started.signals.attempted_at.CopyFrom(event.occurred_at)
    start = event.SerializeToString(deterministic=True)
    event.ClearField("attempt_started")
    event.event_id = f"decision-{attempt}"
    event.aggregate_version = 2
    event.occurred_at.FromMicroseconds(started_at + 1_000_000)
    event.recorded_at.CopyFrom(event.occurred_at)
    event.risk_decision_recorded.attempt_id = attempt
    event.risk_decision_recorded.assessment.outcome = 1
    event.risk_decision_recorded.assessment.policy_version = "rules-v1"
    return start, event.SerializeToString(deterministic=True)


def test_image_kafka_journal_and_checkpoint_recovery():
    epoch = "image-ci"
    source = "payments.image-ci"
    clock = "clock.image-ci"
    output = f"fraud.feature-updates.v1.{epoch}"
    anchor = 1_760_000_000_000_000
    with Network() as network:
        kafka = (
            KafkaContainer("confluentinc/cp-kafka:7.8.0")
            .with_kraft()
            .with_network(network)
            .with_network_aliases("kafka")
            .with_env("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
            .with_env("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
        )
        with kafka:
            brokers = kafka.get_bootstrap_server()
            admin = AdminClient({"bootstrap.servers": brokers})
            for future in admin.create_topics(
                [NewTopic(source, 3, 1), NewTopic(clock, 1, 1), NewTopic(output, 3, 1)]
            ).values():
                future.result(timeout=30)
            producer = Producer({"bootstrap.servers": brokers})
            consumer = Consumer(
                {
                    "bootstrap.servers": brokers,
                    "group.id": "image-ci-readback",
                    "auto.offset.reset": "earliest",
                    "isolation.level": "read_committed",
                    "enable.auto.commit": False,
                }
            )
            consumer.subscribe([output])
            properties = "\n".join(
                [
                    "jobmanager.rpc.address: localhost",
                    "jobmanager.memory.process.size: 1024m",
                    "taskmanager.memory.process.size: 2048m",
                    "taskmanager.numberOfTaskSlots: 1",
                    "python.executable: /app/.venv/bin/python",
                    "python.client.executable: /app/.venv/bin/python",
                    "execution.checkpointing.storage: filesystem",
                    "execution.checkpointing.dir: file:///tmp/checkpoints",
                    "execution.checkpointing.timeout: 60s",
                    "restart-strategy.type: fixed-delay",
                    "restart-strategy.fixed-delay.attempts: 3",
                    "restart-strategy.fixed-delay.delay: 1s",
                ]
            )
            flink = (
                DockerContainer(os.environ["TEST_IMAGE"])
                .with_network(network)
                .with_exposed_ports(8081)
                .waiting_for(HttpWaitStrategy(8081, "/overview").with_startup_timeout(120))
                .with_env("FLINK_PROPERTIES", properties)
                .with_env("KAFKA_BROKERS", "kafka:9092")
                .with_env("SOURCE_TOPIC", source)
                .with_env("FEATURE_TOPIC", output)
                .with_env("FEATURE_EPOCH", epoch)
                .with_env("CLOCK_MODE", "logical")
                .with_env("CLOCK_TOPIC", clock)
                .with_command(
                    [
                        "bash",
                        "-ec",
                        "bin/jobmanager.sh start\nbin/taskmanager.sh start\n"
                        "exec bin/flink run -pym fraud_streaming.job",
                    ]
                )
            )
            try:
                with flink:
                    try:
                        base = (
                            f"http://{flink.get_container_host_ip()}:{flink.get_exposed_port(8081)}"
                        )

                        def rest(path):
                            try:
                                with urlopen(base + path, timeout=5) as response:
                                    return json.load(response)
                            except (OSError, RemoteDisconnected):
                                return {}

                        def running_job():
                            return next(
                                (
                                    job["jid"]
                                    for job in rest("/jobs/overview").get("jobs", [])
                                    if job["state"] == "RUNNING"
                                ),
                                None,
                            )

                        job_id = await_result(running_job)

                        def send(attempt, instant, *, customer="customer"):
                            records = lifecycle(attempt, instant, customer=customer)
                            for record in records:
                                producer.produce(source, key=attempt, value=record, partition=0)
                            assert producer.flush(10) == 0
                            return records

                        latest = {}

                        def receive(predicate):
                            def poll():
                                message = consumer.poll(1)
                                if message is not None:
                                    assert not message.error(), message.error()
                                    value = features_pb2.SnapshotUpdate.FromString(message.value())
                                    schema.validate(
                                        value, epoch=epoch, definition_hash=DEFINITION_HASH
                                    )
                                    view = "customer" if value.view == 1 else "method"
                                    assert message.key().decode() == schema.entity_key(
                                        epoch,
                                        view,
                                        value.integration_id,
                                        value.entity_id,
                                    )
                                    prior = latest.get(value.entity_id)
                                    if prior is not None:
                                        assert value.revision > prior.revision
                                    latest[value.entity_id] = value
                                return predicate(latest)

                            await_result(poll)

                        send("one", anchor)
                        receive(lambda values: len(values) == 2)
                        producer.produce(
                            clock, value=json.dumps({"clock_us": anchor + HOUR + 2_000_000})
                        )
                        assert producer.flush(10) == 0
                        receive(
                            lambda values: all(
                                value.coverage.one_hour == 2 and value.values.assessed_count_1h == 0
                                for value in values.values()
                            )
                        )
                        second = send("two", anchor + HOUR + 3_000_000)
                        receive(
                            lambda values: all(
                                value.values.assessed_count_1h == 1 for value in values.values()
                            )
                        )
                        saved = {entity: value.snapshot_id for entity, value in latest.items()}
                        checkpoint_path = f"/jobs/{job_id}/checkpoints"

                        def checkpoint(name):
                            return rest(checkpoint_path).get("latest", {}).get(name) or {}

                        completed = await_result(lambda: checkpoint("completed"))
                        code, logs = flink.exec(
                            ["bash", "-ec", "bin/taskmanager.sh stop\nbin/taskmanager.sh start"]
                        )
                        assert code == 0, logs
                        await_result(
                            lambda: checkpoint("restored").get("id", -1) >= completed["id"]
                        )
                        for record in second:
                            producer.produce(source, key="two", value=record, partition=0)
                        assert producer.flush(10) == 0
                        send("three", anchor + HOUR + 5_000_000, customer="other-customer")
                        receive(
                            lambda values: (
                                "other-customer" in values
                                and values["shared-method"].values.assessed_count_1h == 2
                            )
                        )
                        assert latest["customer"].snapshot_id == saved["customer"]
                        assert latest["other-customer"].values.assessed_count_1h == 1
                    except BaseException:
                        print(flink.get_logs())
                        print(flink.exec(["bash", "-c", "tail -n 120 log/*.log"])[1].decode())
                        raise
            finally:
                consumer.close()
