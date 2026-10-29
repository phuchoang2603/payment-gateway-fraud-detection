import asyncio
import io
import json
import threading
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest
import yaml

from simulator.contracts.payments.v1.labels_pb2 import LABEL_SOURCE_SIMULATION_TRUTH, FraudLabel
from simulator.live.config import Faults, LiveConfig
from simulator.live.faults import deliveries
from simulator.live.model import HistoricalModel, card_number
from simulator.live.runtime import Gateway, Labels, run_live, traffic
from simulator.live.source import load_model
from simulator.world import World, WorldConfig, entity_id

WORLD = json.loads(
    json.dumps(yaml.safe_load(Path(__file__).with_name("small-generator.yaml").read_text())["world"], default=str)
)


def live_config(**overrides):
    return LiveConfig(
        manifest="s3://platform-dev/datasets/payment-gateway/historical/test/manifest.json",
        manifest_sha256="0" * 64,
        **overrides,
    )


def world_manifest():
    return {
        "generator_digest": "generator",
        "world": WORLD,
        "world_digest": World.build(WorldConfig.model_validate(WORLD)).digest,
    }


def test_live_purchases_come_from_the_world_and_card_identity_holds(monkeypatch):
    manifest = world_manifest()
    model = HistoricalModel(manifest)
    now = datetime(2030, 1, 1, tzinfo=timezone.utc)
    order, signals, index, *_ = model.purchase(2603, 0, now)
    assert order["buyer"]["external_id"].startswith("buyer-")
    assert datetime.fromisoformat(order["buyer"]["account_created_at"]) < model.anchor
    assert model.world.payment_method_fingerprint(index)
    assert "scenario" not in signals and "is_fraud" not in order
    assert len(card_number(10**11 - 1, "ok")) == 16
    with pytest.raises(ValueError, match="outside synthetic encoding"):
        card_number(10**11, "ok")
    monkeypatch.setattr(
        "simulator.s3.boto3.client",
        lambda *a, **kw: SimpleNamespace(get_object=lambda **kw: {"Body": io.BytesIO(json.dumps(manifest).encode())}),
    )
    with pytest.raises(ValueError, match="manifest digest"):
        load_model(live_config())
    with pytest.raises(ValueError, match="world digest"):
        HistoricalModel({**manifest, "world_digest": "wrong"})


def test_gateway_retries_preserve_original_command():
    calls = []

    def handle(request):
        calls.append((request.headers["Idempotency-Key"], request.content))
        if len(calls) == 1:
            raise httpx.ReadTimeout("response lost", request=request)
        return httpx.Response(503 if len(calls) == 2 else 200, json={"attempt_id": "one"})

    async def run():
        async with httpx.AsyncClient(base_url="https://gateway", transport=httpx.MockTransport(handle)) as client:
            return await Gateway(live_config(), client, "private").request(
                "POST", "/attempts", "stable", {"token": "private"}
            )

    assert asyncio.run(run()) == {"attempt_id": "one"}
    assert len(calls) == 3 and len(set(calls)) == 1


def test_label_publication_serialized_and_pending_chargebacks_reported():
    class Producer:
        def __init__(self):
            self.messages = []
            self.lock = threading.Lock()

        def produce(self, topic, key, value, on_delivery):
            assert self.lock.acquire(blocking=False)
            self.messages.append(FraudLabel.FromString(value))
            on_delivery(None, None)

        def flush(self, timeout):
            self.lock.release()
            return 0

    async def run():
        stop = asyncio.Event()

        class GatewayStub:
            status = "PENDING"

            async def request(self, *args):
                stop.set()
                return {"attempts": [{"attempt_id": "a", "status": self.status}]}

        producer = Producer()
        gateway = GatewayStub()
        labels = Labels(live_config(), gateway, "integration", producer)
        now = datetime.now(timezone.utc)
        await asyncio.gather(*(labels.publish("p", "a", now, False, LABEL_SOURCE_SIMULATION_TRUTH) for _ in range(2)))
        await labels.schedule("p", "a", now, True, False, 0)
        await labels.run(stop)
        assert len(producer.messages) == 2
        assert all(m.label_available_at.ToDatetime(tzinfo=timezone.utc) >= now for m in producer.messages)
        assert labels.chargebacks_published == 0 and labels.unfinished_chargebacks == 1
        stop.clear()
        gateway.status = "SUCCEEDED"
        await labels.schedule("p", "a", now, True, False, 0)
        await labels.run(stop)
        assert labels.chargebacks_published == 1
        assert producer.messages[-1].is_fraud is True

    asyncio.run(run())


def test_fault_selection_is_order_independent_and_holds_delivery():
    settings = Faults(duplicate_rate=0.5, late_rate=1, late_min_seconds=2, late_max_seconds=5, hold_seconds=10)
    original = {name: deliveries(settings, 2603, name, 60) for name in ("a", "b", "c")}
    assert original == {name: deliveries(settings, 2603, name, 60) for name in ("c", "a", "b")}
    assert all(t >= 70 for times in original.values() for t in times)
    assert deliveries(Faults(), 2603, "a", 123) == [123]


def test_traffic_failure_is_visible_and_reports_unfinished(capsys):
    class Model:
        manifest = {"generator_digest": "generator"}
        config = SimpleNamespace(integration_id="integration")
        world = SimpleNamespace(digest="world", payment_method_fingerprint=lambda index: "expected")

        def purchase(self, *args):
            return {}, {}, 0, "ok", "legitimate", False, False, 0

    class GatewayStub:
        async def request(self, *args):
            return {"fingerprint": "wrong"}

    stop = asyncio.Event()
    with pytest.raises(ValueError, match="historical identity"):
        asyncio.run(traffic(live_config(duration_seconds=0.01), Model(), GatewayStub(), None, stop))
    assert '"unfinished": 1' in capsys.readouterr().out


def test_fatal_loop_stops_shared_process_and_fault_thread(monkeypatch, capsys):
    finished = threading.Event()

    async def failed_traffic(*args):
        raise ValueError("traffic failure")

    def fault_loop(config, stop):
        stop.wait(5)
        if stop.is_set():
            finished.set()

    monkeypatch.setenv("GATEWAY_API_KEY", "private")
    monkeypatch.setattr(
        "simulator.live.runtime.load_model",
        lambda config: SimpleNamespace(config=SimpleNamespace(integration_id="integration")),
    )
    monkeypatch.setattr("simulator.live.runtime.Producer", lambda config: object())
    monkeypatch.setattr("simulator.live.runtime.traffic", failed_traffic)
    monkeypatch.setattr("simulator.live.runtime.run_faults", fault_loop)
    with pytest.raises(ValueError, match="traffic failure"):
        asyncio.run(run_live(live_config(faults=Faults(enabled=True))))
    assert finished.is_set()
    assert "live_stop" in capsys.readouterr().out


def test_continuous_traffic_stops_on_shutdown_and_finishes_admitted_payments(capsys):
    class Model:
        manifest = {"generator_digest": "generator"}
        config = SimpleNamespace(integration_id="integration")
        world = SimpleNamespace(digest="world", payment_method_fingerprint=lambda index: "expected")

        def purchase(self, *args):
            order = {"seller": {"external_id": "seller"}, "buyer": {"external_id": "buyer"}}
            return order, {}, 0, "ok", "legitimate", False, False, 0

    class GatewayStub:
        async def request(self, method, path, key, body=None, bearer=None):
            if path == "/v1/payment-methods":
                return {"fingerprint": "expected", "payment_method_token": "token"}
            if path == "/v1/payments":
                return {
                    "merchant_id": entity_id("integration", "merchant", "seller"),
                    "customer_id": entity_id("integration", "customer", "buyer"),
                    "payment_id": key,
                    "session_id": "session",
                    "checkout_token": "checkout",
                }
            return {"attempt_id": key}

    async def run():
        stop = asyncio.Event()

        class LabelsStub:
            published = 0

            async def publish(self, *args):
                self.published += 1
                if self.published == 3:
                    stop.set()

            async def schedule(self, *args):
                pass

        labels = LabelsStub()
        await asyncio.wait_for(
            traffic(
                live_config(duration_seconds=0, payments_per_second=1000, concurrency=1),
                Model(),
                GatewayStub(),
                labels,
                stop,
            ),
            timeout=2,
        )
        assert labels.published == 3

    asyncio.run(run())
    reports = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    stopped = next(report for report in reports if report["event"] == "traffic_stop")
    assert stopped["admitted"] == stopped["completed"] == 3
    assert stopped["unfinished"] == 0
