import pyarrow as pa
import pyarrow.parquet as pq

from simulator.arrow import LABEL_SCHEMA, event_schema
from simulator.contracts.payments.v1.events_pb2 import PaymentEvent
from simulator.contracts.payments.v1.labels_pb2 import LABEL_SOURCE_SIMULATION_TRUTH, FraudLabel
from simulator.landing.parquet import Record, decode, encode


def test_protobuf_parquet_reuses_historical_business_schema():
    timestamp = "2026-07-01T00:00:00.123456Z"
    messages = {}
    event = PaymentEvent(event_id="e", payment_id="p", integration_id="i", aggregate_version=1, schema_version=3)
    event.occurred_at.FromJsonString(timestamp)
    event.recorded_at.CopyFrom(event.occurred_at)
    event.attempt_started.attempt_id = "a"
    context = event.attempt_started.context
    context.merchant.merchant_id = "merchant"
    context.customer.customer_id = "customer"
    context.snapshot.total.minor_units = 123
    context.snapshot.total.currency = "USD"
    signals = event.attempt_started.signals
    signals.attempted_at.CopyFrom(event.occurred_at)
    signals.payment_method.type = "card"
    signals.payment_method.fingerprint = "safe"
    signals.user_agent_family = ""
    messages["event-v3"] = event
    for name in ("created-v3", "risk-v3", "expired-v3"):
        event = PaymentEvent()
        event.CopyFrom(messages["event-v3"])
        event.ClearField("attempt_started")
        if name == "created-v3":
            created = event.payment_created
            created.merchant.merchant_id = "merchant"
            created.customer.customer_id = "customer"
            created.snapshot.total.minor_units = 123
            created.snapshot.total.currency = "USD"
            item = created.snapshot.items.add(product_id="sku", quantity=1)
            item.unit_price.CopyFrom(created.snapshot.total)
            created.snapshot.shipping.country = "US"
        elif name == "risk-v3":
            risk = event.risk_decision_recorded
            risk.attempt_id = "a"
            risk.assessment.outcome = 1
            risk.assessment.risk_score = 0
            risk.assessment.reason_codes.append("SAFE")
        else:
            event.payment_expired.present = True
        messages[name] = event
    event = PaymentEvent()
    event.CopyFrom(messages["risk-v3"])
    event.risk_decision_recorded.assessment.assessment_id = "assessment-1"
    event.risk_decision_recorded.assessment.evidence_sha256 = "a" * 64
    messages["risk-ref-v3"] = event
    label = FraudLabel(
        integration_id="i", payment_id="p", attempt_id="a", is_fraud=False, source=LABEL_SOURCE_SIMULATION_TRUTH
    )
    label.occurred_at.FromJsonString(timestamp)
    label.label_available_at.CopyFrom(label.occurred_at)
    messages["label"] = label
    for name, message in messages.items():
        record = Record(
            "labels" if name == "label" else "events",
            0,
            42,
            None,
            b"",
            message.SerializeToString(),
            [("x", None), ("x", b""), ("x", b"\x00\xff")],
        )
        name_table, version, decoded_row = decode(record, "labels")
        table = pq.read_table(pa.BufferReader(encode(name_table, version, [decoded_row])))
        expected = LABEL_SCHEMA if name == "label" else event_schema(version)
        actual = table.select(expected.names)
        assert actual.schema.equals(expected, check_metadata=False), (actual.schema, expected)
        row = table.to_pylist()[0]
        transport = row["_transport"]
        assert transport["value"] == message.SerializeToString()
        assert transport["key"] == b"" and transport["offset"] == 42
        assert transport["headers"] == [
            {"key": "x", "value": None},
            {"key": "x", "value": b""},
            {"key": "x", "value": b"\x00\xff"},
        ]
        if name == "label":
            assert row["is_fraud"] is False and row["scenario"] is None
        else:
            assert version == 4 and row["schema_version"] == 3
            assert row["occurred_at"].microsecond == 123456
            if name.startswith("event-"):
                assert row["payment_created"] is None
                signals = row["attempt_started"]["signals"]
                assert signals["user_agent_family"] == ""
            elif name == "created-v3":
                assert row["payment_created"]["snapshot"]["items"][0]["unit_price"] == {
                    "minor_units": 123,
                    "currency": "USD",
                }
            elif name.startswith("risk-"):
                assessment = row["risk_decision_recorded"]["assessment"]
                assert assessment["outcome"] == "RISK_OUTCOME_APPROVE"
                assert assessment["risk_score"] == 0 and assessment["model_version"] is None
                if name == "risk-ref-v3":
                    assert assessment["assessment_id"] == "assessment-1"
                    assert assessment["evidence_sha256"] == "a" * 64
                else:
                    assert assessment["assessment_id"] is None and assessment["evidence_sha256"] is None
            else:
                assert row["payment_expired"]["_present"] is True
