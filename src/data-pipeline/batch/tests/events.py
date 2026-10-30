from datetime import datetime, timedelta, timezone

from batch.bronze.schema import EVENT_COLUMNS
from batch.bronze.validation import EVENT_PAYLOADS

BASE = datetime(2026, 7, 31, 12, tzinfo=timezone.utc)


def at(minutes):
    return BASE + timedelta(minutes=minutes)


def money(amount):
    return {"minor_units": amount, "currency": "EUR"}


def checkout(amount=100):
    return {
        "external_order_id": "order-1",
        "total": money(amount),
        "items": [
            {"product_id": "p-1", "category": "phone", "condition": "good", "quantity": 1, "unit_price": money(amount)}
        ],
        "merchant_account_created_at": BASE,
        "customer_account_created_at": BASE,
        "shipping": {"country": "DE", "region": "BE", "postal_code": "10115", "address_fingerprint": "f"},
    }


def context(amount=100):
    return {
        "merchant": {"merchant_id": "m-1", "external_seller_id": "s-1"},
        "customer": {"customer_id": "c-1", "external_buyer_id": "b-1"},
        "snapshot": checkout(amount),
    }


def row(payment_id, event_id, version, minutes, recorded=None, **payload):
    values = {column: None for column in EVENT_COLUMNS}
    values.update(
        event_id=event_id,
        payment_id=payment_id,
        integration_id="int-1",
        aggregate_version=version,
        occurred_at=at(minutes),
        recorded_at=at(minutes if recorded is None else recorded),
        schema_version=3,
        correlation_id=f"corr-{payment_id}",
    )
    values.update(payload)
    values["payload_case"] = next((name for name in payload if name in EVENT_PAYLOADS), None)
    return values


def created(payment_id, event_id, minutes=0, recorded=None, amount=100):
    return row(
        payment_id,
        event_id,
        1,
        minutes,
        recorded,
        payment_created={**context(amount)},
    )


def started(payment_id, event_id, version, minutes, attempt):
    return row(
        payment_id,
        event_id,
        version,
        minutes,
        attempt_started={
            "attempt_id": attempt,
            "signals": {
                "attempted_at": at(minutes),
                "payment_method": {"type": "card", "fingerprint": "pm-1"},
                "device_fingerprint": "device-1",
                "ip_country": None,
                "ip_network_fingerprint": None,
                "user_agent_family": None,
            },
            "context": context(),
        },
    )


def approved(payment_id, event_id, version, minutes, attempt):
    return row(
        payment_id,
        event_id,
        version,
        minutes,
        risk_decision_recorded={
            "attempt_id": attempt,
            "assessment": {
                "outcome": "RISK_OUTCOME_APPROVE",
                "reason_codes": [],
                "policy_version": "policy-1",
                "model_version": None,
                "feature_set_version": None,
                "risk_score": None,
                "assessment_id": "assessment-1",
                "evidence_sha256": "evidence-1",
            },
        },
    )


def requested(payment_id, event_id, version, minutes, attempt):
    return row(
        payment_id,
        event_id,
        version,
        minutes,
        processor_operation_requested={"attempt_id": attempt, "operation_key": f"op-{attempt}"},
    )


def succeeded(payment_id, event_id, version, minutes, attempt):
    return row(
        payment_id,
        event_id,
        version,
        minutes,
        payment_succeeded={"attempt_id": attempt, "processor_reference": "ref-1"},
    )


def declined(payment_id, event_id, version, minutes, attempt, source="DECLINE_SOURCE_PROCESSOR"):
    return row(
        payment_id,
        event_id,
        version,
        minutes,
        attempt_declined={"attempt_id": attempt, "source": source, "reason": "issuer"},
    )


def label(payment_id, attempt, available, source="LABEL_SOURCE_SIMULATION_TRUTH", minutes=0):
    return {
        "integration_id": "int-1",
        "payment_id": payment_id,
        "attempt_id": attempt,
        "is_fraud": source == "LABEL_SOURCE_CHARGEBACK",
        "occurred_at": at(minutes),
        "label_available_at": at(available),
        "source": source,
        "scenario": None if source == "LABEL_SOURCE_CHARGEBACK" else "card_testing",
    }
