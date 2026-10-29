from collections.abc import Callable
from datetime import UTC, date, datetime, time, timedelta
from hashlib import sha256
from typing import Any
from uuid import uuid5

import numpy as np

from simulator.world import CARD_TESTING, IDENTITY_NAMESPACE, SCENARIOS, Purchase, World

Record = dict[str, Any]
POLICY_VERSION = "rules-v1"
GATEWAY_SCHEMA_VERSION = 3
MAX_ATTEMPTS = 3
HIGH_AMOUNT_MINOR = 200_000
GEO_MISMATCH_MINOR = 20_000
NEW_ACCOUNT_WINDOW = timedelta(days=7)
SHIPPING_COUNTRY = "US"


def risk_reasons(
    amount: int, customer_created_at: datetime, attempted_at: datetime, shipping_country: str, ip_country: str | None
) -> list[str]:
    reasons = []
    if amount > HIGH_AMOUNT_MINOR:
        reasons.append("HIGH_AMOUNT")
    if (
        ip_country is not None
        and ip_country != shipping_country
        and attempted_at - customer_created_at < NEW_ACCOUNT_WINDOW
        and amount > GEO_MISMATCH_MINOR
    ):
        reasons.append("NEW_ACCOUNT_GEO_MISMATCH")
    return reasons


def payment_history(
    world: World,
    purchase: Purchase,
    rng: np.random.Generator,
    *,
    day: date,
    key: str,
    physical_version_at: Callable[[datetime], int],
    duplicate_rate: float,
) -> tuple[list[Record], list[Record]]:
    behavior = world.config.behavior
    integration_id = str(world.config.integration_id)
    payment_id = str(uuid5(IDENTITY_NAMESPACE, f"{world.digest}:{day.isoformat()}:{key}:payment"))
    started_at = datetime.combine(day, time.min, tzinfo=UTC) + timedelta(seconds=purchase.started_seconds)
    events: list[Record] = []
    labels: list[Record] = []
    clock = started_at
    aggregate_version = 0

    def append(payload: str, **fields: Any) -> Record:
        nonlocal aggregate_version
        aggregate_version += 1
        row = {
            "event_id": str(uuid5(IDENTITY_NAMESPACE, f"{payment_id}:{len(events) + 1}")),
            "payment_id": payment_id,
            "aggregate_version": aggregate_version,
            "integration_id": integration_id,
            "schema_version": GATEWAY_SCHEMA_VERSION,
            "physical_schema_version": physical_version_at(clock),
            "correlation_id": payment_id,
            "occurred_at": clock,
            "recorded_at": clock,
            payload: fields,
        }
        events.append(row)
        if duplicate_rate and rng.random() < duplicate_rate:
            events.append(dict(row))
        return row

    def money(minor_units: int) -> Record:
        return {"minor_units": minor_units, "currency": "USD"}

    amount = purchase.unit_price * purchase.quantity
    created = append(
        "payment_created",
        merchant={"merchant_id": purchase.merchant_id, "external_seller_id": purchase.merchant_external_id},
        customer={"customer_id": purchase.customer_id, "external_buyer_id": purchase.customer_external_id},
        snapshot={
            "external_order_id": f"order-{day.isoformat()}-{key}",
            "total": money(amount),
            "items": [
                {
                    "product_id": f"sku-{purchase.category}-{int(rng.integers(1, 200)):03d}",
                    "category": purchase.category,
                    "condition": purchase.condition,
                    "quantity": purchase.quantity,
                    "unit_price": money(purchase.unit_price),
                }
            ],
            "merchant_account_created_at": purchase.merchant_created_at,
            "customer_account_created_at": purchase.customer_created_at,
            "shipping": {
                "country": SHIPPING_COUNTRY,
                "region": purchase.shipping_region,
                "postal_code": purchase.postal_code,
                "address_fingerprint": purchase.address_fingerprint,
            },
        },
    )["payment_created"]

    fraud = purchase.scenario != "legitimate"
    succeeded = False
    for attempt in range(1, MAX_ATTEMPTS + 1):
        clock += timedelta(seconds=1)
        attempt_started = clock
        attempt_id = str(uuid5(IDENTITY_NAMESPACE, f"{payment_id}:attempt:{attempt}"))
        append(
            "attempt_started",
            attempt_id=attempt_id,
            signals={
                "attempted_at": clock,
                "payment_method": {"type": "card", "fingerprint": purchase.payment_method},
                "device_fingerprint": purchase.device,
                "ip_country": purchase.ip_country,
                "ip_network_fingerprint": purchase.network,
                "user_agent_family": purchase.browser,
            },
            context=created,
        )

        clock += timedelta(milliseconds=int(rng.integers(50, 400)))
        reasons = risk_reasons(
            amount, purchase.customer_created_at, attempt_started, SHIPPING_COUNTRY, purchase.ip_country
        )
        append(
            "risk_decision_recorded",
            attempt_id=attempt_id,
            assessment={
                "outcome": "RISK_OUTCOME_DECLINE" if reasons else "RISK_OUTCOME_APPROVE",
                "reason_codes": reasons,
                "policy_version": POLICY_VERSION,
                "assessment_id": str(uuid5(IDENTITY_NAMESPACE, f"{payment_id}:assessment:{attempt}")),
                "evidence_sha256": sha256(f"{payment_id}:{attempt}".encode()).hexdigest(),
            },
        )
        clock += timedelta(milliseconds=int(rng.integers(10, 100)))
        succeeded = False
        if reasons:
            append("attempt_declined", attempt_id=attempt_id, source="DECLINE_SOURCE_RISK", reason=reasons[0].lower())
        else:
            append("processor_operation_requested", attempt_id=attempt_id, operation_key=f"{payment_id}:{attempt}")
            if rng.random() < behavior.unknown_outcome_rate:
                clock += timedelta(seconds=30)
                append("processor_outcome_unknown", attempt_id=attempt_id)
                clock += timedelta(minutes=behavior.unknown_resolution_minutes)
            else:
                clock += timedelta(milliseconds=int(rng.integers(200, 2000)))
            outcome = (
                purchase.processor_outcome
                if attempt == 1
                else world.processor_outcome(rng, SCENARIOS.index(purchase.scenario))
            )
            if outcome == "succeeded":
                append("payment_succeeded", attempt_id=attempt_id, processor_reference=f"sim-{payment_id}-{attempt}")
                succeeded = True
            else:
                append("attempt_declined", attempt_id=attempt_id, source="DECLINE_SOURCE_PROCESSOR", reason=outcome)

        friendly = purchase.friendly_fraud and succeeded
        labels.append(
            {
                "integration_id": integration_id,
                "payment_id": payment_id,
                "attempt_id": attempt_id,
                "is_fraud": fraud,
                "occurred_at": attempt_started,
                "label_available_at": clock,
                "source": "LABEL_SOURCE_SIMULATION_TRUTH",
                "scenario": "friendly_fraud" if friendly else purchase.scenario,
            }
        )
        if succeeded and (fraud or friendly) and purchase.chargeback_reported:
            labels.append(
                {
                    "integration_id": integration_id,
                    "payment_id": payment_id,
                    "attempt_id": attempt_id,
                    "is_fraud": True,
                    "occurred_at": attempt_started,
                    "label_available_at": clock + timedelta(days=purchase.label_delay_days),
                    "source": "LABEL_SOURCE_CHARGEBACK",
                    "scenario": None,
                }
            )
        if succeeded:
            break
        if (
            attempt == MAX_ATTEMPTS
            or purchase.scenario == SCENARIOS[CARD_TESTING]
            or rng.random() >= behavior.retry_rate
        ):
            clock = max(clock + timedelta(seconds=1), started_at + timedelta(minutes=behavior.expiry_minutes))
            append("payment_expired", _present=True)
            break
        clock += timedelta(seconds=int(rng.integers(20, 300)))
    return events, labels
