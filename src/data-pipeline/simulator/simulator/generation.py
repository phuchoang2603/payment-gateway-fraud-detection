from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from typing import Any
from uuid import uuid5

import numpy as np

from simulator.behavior import PurchaseContext, purchase_context
from simulator.config import HistoricalConfig
from simulator.identity import IDENTITY_NAMESPACE, fingerprint
from simulator.profiles import (
    CARD_TESTING,
    CATEGORIES,
    CONDITIONS,
    LEGITIMATE,
    REGIONS,
    SCENARIO_NAMES,
    Profiles,
    Schedule,
)

Record = dict[str, Any]
POLICY_VERSION = "sim-rules-v1"
HIGH_VALUE_MINOR_UNITS = 150_000
MAX_ATTEMPTS = 3


@dataclass(frozen=True)
class GeneratedHistory:
    events: tuple[Record, ...]
    labels: tuple[Record, ...]
    truth: str


def _risk_decision(context: PurchaseContext, random: np.random.Generator) -> str | None:
    if context.unit_price * context.quantity >= HIGH_VALUE_MINOR_UNITS and random.random() < 0.8:
        return "HIGH_VALUE"
    if context.new_device and context.ip_country != context.shipping_country and random.random() < 0.3:
        return "GEO_MISMATCH"
    return None


def build_history(
    config: HistoricalConfig,
    profiles: Profiles,
    schedule: Schedule,
    index: int,
    day: date,
    ordinal: int,
    random: np.random.Generator,
) -> GeneratedHistory:
    offset = (day - config.start_date).days
    behavior = config.behavior
    customer = int(schedule.customers[index])
    merchant = int(schedule.merchants[index])
    kind = int(schedule.scenarios[index])
    fraud = kind != LEGITIMATE
    integration_id = str(config.integration_id)
    payment_id = str(uuid5(IDENTITY_NAMESPACE, f"{config.dataset_version}:{day}:{ordinal}:payment"))
    started_at = datetime.combine(day, time.min, tzinfo=timezone.utc) + timedelta(seconds=int(schedule.seconds[index]))
    context = purchase_context(config, profiles, schedule, index, offset, ordinal, random)
    category = CATEGORIES[context.category]
    region, postal_code = REGIONS[context.shipping_country]
    quantity = context.quantity
    unit_price = context.unit_price
    events: list[Record] = []
    labels: list[Record] = []
    clock = started_at

    def append(payload: str, **fields: Any) -> None:
        events.append(
            {
                "event_id": str(uuid5(IDENTITY_NAMESPACE, f"{payment_id}:{len(events) + 1}")),
                "payment_id": payment_id,
                "aggregate_version": len(events) + 1,
                "integration_id": integration_id,
                "schema_version": config.schema_version_on(clock.date()),
                "correlation_id": payment_id,
                "occurred_at": clock,
                "recorded_at": clock,
                payload: fields,
            }
        )

    def money(minor_units: int) -> Record:
        return {"minor_units": minor_units, "currency": "USD"}

    append(
        "payment_created",
        merchant={
            "merchant_id": profiles.merchant_ids[merchant],
            "external_seller_id": profiles.merchant_external_ids[merchant],
        },
        customer={
            "customer_id": profiles.customer_ids[customer],
            "external_buyer_id": profiles.customer_external_ids[customer],
        },
        snapshot={
            "external_order_id": f"order-{day.isoformat()}-{ordinal:06d}",
            "total": money(unit_price * quantity),
            "items": [
                {
                    "product_id": f"sku-{category}-{int(random.integers(1, 200)):03d}",
                    "category": category,
                    "condition": CONDITIONS[context.condition],
                    "quantity": quantity,
                    "unit_price": money(unit_price),
                }
            ],
            "merchant_account_created_at": started_at - timedelta(days=int(profiles.merchant_age_days[merchant])),
            "customer_account_created_at": started_at - timedelta(days=int(profiles.customer_age_days[customer])),
            "shipping": {
                "country": context.shipping_country,
                "region": region,
                "postal_code": postal_code,
                "address_fingerprint": context.address,
            },
        },
    )

    method = context.method
    truth = SCENARIO_NAMES[kind]
    for attempt in range(1, MAX_ATTEMPTS + 1):
        clock += timedelta(seconds=1)
        attempt_started = clock
        attempt_id = str(uuid5(IDENTITY_NAMESPACE, f"{payment_id}:attempt:{attempt}"))
        signals: Record = {
            "attempted_at": clock,
            "payment_method": {"type": "card", "fingerprint": method},
            "device_fingerprint": context.device,
            "ip_country": context.ip_country,
            "ip_network_fingerprint": context.network,
        }
        if config.schema_version_on(clock.date()) == 2:
            signals["user_agent_family"] = "Chromium" if random.random() < 0.7 else "Safari"
        append("attempt_started", attempt_id=attempt_id, signals=signals)

        clock += timedelta(milliseconds=int(random.integers(50, 400)))
        risk_reason = _risk_decision(context, random)
        append(
            "risk_decision_recorded",
            attempt_id=attempt_id,
            assessment={
                "outcome": "RISK_OUTCOME_DECLINE" if risk_reason else "RISK_OUTCOME_APPROVE",
                "reason_codes": [risk_reason or "ALLOW"],
                "policy_version": POLICY_VERSION,
            },
        )
        succeeded = False
        clock += timedelta(milliseconds=int(random.integers(10, 100)))
        if risk_reason:
            append("attempt_declined", attempt_id=attempt_id, source="DECLINE_SOURCE_RISK", reason=risk_reason.lower())
        else:
            append("processor_operation_requested", attempt_id=attempt_id, operation_key=f"{payment_id}:{attempt}")
            if random.random() < behavior.unknown_outcome_rate:
                clock += timedelta(seconds=30)
                append("processor_outcome_unknown", attempt_id=attempt_id)
                clock += timedelta(minutes=behavior.unknown_resolution_minutes)
            else:
                clock += timedelta(milliseconds=int(random.integers(200, 2000)))
            outcome = random.random()
            if kind == CARD_TESTING:
                decline = "issuer_declined" if outcome < config.scenarios.card_testing.decline_rate else None
            elif outcome < behavior.issuer_decline_rate:
                decline = "issuer_declined"
            elif outcome < behavior.issuer_decline_rate + behavior.insufficient_funds_rate:
                decline = "insufficient_funds"
            else:
                decline = None
            if decline:
                append("attempt_declined", attempt_id=attempt_id, source="DECLINE_SOURCE_PROCESSOR", reason=decline)
            else:
                append("payment_succeeded", attempt_id=attempt_id, processor_reference=f"sim-{payment_id}-{attempt}")
                succeeded = True

        friendly = not fraud and succeeded and random.random() < config.scenarios.friendly_fraud.rate
        if friendly:
            truth = "friendly_fraud"
        labels.append(
            {
                "integration_id": integration_id,
                "payment_id": payment_id,
                "attempt_id": attempt_id,
                "is_fraud": fraud,
                "occurred_at": attempt_started,
                "label_available_at": clock,
                "source": "LABEL_SOURCE_SIMULATION_TRUTH",
                "scenario": "friendly_fraud" if friendly else SCENARIO_NAMES[kind],
            }
        )
        if succeeded and (fraud or friendly):
            delay = int(random.integers(config.label_delays.min_days, config.label_delays.max_days + 1))
            labels.append(
                {
                    "integration_id": integration_id,
                    "payment_id": payment_id,
                    "attempt_id": attempt_id,
                    "is_fraud": True,
                    "occurred_at": clock,
                    "label_available_at": clock + timedelta(days=delay),
                    "source": "LABEL_SOURCE_CHARGEBACK",
                    "scenario": None,
                }
            )
        if succeeded:
            break
        if attempt == MAX_ATTEMPTS or kind == CARD_TESTING or random.random() >= behavior.retry_rate:
            clock = max(clock + timedelta(seconds=1), started_at + timedelta(minutes=behavior.expiry_minutes))
            append("payment_expired", _present=True)
            break
        clock += timedelta(seconds=int(random.integers(20, 300)))
        if fraud:
            method = fingerprint(config.synthetic_key, "payment_method", f"tok_card-retry-{payment_id}-{attempt}_ok")
        else:
            method = profiles.method_pool[profiles.payment_methods[customer, 1]]

    return GeneratedHistory(tuple(events), tuple(labels), truth)
