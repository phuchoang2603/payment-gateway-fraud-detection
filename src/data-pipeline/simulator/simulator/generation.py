from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from typing import Any
from uuid import uuid5

import numpy as np

from simulator.behavior import purchase_context
from simulator.calibration import zone
from simulator.config import HistoricalConfig
from simulator.identity import IDENTITY_NAMESPACE
from simulator.profiles import (
    CARD_TESTING,
    CATEGORIES,
    CONDITIONS,
    LEGITIMATE,
    SCENARIO_NAMES,
    Profiles,
    Schedule,
)

Record = dict[str, Any]
POLICY_VERSION = "rules-v1"
MAX_ATTEMPTS = 3


@dataclass(frozen=True)
class GeneratedHistory:
    events: tuple[Record, ...]
    labels: tuple[Record, ...]
    truth: str
    diagnostics: dict[str, int]


def risk_reasons(
    amount: int, created_at: datetime, attempted_at: datetime, shipping_country: str, ip_country: str | None
) -> list[str]:
    reasons = []
    if amount > 200_000:
        reasons.append("HIGH_AMOUNT")
    if (
        ip_country is not None
        and ip_country != shipping_country
        and attempted_at - created_at < timedelta(days=7)
        and amount > 20_000
    ):
        reasons.append("NEW_ACCOUNT_GEO_MISMATCH")
    return reasons


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
    anchor = datetime.combine(config.start_date, time.min, tzinfo=timezone.utc)
    customer_created = anchor - timedelta(days=int(profiles.customer_age_days[customer]))
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
                "schema_version": 3,
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
            "merchant_account_created_at": anchor - timedelta(days=int(profiles.merchant_age_days[merchant])),
            "customer_account_created_at": customer_created,
            "shipping": {
                "country": context.shipping_country,
                "region": context.region,
                "postal_code": context.postal_code,
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
        signals["user_agent_family"] = context.browser
        append("attempt_started", attempt_id=attempt_id, signals=signals, context=events[0]["payment_created"])

        clock += timedelta(milliseconds=int(random.integers(50, 400)))
        reasons = risk_reasons(
            unit_price * quantity, customer_created, attempt_started, context.shipping_country, context.ip_country
        )
        risk_reason = reasons[0] if reasons else None
        append(
            "risk_decision_recorded",
            attempt_id=attempt_id,
            assessment={
                "outcome": "RISK_OUTCOME_DECLINE" if risk_reason else "RISK_OUTCOME_APPROVE",
                "reason_codes": reasons,
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
        if succeeded and (fraud or friendly) and random.random() < config.label_delays.reporting_probability:
            delay = int(random.integers(config.label_delays.min_days, config.label_delays.max_days + 1))
            labels.append(
                {
                    "integration_id": integration_id,
                    "payment_id": payment_id,
                    "attempt_id": attempt_id,
                    "is_fraud": True,
                    "occurred_at": attempt_started,
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
        method = profiles.method_pool[profiles.payment_methods[customer, 1]]

    amount = unit_price * quantity
    bucket = next((limit for limit in (500, 5000, 20000, 100000, 200000) if amount <= limit), "above_200000")
    available_by_end = datetime.combine(config.end_date, time.min, tzinfo=timezone.utc)
    successful = any("payment_succeeded" in event for event in events)
    reports = [label for label in labels if label["source"] == "LABEL_SOURCE_CHARGEBACK"]
    local = started_at.astimezone(zone(int(profiles.customer_regions[customer])))
    diagnostics = {
        f"amount.{truth}.{bucket}": 1,
        f"utc_hour.{started_at.hour:02d}": 1,
        f"local_hour.{local.hour:02d}": 1,
        f"activity_milli.{min(10, int(profiles.activity_milli[customer]) // 500)}": 1,
        "succeeded_payments": int(successful),
        "fraud_succeeded_payments": int(successful and fraud),
        "eligible_dispute_payments": int(successful and (fraud or truth == "friendly_fraud")),
        "unreported_eligible_dispute_payments": int(
            successful and (fraud or truth == "friendly_fraud") and not reports
        ),
        "chargebacks_available_by_history_end": sum(
            label["label_available_at"] < available_by_end for label in reports
        ),
        "chargebacks_available_after_history_end": sum(
            label["label_available_at"] >= available_by_end for label in reports
        ),
        "new_context_payments": int(context.new_device),
        f"context.{truth}.{'new' if context.new_device else 'familiar'}": 1,
    }
    return GeneratedHistory(tuple(events), tuple(labels), truth, diagnostics)
