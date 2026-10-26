from datetime import datetime, time, timedelta, timezone

import numpy as np

from simulator.behavior import purchase_context
from simulator.config import HistoricalConfig
from simulator.profiles import (
    ACCOUNT_TAKEOVER,
    CARD_TESTING,
    CATEGORIES,
    CONDITIONS,
    LEGITIMATE,
    OBVIOUS_HIGH_VALUE,
    REGIONS,
    STOLEN_CARD_NEW_DEVICE,
    Schedule,
    profiles_from_manifest,
    stage_rng,
    zipf_weights,
)


class HistoricalModel:
    def __init__(self, manifest):
        configuration = {
            key: value
            for key, value in manifest["configuration"].items()
            if key not in {"validation_gate", "execution", "storage"}
        }
        self.config = HistoricalConfig.model_validate(configuration)
        self.profiles = profiles_from_manifest({**manifest, "configuration": configuration})
        if self.config.counts.payment_methods > 3900000:
            raise ValueError("historical card pool exceeds vault mapping")
        self.merchant_cdf = np.cumsum(
            zipf_weights(self.config.counts.merchants, self.config.behavior.merchant_zipf_exponent)
        )
        self.anchor = datetime.combine(self.config.start_date, time.min, timezone.utc)

    def purchase(self, seed, ordinal, now):
        cfg, p = self.config, self.profiles
        offset = (now.date() - cfg.start_date).days
        random = stage_rng(seed, 100, now.date().toordinal(), ordinal)
        customer = int(random.integers(cfg.counts.customers))
        if random.random() < cfg.behavior.familiar_merchant_probability:
            merchant = int(random.choice(p.familiar_merchants[customer]))
        else:
            merchant = int(np.searchsorted(self.merchant_cdf, random.random()))
        scenario = LEGITIMATE
        for kind, settings in [
            (STOLEN_CARD_NEW_DEVICE, cfg.scenarios.stolen_card_new_device),
            (ACCOUNT_TAKEOVER, cfg.scenarios.account_takeover),
        ]:
            for day in range(max(0, offset - settings.window_days + 1), offset + 1):
                if stage_rng(cfg.seed, 103, kind, day, customer).random() < settings.rate:
                    scenario = kind
                    break
        if scenario == LEGITIMATE and random.random() < cfg.scenarios.obvious_high_value.rate:
            scenario = OBVIOUS_HIGH_VALUE
        daily_purchases = cfg.counts.customers * cfg.behavior.purchases_per_customer_day
        testing_share = (
            cfg.scenarios.card_testing.rate * cfg.scenarios.card_testing.burst_size / max(daily_purchases, 1)
        )
        if random.random() < min(testing_share, 1):
            scenario = CARD_TESTING
        schedule = Schedule(
            np.array([customer]), np.array([merchant]), np.array([0]), np.array([scenario]), np.array([offset])
        )
        context = purchase_context(cfg, p, schedule, 0, offset, ordinal, random)
        method = int(p.payment_methods[customer, int(random.integers(0, 2))])
        outcome = random.random()
        behavior = (
            "nsf"
            if outcome < cfg.behavior.insufficient_funds_rate
            else "issuer"
            if outcome < cfg.behavior.insufficient_funds_rate + cfg.behavior.issuer_decline_rate
            else "ok"
        )
        if scenario == CARD_TESTING and random.random() < cfg.scenarios.card_testing.decline_rate:
            behavior = "invalid"
        region, postal = REGIONS[context.shipping_country]
        order = {
            "seller": {
                "external_id": p.merchant_external_ids[merchant],
                "account_created_at": (self.anchor - timedelta(days=int(p.merchant_age_days[merchant]))).isoformat(),
            },
            "buyer": {
                "external_id": p.customer_external_ids[customer],
                "account_created_at": (self.anchor - timedelta(days=int(p.customer_age_days[customer]))).isoformat(),
            },
            "amount": {"minor_units": context.unit_price * context.quantity, "currency": "USD"},
            "items": [
                {
                    "product_id": f"sku-{CATEGORIES[context.category]}-001",
                    "category": CATEGORIES[context.category],
                    "condition": CONDITIONS[context.condition],
                    "quantity": context.quantity,
                    "unit_price": context.unit_price,
                }
            ],
            "shipping_address": {
                "name": "Synthetic buyer",
                "line1": context.address_value,
                "city": "Example City",
                "country": context.shipping_country,
                "region": region,
                "postal_code": postal,
            },
        }
        signals = {
            "device_fingerprint": context.device,
            "user_agent_family": "browser",
            "simulated_client": {"ip_country": context.ip_country, "ip_network": context.network_address},
        }
        fraud = scenario != LEGITIMATE
        delay = random.uniform(cfg.label_delays.min_days, cfg.label_delays.max_days) * 86400
        friendly = not fraud and random.random() < cfg.scenarios.friendly_fraud.rate
        return order, signals, method, behavior, scenario, fraud, friendly, delay


def card_number(index, behavior):
    if not 0 <= index < 3900000:
        raise ValueError("card index outside historical domain")
    prefix = {"ok": "4111", "nsf": "4112", "issuer": "4113", "invalid": "4114"}[behavior]
    body = f"{prefix}{index:011d}"
    total = 0
    for i, digit in enumerate(reversed(body)):
        value = int(digit) * (2 if i % 2 == 0 else 1)
        total += value - 9 if value > 9 else value
    return body + str(-total % 10)
