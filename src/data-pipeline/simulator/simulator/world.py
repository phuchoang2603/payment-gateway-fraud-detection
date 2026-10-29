import json
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from hashlib import sha256
from uuid import UUID

import numpy as np
from pydantic import Field

from simulator.calibration import CALIBRATION, REGIONS, hourly_weights, provenance, purchase_seconds
from simulator.config import EntityCounts, LabelDelays, PurchaseBehavior, Scenarios, StrictModel
from simulator.identity import entity_id, fingerprint
from simulator.profiles import (
    ACCESSORY,
    ACCOUNT_TAKEOVER,
    CARD_TESTING,
    CATEGORIES,
    CATEGORY_PRICES,
    CONDITION_PRICE_FACTORS,
    CONDITION_WEIGHTS,
    CONDITIONS,
    LEGITIMATE,
    OBVIOUS_HIGH_VALUE,
    SCENARIO_NAMES,
    STOLEN_CARD_NEW_DEVICE,
    address_parts,
    address_value,
    stage_rng,
    zipf_weights,
)

SCENARIOS = SCENARIO_NAMES
ACTIVITY_STAGE = 300
REGION_STAGE = 301
MERCHANT_STAGE = 302
CUSTOMER_STAGE = 303
COMPROMISE_STAGE = 304
PURCHASE_COUNT_STAGE = 305
BURST_COUNT_STAGE = 306
BURST_STAGE = 307
PURCHASE_STAGE = 308

REGION_WEIGHTS = np.array([region[4] for region in REGIONS])
SECONDS_PER_DAY = 86400


class WorldConfig(StrictModel):
    seed: int = Field(ge=0)
    integration_id: UUID
    synthetic_key: str = Field(min_length=1)
    start_date: date
    counts: EntityCounts
    behavior: PurchaseBehavior
    scenarios: Scenarios
    label_delays: LabelDelays


@dataclass(frozen=True)
class Merchant:
    id: str
    external_id: str
    created_at: datetime
    category: int


@dataclass(frozen=True)
class Customer:
    index: int
    id: str
    external_id: str
    created_at: datetime
    region: int
    spend: float
    familiar_merchants: tuple[int, ...]
    devices: tuple[int, int]
    home_network: int
    address: int
    payment_methods: tuple[int, int]


@dataclass(frozen=True)
class Purchase:
    customer_id: str
    customer_external_id: str
    customer_created_at: datetime
    merchant_id: str
    merchant_external_id: str
    merchant_created_at: datetime
    category: str
    condition: str
    quantity: int
    unit_price: int
    device: str | None
    browser: str | None
    network: str | None
    shipping_region: str
    postal_code: str
    address_fingerprint: str
    payment_method: str
    scenario: str
    processor_outcome: str
    friendly_fraud: bool
    label_delay_days: int
    chargeback_reported: bool
    started_seconds: int


class World:
    def __init__(self, config: WorldConfig):
        self.config = config
        self.start_date = config.start_date
        self.digest = sha256(
            json.dumps(
                {"world": config.model_dump(mode="json"), "calibration": provenance()["sha256"]}, sort_keys=True
            ).encode()
        ).hexdigest()
        self._anchor = datetime.combine(config.start_date, time.min, tzinfo=UTC)
        self._merchant_cdf = np.cumsum(zipf_weights(config.counts.merchants, config.behavior.merchant_zipf_exponent))
        self._category_weights = zipf_weights(len(CATEGORIES), config.behavior.category_zipf_exponent)
        sigma = CALIBRATION["activity_log_sigma"]
        self._activity = self._rng(ACTIVITY_STAGE).lognormal(-(sigma**2) / 2, sigma, config.counts.customers)
        self._regions = self._rng(REGION_STAGE).choice(len(REGIONS), config.counts.customers, p=REGION_WEIGHTS)

    @classmethod
    def build(cls, config: WorldConfig) -> "World":
        return cls(config)

    def _rng(self, stage: int, *keys: int) -> np.random.Generator:
        return stage_rng(self.config.seed, stage, *keys)

    def merchant(self, index: int) -> Merchant:
        rng = self._rng(MERCHANT_STAGE, index)
        category = int(rng.choice(len(CATEGORIES), p=self._category_weights))
        age = int(rng.integers(30, 1500))
        external_id = f"seller-{index + 1:06d}"
        return Merchant(
            id=entity_id(self.config.integration_id, "merchant", external_id),
            external_id=external_id,
            created_at=self._anchor - timedelta(days=age),
            category=category,
        )

    def customer(self, index: int) -> Customer:
        counts = self.config.counts
        rng = self._rng(CUSTOMER_STAGE, index)
        region = int(self._regions[index])
        age = int(rng.integers(1, 2500))
        spend = float(rng.lognormal(0, CALIBRATION["spend_log_sigma"]))
        familiar = self._familiar_merchants(rng)
        home_network = int(rng.integers(counts.ip_networks))
        address = self._customer_address(rng, region)
        payment_methods = (int(rng.integers(counts.payment_methods)), int(rng.integers(counts.payment_methods)))
        external_id = f"buyer-{index + 1:06d}"
        shared = index // 2
        return Customer(
            index=index,
            id=entity_id(self.config.integration_id, "customer", external_id),
            external_id=external_id,
            created_at=self._anchor - timedelta(days=age),
            region=region,
            spend=spend,
            familiar_merchants=familiar,
            devices=(index % counts.devices, (shared + counts.customers) % counts.devices),
            home_network=home_network,
            address=address,
            payment_methods=payment_methods,
        )

    def _familiar_merchants(self, rng: np.random.Generator) -> tuple[int, ...]:
        count = self.config.counts.merchants
        chosen: list[int] = []
        while len(chosen) < min(3, count):
            candidate = min(int(np.searchsorted(self._merchant_cdf, rng.random())), count - 1)
            if candidate not in chosen:
                chosen.append(candidate)
        return tuple(chosen)

    def _customer_address(self, rng: np.random.Generator, region: int) -> int:
        addresses = self.config.counts.addresses
        if region >= addresses:
            return region % addresses
        return region + len(REGIONS) * int(rng.integers((addresses - 1 - region) // len(REGIONS) + 1))

    def _zipf_merchant(self, rng: np.random.Generator) -> int:
        return min(int(np.searchsorted(self._merchant_cdf, rng.random())), self.config.counts.merchants - 1)

    def processor_outcome(self, rng: np.random.Generator, scenario: int) -> str:
        behavior = self.config.behavior
        if scenario == CARD_TESTING:
            return "issuer_declined" if rng.random() < self.config.scenarios.card_testing.decline_rate else "succeeded"
        outcome = rng.random()
        if outcome < behavior.issuer_decline_rate:
            return "issuer_declined"
        if outcome < behavior.issuer_decline_rate + behavior.insufficient_funds_rate:
            return "insufficient_funds"
        return "succeeded"

    def purchase_rng(self, day: date, ordinal: int) -> np.random.Generator:
        return self._rng(PURCHASE_STAGE, (day - self.start_date).days, ordinal)

    def day_customers(self, day: date, customers: range) -> np.ndarray:
        rng = self._rng(PURCHASE_COUNT_STAGE, (day - self.start_date).days, customers.start, len(customers))
        window = slice(customers.start, customers.stop)
        intensity = np.array([hourly_weights(day, region)[1] for region in range(len(REGIONS))])
        weights = self._activity[window] * intensity[self._regions[window]]
        total = weights.sum()
        count = int(rng.poisson(self.config.behavior.purchases_per_customer_day * total))
        chosen = rng.choice(len(customers), size=count, p=weights / total)
        return (customers.start + chosen).astype(np.int64)

    def burst_count(self, day: date) -> int:
        rng = self._rng(BURST_COUNT_STAGE, (day - self.start_date).days)
        return int(rng.poisson(self.config.scenarios.card_testing.rate))

    def purchase(self, rng: np.random.Generator, day: date, ordinal: int, customer: int) -> Purchase:
        counts, behavior, scenarios = self.config.counts, self.config.behavior, self.config.scenarios
        offset = (day - self.start_date).days
        customer = self.customer(customer)
        scenario, window_start = self._scenario(rng, offset, customer.index)
        compromised = scenario in (STOLEN_CARD_NEW_DEVICE, ACCOUNT_TAKEOVER)
        campaign = self._rng(COMPROMISE_STAGE, scenario, window_start, customer.index, 1) if compromised else rng
        merchant_index = (
            int(customer.familiar_merchants[int(rng.integers(len(customer.familiar_merchants)))])
            if rng.random() < behavior.familiar_merchant_probability
            else self._zipf_merchant(rng)
        )
        merchant = self.merchant(merchant_index)
        condition = int(rng.choice(len(CONDITIONS), p=CONDITION_WEIGHTS))
        quantity = int(rng.integers(1, 4)) if merchant.category == ACCESSORY else 1
        typical = CATEGORY_PRICES[merchant.category] * CONDITION_PRICE_FACTORS[condition] * customer.spend
        unit_price = max(50, round(float(rng.lognormal(np.log(typical), CALIBRATION["purchase_log_sigma"]))))
        if merchant.category == ACCESSORY and rng.random() < 0.1:
            unit_price = int(rng.integers(50, 1501))
        if scenario == OBVIOUS_HIGH_VALUE:
            unit_price = int(round(unit_price * scenarios.obvious_high_value.amount_multiplier))
        elif compromised:
            settings = (
                scenarios.stolen_card_new_device if scenario == STOLEN_CARD_NEW_DEVICE else scenarios.account_takeover
            )
            unit_price = int(round(unit_price * settings.amount_multiplier))

        new_device = rng.random() >= behavior.familiar_context_probability
        if compromised:
            new_device = rng.random() < CALIBRATION["compromise_new_context_probability"]
        if new_device:
            device = int(campaign.integers(counts.devices))
        else:
            device = customer.devices[int(rng.integers(2))]
            if offset >= CALIBRATION["device_rotation_days"]:
                device = (device + offset // CALIBRATION["device_rotation_days"]) % counts.devices
        network = customer.home_network
        if rng.random() < CALIBRATION["network_rotation_probability"] or (compromised and new_device):
            network = int(campaign.integers(counts.ip_networks))
        address = customer.address
        address_change = CALIBRATION["compromise_address_change_probability"] if compromised else 0.02
        if rng.random() < address_change:
            address = int(rng.integers(counts.addresses))

        return self._assemble(
            rng,
            customer=customer,
            merchant=merchant,
            scenario=scenario,
            condition=condition,
            quantity=quantity,
            unit_price=unit_price,
            device=device,
            network=network,
            address=address,
            method=customer.payment_methods[int(rng.integers(2))],
            started_seconds=int(purchase_seconds(day, np.array([customer.region]), rng)[0]),
        )

    def card_test(self, rng: np.random.Generator, day: date, burst: int) -> Purchase:
        counts, testing = self.config.counts, self.config.scenarios.card_testing
        burst_rng = self._rng(BURST_STAGE, (day - self.start_date).days, burst)
        victim = self.customer(int(burst_rng.integers(counts.customers)))
        clock = int(burst_rng.integers(0, SECONDS_PER_DAY - testing.burst_minutes * 60))
        merchant = self.merchant(self._zipf_merchant(rng))
        return self._assemble(
            rng,
            customer=victim,
            merchant=merchant,
            scenario=CARD_TESTING,
            condition=int(rng.choice(len(CONDITIONS), p=CONDITION_WEIGHTS)),
            quantity=1,
            unit_price=int(rng.integers(50, 501)),
            device=int(rng.integers(counts.devices)),
            network=int(rng.integers(counts.ip_networks)),
            address=victim.address,
            method=int(rng.integers(counts.payment_methods)),
            started_seconds=clock + int(rng.integers(0, testing.burst_minutes * 60)),
        )

    def _scenario(self, rng: np.random.Generator, offset: int, customer: int) -> tuple[int, int]:
        scenarios, purchases_per_day = self.config.scenarios, self.config.behavior.purchases_per_customer_day
        for scenario, settings in (
            (STOLEN_CARD_NEW_DEVICE, scenarios.stolen_card_new_device),
            (ACCOUNT_TAKEOVER, scenarios.account_takeover),
        ):
            for start in range(max(0, offset - settings.window_days + 1), offset + 1):
                if self._rng(COMPROMISE_STAGE, scenario, start, customer).random() < settings.rate:
                    if rng.random() < min(1, settings.attempts_per_day / purchases_per_day):
                        return scenario, start
                    break
        if rng.random() < scenarios.obvious_high_value.rate:
            return OBVIOUS_HIGH_VALUE, -1
        return LEGITIMATE, -1

    def _assemble(
        self,
        rng: np.random.Generator,
        *,
        customer: Customer,
        merchant: Merchant,
        scenario: int,
        condition: int,
        quantity: int,
        unit_price: int,
        device: int,
        network: int,
        address: int,
        method: int,
        started_seconds: int,
    ) -> Purchase:
        key = self.config.synthetic_key
        delays = self.config.label_delays
        _, _, region, postal = address_parts(address)
        device_value = (
            fingerprint(key, "device", f"device-{device:06d}")
            if rng.random() >= CALIBRATION["device_missing_probability"]
            else None
        )
        network_value = (
            fingerprint(key, "ip_network", f"10.{network // 256}.{network % 256}.1")
            if rng.random() >= CALIBRATION["network_missing_probability"]
            else None
        )
        browser = str(rng.choice(CALIBRATION["browser_families"], p=CALIBRATION["browser_weights"]))
        if rng.random() < CALIBRATION["browser_missing_probability"]:
            browser = None
        friendly = scenario == LEGITIMATE and rng.random() < self.config.scenarios.friendly_fraud.rate
        return Purchase(
            customer_id=customer.id,
            customer_external_id=customer.external_id,
            customer_created_at=customer.created_at,
            merchant_id=merchant.id,
            merchant_external_id=merchant.external_id,
            merchant_created_at=merchant.created_at,
            category=CATEGORIES[merchant.category],
            condition=CONDITIONS[condition],
            quantity=quantity,
            unit_price=unit_price,
            device=device_value,
            browser=browser,
            network=network_value,
            shipping_region=region,
            postal_code=postal,
            address_fingerprint=fingerprint(key, "address", address_value(address)),
            payment_method=fingerprint(key, "payment_method", f"tok_card-{method:06d}_ok"),
            scenario=SCENARIO_NAMES[scenario],
            processor_outcome=self.processor_outcome(rng, scenario),
            friendly_fraud=friendly,
            label_delay_days=int(rng.integers(delays.min_days, delays.max_days + 1)),
            chargeback_reported=bool(rng.random() < delays.reporting_probability),
            started_seconds=started_seconds,
        )
