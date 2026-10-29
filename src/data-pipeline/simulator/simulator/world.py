import hmac
import ipaddress
import json
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from functools import cache, lru_cache
from hashlib import sha256
from importlib.resources import files
from typing import Self
from uuid import UUID, uuid5
from zoneinfo import ZoneInfo

import numpy as np
import pytz
from pydantic import BaseModel, ConfigDict, Field, model_validator

CALIBRATION_BYTES = files("simulator").joinpath("us-calibration.json").read_bytes()
CALIBRATION = json.loads(CALIBRATION_BYTES)
REGIONS = tuple(CALIBRATION["regions"])
IDENTITY_NAMESPACE = UUID("71088b85-53a3-5ed5-bf27-8ea372bf796d")

CATEGORIES = ("phone", "laptop", "accessory", "tablet", "headphones", "smartwatch", "game_console", "camera")
CATEGORY_PRICES = np.array(CALIBRATION["category_prices_minor"])
CONDITIONS = ("excellent", "good", "fair")
CONDITION_WEIGHTS = np.array(CALIBRATION["condition_weights"])
CONDITION_PRICE_FACTORS = np.array(CALIBRATION["condition_price_factors"])
ACCESSORY = CATEGORIES.index("accessory")
REGION_WEIGHTS = np.array([region[4] for region in REGIONS])
FOREIGN_COUNTRIES = ("CA", "GB", "DE", "MX")
HOME_COUNTRY = "US"

LEGITIMATE = 0
OBVIOUS_HIGH_VALUE = 1
STOLEN_CARD_NEW_DEVICE = 2
ACCOUNT_TAKEOVER = 3
CARD_TESTING = 4
SCENARIOS = ("legitimate", "obvious_high_value", "stolen_card_new_device", "account_takeover", "card_testing")

ACTIVITY_STAGE = 300
REGION_STAGE = 301
MERCHANT_STAGE = 302
CUSTOMER_STAGE = 303
COMPROMISE_STAGE = 304
PURCHASE_COUNT_STAGE = 305
BURST_COUNT_STAGE = 306
BURST_STAGE = 307
PURCHASE_STAGE = 308
BURST_PURCHASE_STAGE = 309
SECONDS_PER_DAY = 86400


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)


class EntityCounts(StrictModel):
    customers: int = Field(gt=0)
    merchants: int = Field(gt=0)
    devices: int = Field(gt=0)
    ip_networks: int = Field(gt=0, le=65536)
    addresses: int = Field(gt=0)
    payment_methods: int = Field(gt=0)


class PurchaseBehavior(StrictModel):
    purchases_per_customer_day: float = Field(gt=0)
    merchant_zipf_exponent: float = Field(gt=0)
    category_zipf_exponent: float = Field(gt=0)
    familiar_merchant_probability: float = Field(ge=0, le=1)
    familiar_context_probability: float = Field(ge=0, le=1)
    foreign_ip_probability: float = Field(ge=0, le=1)
    issuer_decline_rate: float = Field(ge=0, le=1)
    insufficient_funds_rate: float = Field(ge=0, le=1)
    unknown_outcome_rate: float = Field(ge=0, le=1)
    unknown_resolution_minutes: int = Field(gt=0)
    retry_rate: float = Field(ge=0, le=1)
    expiry_minutes: int = Field(gt=0)

    @model_validator(mode="after")
    def validate_outcomes(self) -> Self:
        if self.issuer_decline_rate + self.insufficient_funds_rate + self.unknown_outcome_rate > 1:
            raise ValueError("processor outcome rates must sum to at most one")
        return self


class HighValueScenario(StrictModel):
    rate: float = Field(ge=0, le=1)
    amount_multiplier: float = Field(gt=1)


class CompromiseScenario(StrictModel):
    rate: float = Field(ge=0, le=1, description="Daily probability that a customer becomes compromised")
    window_days: int = Field(gt=0)
    attempts_per_day: float = Field(gt=0)
    amount_multiplier: float = Field(gt=0)


class CardTestingScenario(StrictModel):
    rate: float = Field(ge=0, description="Expected bursts per day")
    burst_size: int = Field(gt=1)
    burst_minutes: int = Field(gt=0, le=60)
    decline_rate: float = Field(ge=0, le=1)


class FriendlyFraudScenario(StrictModel):
    rate: float = Field(ge=0, le=1)


class Scenarios(StrictModel):
    obvious_high_value: HighValueScenario
    stolen_card_new_device: CompromiseScenario
    account_takeover: CompromiseScenario
    card_testing: CardTestingScenario
    friendly_fraud: FriendlyFraudScenario


class LabelDelays(StrictModel):
    reporting_probability: float = Field(default=0.85, ge=0, le=1)
    min_days: int = Field(ge=0)
    max_days: int = Field(ge=0)

    @model_validator(mode="after")
    def validate_range(self) -> Self:
        if self.min_days > self.max_days:
            raise ValueError("label delay min_days must not exceed max_days")
        return self


class WorldConfig(StrictModel):
    seed: int = Field(ge=0)
    integration_id: UUID
    synthetic_key: str = Field(min_length=1)
    start_date: date
    counts: EntityCounts
    behavior: PurchaseBehavior
    scenarios: Scenarios
    label_delays: LabelDelays


def entity_id(integration_id: UUID, kind: str, external_id: str) -> str:
    if kind not in ("merchant", "customer"):
        raise ValueError(f"unsupported entity kind: {kind}")
    return str(uuid5(IDENTITY_NAMESPACE, f"{integration_id}:{kind}:{external_id}"))


def fingerprint(synthetic_key: str, kind: str, value: str) -> str:
    if kind == "ip_network":
        address = ipaddress.ip_address(value)
        prefix = 24 if address.version == 4 else 48
        normalized = str(ipaddress.ip_network(f"{address}/{prefix}", strict=False))
    elif kind == "payment_method":
        if not value.startswith("tok_") or value.rsplit("_", maxsplit=1)[-1] not in ("ok", "nsf", "issuer", "invalid"):
            raise ValueError("payment method must be a synthetic token")
        normalized = value[4:].rsplit("_", maxsplit=1)[0]
    elif kind in ("address", "device"):
        normalized = " ".join(value.casefold().split())
    else:
        raise ValueError(f"unsupported fingerprint kind: {kind}")
    return hmac.new(synthetic_key.encode(), normalized.encode(), sha256).hexdigest()


def stage_rng(seed: int, stage: int, *keys: int) -> np.random.Generator:
    return np.random.default_rng(np.random.SeedSequence([seed, stage, *keys]))


def zipf_weights(count: int, exponent: float) -> np.ndarray:
    weights = np.arange(1, count + 1, dtype=float) ** (-exponent)
    return weights / weights.sum()


def address_parts(index: int):
    region, postal, city, _, _ = REGIONS[index % len(REGIONS)]
    return f"{index + 1} Market Street", city, region, postal


def address_value(index: int) -> str:
    line, city, region, postal = address_parts(index)
    return f"{line} {city} {region} {postal} US"


@cache
def zone(index: int) -> ZoneInfo:
    name = REGIONS[index][3]
    with files("pytz").joinpath("zoneinfo", name).open("rb") as stream:
        return ZoneInfo.from_file(stream, key=name)


@cache
def hourly_weights(day: date, region: int):
    midnight = datetime.combine(day, time.min, UTC)
    local = [(midnight + timedelta(hours=hour, minutes=30)).astimezone(zone(region)) for hour in range(24)]
    weights = np.array([CALIBRATION["local_hour_weights"][value.hour] for value in local])
    intensity = np.array(
        [
            CALIBRATION["weekday_multipliers"][value.weekday()] * CALIBRATION["month_multipliers"][value.month - 1]
            for value in local
        ]
    )
    return weights * intensity / (weights * intensity).sum(), float(np.average(intensity, weights=weights))


def purchase_seconds(day: date, regions, rng: np.random.Generator) -> np.ndarray:
    result = np.empty(len(regions), dtype=np.int64)
    for region in np.unique(regions):
        selected = np.flatnonzero(regions == region)
        weights, _ = hourly_weights(day, int(region))
        result[selected] = rng.choice(24, len(selected), p=weights) * 3600 + rng.integers(0, 3600, len(selected))
    return result


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
    network_address: str | None
    ip_country: str | None
    shipping_line: str
    shipping_city: str
    shipping_region: str
    postal_code: str
    address_fingerprint: str
    payment_method: str
    payment_method_index: int
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
                {
                    "world": config.model_dump(mode="json"),
                    "calibration_sha256": sha256(CALIBRATION_BYTES).hexdigest(),
                    "tzdb": pytz.__version__,
                },
                sort_keys=True,
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

    def _offset(self, day: date) -> int:
        return (day - self.start_date).days

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

    def payment_method_fingerprint(self, index: int) -> str:
        return fingerprint(self.config.synthetic_key, "payment_method", f"tok_card-{index:06d}_ok")

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

    @lru_cache(maxsize=8)
    def _day_counts(self, day: date) -> np.ndarray:
        rng = self._rng(PURCHASE_COUNT_STAGE, self._offset(day))
        intensity = np.array([hourly_weights(day, region)[1] for region in range(len(REGIONS))])
        rates = self.config.behavior.purchases_per_customer_day * self._activity * intensity[self._regions]
        return rng.poisson(rates)

    def day_purchases(self, day: date, customers: range) -> list[tuple[int, int]]:
        counts = self._day_counts(day)[customers.start : customers.stop]
        customer_of = np.repeat(np.arange(customers.start, customers.stop), counts)
        starts = np.cumsum(counts) - counts
        number = np.arange(len(customer_of)) - np.repeat(starts, counts)
        return list(zip(customer_of.tolist(), number.tolist()))

    def purchase_rng(self, day: date, customer: int, number: int) -> np.random.Generator:
        return self._rng(PURCHASE_STAGE, self._offset(day), customer, number)

    def burst_count(self, day: date) -> int:
        rng = self._rng(BURST_COUNT_STAGE, self._offset(day))
        return int(rng.poisson(self.config.scenarios.card_testing.rate))

    def burst_rng(self, day: date, burst: int, number: int) -> np.random.Generator:
        return self._rng(BURST_PURCHASE_STAGE, self._offset(day), burst, number)

    def purchase(self, rng: np.random.Generator, day: date, customer: int) -> Purchase:
        counts, behavior, scenarios = self.config.counts, self.config.behavior, self.config.scenarios
        offset = self._offset(day)
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
        ip_country = HOME_COUNTRY
        if rng.random() < behavior.foreign_ip_probability:
            ip_country = str(rng.choice(FOREIGN_COUNTRIES))
        if scenario == ACCOUNT_TAKEOVER or (scenario == STOLEN_CARD_NEW_DEVICE and rng.random() < 0.5):
            ip_country = str(rng.choice(FOREIGN_COUNTRIES))

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
            ip_country=ip_country,
            started_seconds=int(purchase_seconds(day, np.array([customer.region]), rng)[0]),
        )

    def card_test(self, rng: np.random.Generator, day: date, burst: int) -> Purchase:
        counts, testing = self.config.counts, self.config.scenarios.card_testing
        burst_rng = self._rng(BURST_STAGE, self._offset(day), burst)
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
            ip_country=HOME_COUNTRY,
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
        ip_country: str,
        started_seconds: int,
    ) -> Purchase:
        key = self.config.synthetic_key
        delays = self.config.label_delays
        line, city, region, postal = address_parts(address)
        device_missing = rng.random() < CALIBRATION["device_missing_probability"]
        network_missing = rng.random() < CALIBRATION["network_missing_probability"]
        browser = str(rng.choice(CALIBRATION["browser_families"], p=CALIBRATION["browser_weights"]))
        if rng.random() < CALIBRATION["browser_missing_probability"]:
            browser = None
        friendly = scenario == LEGITIMATE and rng.random() < self.config.scenarios.friendly_fraud.rate
        network_address = None if network_missing else f"10.{network // 256}.{network % 256}.1"
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
            device=None if device_missing else fingerprint(key, "device", f"device-{device:06d}"),
            browser=browser,
            network=None if network_missing else fingerprint(key, "ip_network", network_address),
            network_address=network_address,
            ip_country=ip_country,
            shipping_line=line,
            shipping_city=city,
            shipping_region=region,
            postal_code=postal,
            address_fingerprint=fingerprint(key, "address", address_value(address)),
            payment_method=self.payment_method_fingerprint(method),
            payment_method_index=method,
            scenario=SCENARIOS[scenario],
            processor_outcome=self.processor_outcome(rng, scenario),
            friendly_fraud=friendly,
            label_delay_days=int(rng.integers(delays.min_days, delays.max_days + 1)),
            chargeback_reported=bool(rng.random() < delays.reporting_probability),
            started_seconds=started_seconds,
        )
