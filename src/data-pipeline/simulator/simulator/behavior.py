from dataclasses import dataclass

import numpy as np

from simulator.calibration import CALIBRATION
from simulator.config import HistoricalConfig
from simulator.profiles import (
    ACCESSORY,
    ACCOUNT_TAKEOVER,
    CARD_TESTING,
    CATEGORY_PRICES,
    CONDITION_PRICE_FACTORS,
    CONDITION_WEIGHTS,
    CONDITIONS,
    OBVIOUS_HIGH_VALUE,
    STOLEN_CARD_NEW_DEVICE,
    Profiles,
    Schedule,
    address_parts,
    stage_rng,
)


@dataclass(frozen=True)
class PurchaseContext:
    category: int
    condition: int
    quantity: int
    unit_price: int
    device: str | None
    network: str | None
    ip_country: str | None
    address: str
    shipping_country: str
    method: str
    new_device: bool
    network_address: str | None
    address_value: str
    city: str
    region: str
    postal_code: str
    browser: str | None


def purchase_context(
    config: HistoricalConfig,
    profiles: Profiles,
    schedule: Schedule,
    index: int,
    offset: int,
    ordinal: int,
    random: np.random.Generator,
) -> PurchaseContext:
    customer = int(schedule.customers[index])
    kind = int(schedule.scenarios[index])
    group = int(schedule.groups[index])
    category = int(profiles.merchant_categories[schedule.merchants[index]])
    if kind == CARD_TESTING:
        category = ACCESSORY
    condition = int(random.choice(len(CONDITIONS), p=CONDITION_WEIGHTS))
    quantity = int(random.integers(1, 4)) if category == ACCESSORY else 1
    typical = CATEGORY_PRICES[category] * CONDITION_PRICE_FACTORS[condition] * profiles.spend_milli[customer] / 1000
    unit_price = max(50, round(float(random.lognormal(np.log(typical), CALIBRATION["purchase_log_sigma"]))))
    if category == ACCESSORY and random.random() < 0.1:
        unit_price = int(random.integers(50, 1501))
    device_index = int(profiles.familiar_devices[customer, int(random.integers(0, 2))])
    network_index = int(profiles.home_networks[customer])
    address_index = int(profiles.addresses[customer])
    method_index = int(profiles.payment_methods[customer, int(random.integers(0, 2))])
    new_device = random.random() >= config.behavior.familiar_context_probability
    compromised = kind in (STOLEN_CARD_NEW_DEVICE, ACCOUNT_TAKEOVER)
    if compromised:
        new_device = random.random() < CALIBRATION["compromise_new_context_probability"]
    campaign = stage_rng(config.seed, 41, kind, customer, max(group, 0)) if compromised else random
    if new_device:
        device_index = int(campaign.integers(0, len(profiles.device_pool)))
    elif offset >= CALIBRATION["device_rotation_days"]:
        device_index = (device_index + offset // CALIBRATION["device_rotation_days"]) % len(profiles.device_pool)
    if random.random() < CALIBRATION["network_rotation_probability"] or (compromised and new_device):
        network_index = int(campaign.integers(0, len(profiles.network_pool)))
    if random.random() < (CALIBRATION["compromise_address_change_probability"] if compromised else 0.02):
        address_index = int(random.integers(0, len(profiles.address_pool)))
    missing = random
    if kind == OBVIOUS_HIGH_VALUE:
        unit_price = round(unit_price * config.scenarios.obvious_high_value.amount_multiplier)
    elif compromised:
        settings = (
            config.scenarios.stolen_card_new_device
            if kind == STOLEN_CARD_NEW_DEVICE
            else config.scenarios.account_takeover
        )
        unit_price = round(unit_price * settings.amount_multiplier)
    elif kind == CARD_TESTING:
        campaign = stage_rng(config.seed, 40, offset, group)
        quantity = 1
        unit_price = int(random.integers(50, 501))
        device_index = int(campaign.integers(0, len(profiles.device_pool)))
        network_index = int(campaign.integers(0, len(profiles.network_pool)))
        method_index = (int(campaign.integers(0, len(profiles.method_pool))) + ordinal) % len(profiles.method_pool)
        missing = campaign
    device = (
        profiles.device_pool[device_index] if missing.random() >= CALIBRATION["device_missing_probability"] else None
    )
    network_address = f"10.{network_index // 256}.{network_index % 256}.1"
    network = profiles.network_pool[network_index]
    if missing.random() < CALIBRATION["network_missing_probability"]:
        network, network_address = None, None
    browser = str(random.choice(CALIBRATION["browser_families"], p=CALIBRATION["browser_weights"]))
    if random.random() < CALIBRATION["browser_missing_probability"]:
        browser = None
    line, city, region, postal = address_parts(address_index)
    return PurchaseContext(
        category,
        condition,
        quantity,
        unit_price,
        device,
        network,
        None,
        profiles.address_pool[address_index],
        "US",
        profiles.method_pool[method_index],
        new_device,
        network_address,
        line,
        city,
        region,
        postal,
        browser,
    )
