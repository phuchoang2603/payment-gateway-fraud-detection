from dataclasses import dataclass

import numpy as np

from simulator.config import HistoricalConfig
from simulator.identity import fingerprint
from simulator.profiles import (
    ACCESSORY,
    ACCOUNT_TAKEOVER,
    CARD_TESTING,
    CATEGORY_PRICES,
    CONDITION_PRICE_FACTORS,
    CONDITION_WEIGHTS,
    CONDITIONS,
    COUNTRIES,
    OBVIOUS_HIGH_VALUE,
    STOLEN_CARD_NEW_DEVICE,
    Profiles,
    Schedule,
)


@dataclass(frozen=True)
class PurchaseContext:
    category: int
    condition: int
    quantity: int
    unit_price: int
    device: str
    network: str
    ip_country: str
    address: str
    shipping_country: str
    method: str
    new_device: bool
    network_address: str
    address_value: str


def purchase_context(
    config: HistoricalConfig,
    profiles: Profiles,
    schedule: Schedule,
    index: int,
    offset: int,
    ordinal: int,
    random: np.random.Generator,
) -> PurchaseContext:
    key = config.synthetic_key
    customer = int(schedule.customers[index])
    kind = int(schedule.scenarios[index])
    group = int(schedule.groups[index])
    category = int(profiles.merchant_categories[schedule.merchants[index]])
    if kind == CARD_TESTING:
        category = ACCESSORY
    condition = int(random.choice(len(CONDITIONS), p=CONDITION_WEIGHTS))
    quantity = int(random.integers(1, 4)) if category == ACCESSORY else 1
    typical = CATEGORY_PRICES[category] * CONDITION_PRICE_FACTORS[condition]
    unit_price = max(50, round(float(random.lognormal(np.log(typical), 0.3))))
    home_country = COUNTRIES[profiles.customer_countries[customer]]
    device = profiles.device_pool[profiles.familiar_devices[customer, int(random.integers(0, 2))]]
    network_index = int(profiles.home_networks[customer])
    network_address = f"10.{network_index // 256}.{network_index % 256}.1"
    network = profiles.network_pool[network_index]
    address_value = f"{int(profiles.addresses[customer])} Market Street, Example City"
    address = profiles.address_pool[profiles.addresses[customer]]
    method = profiles.method_pool[profiles.payment_methods[customer, 0]]
    ip_country = home_country
    new_device = False
    if random.random() >= config.behavior.familiar_context_probability:
        device = fingerprint(key, "device", f"new-{customer}-{offset}-{ordinal}")
        new_device = True
    if random.random() < config.behavior.foreign_ip_probability:
        ip_country = COUNTRIES[(COUNTRIES.index(home_country) + 1) % len(COUNTRIES)]

    foreign = COUNTRIES[(COUNTRIES.index(home_country) + 1 + int(random.integers(0, 2))) % len(COUNTRIES)]
    if kind == OBVIOUS_HIGH_VALUE:
        unit_price = round(unit_price * config.scenarios.obvious_high_value.amount_multiplier)
    elif kind == STOLEN_CARD_NEW_DEVICE:
        unit_price = round(unit_price * config.scenarios.stolen_card_new_device.amount_multiplier)
        device = fingerprint(key, "device", f"stolen-{customer}-{group}")
        network_address = f"10.250.{(customer + group) % 256}.1"
        network = fingerprint(key, "ip_network", network_address)
        ip_country = foreign if random.random() < 0.5 else home_country
        new_device = True
    elif kind == ACCOUNT_TAKEOVER:
        unit_price = round(unit_price * config.scenarios.account_takeover.amount_multiplier)
        device = fingerprint(key, "device", f"takeover-{customer}-{group}")
        network_address = f"10.252.{(customer + group) % 256}.1"
        network = fingerprint(key, "ip_network", network_address)
        address_value = f"takeover-{customer}-{group}"
        address = fingerprint(key, "address", address_value)
        ip_country = foreign
        new_device = True
    elif kind == CARD_TESTING:
        quantity = 1
        unit_price = int(random.integers(50, 501))
        device = fingerprint(key, "device", f"card-testing-{offset}-{group}")
        network_address = f"10.251.{(offset + group) % 256}.1"
        network = fingerprint(key, "ip_network", network_address)
        method = fingerprint(key, "payment_method", f"tok_card-test-{offset}-{ordinal}_ok")
        new_device = True
    return PurchaseContext(
        category,
        condition,
        quantity,
        unit_price,
        device,
        network,
        ip_country,
        address,
        home_country,
        method,
        new_device,
        network_address,
        address_value,
    )
