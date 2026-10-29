from collections.abc import Sequence
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from typing import Callable

import numpy as np

from simulator.calibration import CALIBRATION, REGIONS
from simulator.config import ALGORITHM_VERSION, HistoricalConfig
from simulator.identity import entity_id, fingerprint

CATEGORIES = ("phone", "laptop", "accessory", "tablet", "headphones", "smartwatch", "game_console", "camera")
CATEGORY_PRICES = np.array(CALIBRATION["category_prices_minor"])
CONDITIONS = ("excellent", "good", "fair")
CONDITION_WEIGHTS = np.array(CALIBRATION["condition_weights"])
CONDITION_PRICE_FACTORS = np.array(CALIBRATION["condition_price_factors"])
ACCESSORY = CATEGORIES.index("accessory")

LEGITIMATE = 0
OBVIOUS_HIGH_VALUE = 1
STOLEN_CARD_NEW_DEVICE = 2
ACCOUNT_TAKEOVER = 3
CARD_TESTING = 4
SCENARIO_NAMES = ("legitimate", "obvious_high_value", "stolen_card_new_device", "account_takeover", "card_testing")

PROFILE_STAGE = 1
SCHEDULE_STAGE = 2
COMPROMISE_STAGE = 3
CARD_TESTING_STAGE = 4
HISTORY_STAGE = 5
DUPLICATE_STAGE = 6


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


class IdentityPool(Sequence):
    def __init__(self, count: int, derive: Callable[[int], str]):
        self.count = count
        self.derive = derive

    def __len__(self):
        return self.count

    def __getitem__(self, index):
        if not 0 <= index < self.count:
            raise IndexError(index)
        return self.derive(int(index))


@dataclass(frozen=True)
class Profiles:
    merchant_ids: IdentityPool
    merchant_external_ids: IdentityPool
    merchant_categories: np.ndarray
    merchant_age_days: np.ndarray
    customer_ids: IdentityPool
    customer_external_ids: IdentityPool
    customer_age_days: np.ndarray
    customer_regions: np.ndarray
    activity_milli: np.ndarray
    spend_milli: np.ndarray
    familiar_merchants: np.ndarray
    familiar_devices: np.ndarray
    home_networks: np.ndarray
    addresses: np.ndarray
    payment_methods: np.ndarray
    device_pool: IdentityPool
    network_pool: IdentityPool
    address_pool: IdentityPool
    method_pool: IdentityPool
    identity_digest: str

    def digest(self) -> str:
        digest = sha256(self.identity_digest.encode())
        for name, value in sorted(vars(self).items()):
            if isinstance(value, np.ndarray):
                digest.update(name.encode())
                for start in range(0, len(value), 8192):
                    digest.update(np.ascontiguousarray(value[start : start + 8192], dtype=np.int64).tobytes())
        return digest.hexdigest()


@dataclass(frozen=True)
class Schedule:
    customers: np.ndarray
    merchants: np.ndarray
    seconds: np.ndarray
    scenarios: np.ndarray
    groups: np.ndarray

    def __len__(self) -> int:
        return len(self.customers)


def build_profiles(config: HistoricalConfig, directory: Path | None = None, *, reopen: bool = False) -> Profiles:
    counts = config.counts
    random = stage_rng(config.seed, PROFILE_STAGE)
    key = config.synthetic_key
    arrays = {}
    if directory:
        directory.mkdir(parents=True, exist_ok=True)

    def array(name, shape, generate):
        if directory and reopen:
            value = np.load(directory / f"{name}.npy", mmap_mode="r")
        else:
            value = (
                np.lib.format.open_memmap(directory / f"{name}.npy", mode="w+", dtype="int64", shape=shape)
                if directory
                else np.empty(shape, dtype="int64")
            )
            for start in range(0, shape[0], 8192):
                stop = min(start + 8192, shape[0])
                value[start:stop] = generate(stop - start)
            if directory:
                value.flush()
                value = np.load(directory / f"{name}.npy", mmap_mode="r")
            else:
                value.flags.writeable = False
        arrays[name] = value

    category_weights = zipf_weights(len(CATEGORIES), config.behavior.category_zipf_exponent)
    merchant_cdf = np.cumsum(zipf_weights(counts.merchants, config.behavior.merchant_zipf_exponent))
    familiar_count = min(3, counts.merchants)

    def familiar(size):
        result = np.empty((size, familiar_count), dtype="int64")
        for column in range(familiar_count):
            result[:, column] = np.searchsorted(merchant_cdf, random.random(size))
            collisions = np.any(result[:, :column] == result[:, column : column + 1], axis=1)
            while collisions.any():
                result[collisions, column] = np.searchsorted(merchant_cdf, random.random(int(collisions.sum())))
                collisions = np.any(result[:, :column] == result[:, column : column + 1], axis=1)
        return result

    array("merchant_categories", (counts.merchants,), lambda n: random.choice(len(CATEGORIES), n, p=category_weights))
    array("merchant_age_days", (counts.merchants,), lambda n: random.integers(30, 1500, n))
    array("customer_age_days", (counts.customers,), lambda n: random.integers(1, 2500, n))
    array("customer_regions", (counts.customers,), lambda n: random.choice(len(REGIONS), n, p=[r[4] for r in REGIONS]))
    sigma = CALIBRATION["activity_log_sigma"]
    array(
        "activity_milli",
        (counts.customers,),
        lambda n: np.maximum(1, random.lognormal(-(sigma**2) / 2, sigma, n) * 1000),
    )
    array(
        "spend_milli",
        (counts.customers,),
        lambda n: np.maximum(1, random.lognormal(0, CALIBRATION["spend_log_sigma"], n) * 1000),
    )
    array("familiar_merchants", (counts.customers, familiar_count), familiar)
    device_cursor = 0

    def devices(n):
        nonlocal device_cursor
        customers = np.arange(device_cursor, device_cursor + n)
        device_cursor += n
        shared = customers // 2
        return np.column_stack((customers % counts.devices, (shared + counts.customers) % counts.devices))

    array("familiar_devices", (counts.customers, 2), devices)
    array("home_networks", (counts.customers,), lambda n: random.integers(0, counts.ip_networks, n))
    address_cursor = 0

    def addresses(n):
        nonlocal address_cursor
        regions = arrays["customer_regions"][address_cursor : address_cursor + n]
        address_cursor += n
        return np.array(
            [
                int(r) + len(REGIONS) * int(random.integers(0, (counts.addresses - 1 - int(r)) // len(REGIONS) + 1))
                if int(r) < counts.addresses
                else int(r) % counts.addresses
                for r in regions
            ]
        )

    array("addresses", (counts.customers,), addresses)
    array("payment_methods", (counts.customers, 2), lambda n: random.integers(0, counts.payment_methods, (n, 2)))
    merchant_external = IdentityPool(counts.merchants, lambda i: f"seller-{i + 1:06d}")
    customer_external = IdentityPool(counts.customers, lambda i: f"buyer-{i + 1:06d}")
    identity_digest = sha256(f"{config.integration_id}:{key}:{counts.model_dump_json()}".encode()).hexdigest()
    return Profiles(
        **arrays,
        merchant_ids=IdentityPool(
            counts.merchants, lambda i: entity_id(config.integration_id, "merchant", merchant_external[i])
        ),
        merchant_external_ids=merchant_external,
        customer_ids=IdentityPool(
            counts.customers, lambda i: entity_id(config.integration_id, "customer", customer_external[i])
        ),
        customer_external_ids=customer_external,
        device_pool=IdentityPool(counts.devices, lambda i: fingerprint(key, "device", f"device-{i:06d}")),
        network_pool=IdentityPool(
            counts.ip_networks, lambda i: fingerprint(key, "ip_network", f"10.{i // 256}.{i % 256}.1")
        ),
        address_pool=IdentityPool(counts.addresses, lambda i: fingerprint(key, "address", address_value(i))),
        method_pool=IdentityPool(
            counts.payment_methods, lambda i: fingerprint(key, "payment_method", f"tok_card-{i:06d}_ok")
        ),
        identity_digest=identity_digest,
    )


def profiles_from_manifest(manifest: dict, directory: Path | None = None) -> Profiles:
    if manifest.get("algorithm_version") != ALGORITHM_VERSION:
        raise ValueError("unsupported historical generation algorithm for continuation")
    profiles = build_profiles(HistoricalConfig.from_manifest(manifest), directory)
    if profiles.digest() != manifest["profile_sha256"]:
        raise ValueError("historical profile digest mismatch for continuation")
    return profiles
