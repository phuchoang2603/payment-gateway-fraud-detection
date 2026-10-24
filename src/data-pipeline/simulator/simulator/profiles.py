from collections.abc import Sequence
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from typing import Callable

import numpy as np

from simulator.config import ALGORITHM_VERSION, HistoricalConfig, load_published_config
from simulator.identity import entity_id, fingerprint

CATEGORIES = ("phone", "laptop", "accessory", "tablet", "headphones", "smartwatch", "game_console", "camera")
CATEGORY_PRICES = np.array([35000, 65000, 3000, 30000, 12000, 18000, 28000, 45000])
CONDITIONS = ("excellent", "good", "fair")
CONDITION_WEIGHTS = np.array([0.4, 0.4, 0.2])
CONDITION_PRICE_FACTORS = np.array([1.0, 0.88, 0.75])
ACCESSORY = CATEGORIES.index("accessory")
COUNTRIES = ("US", "CA", "GB")
COUNTRY_WEIGHTS = np.array([0.8, 0.12, 0.08])
REGIONS = {"US": ("NY", "10001"), "CA": ("ON", "M5V 2T6"), "GB": ("LDN", "EC1A 1BB")}

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
    customer_countries: np.ndarray
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
    array("customer_countries", (counts.customers,), lambda n: random.choice(len(COUNTRIES), n, p=COUNTRY_WEIGHTS))
    array("familiar_merchants", (counts.customers, familiar_count), familiar)
    array("familiar_devices", (counts.customers, 2), lambda n: random.integers(0, counts.devices, (n, 2)))
    array("home_networks", (counts.customers,), lambda n: random.integers(0, counts.ip_networks, n))
    array("addresses", (counts.customers,), lambda n: random.integers(0, counts.addresses, n))
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
        address_pool=IdentityPool(
            counts.addresses, lambda i: fingerprint(key, "address", f"{i} Market Street, Example City")
        ),
        method_pool=IdentityPool(
            counts.payment_methods, lambda i: fingerprint(key, "payment_method", f"tok_card-{i:06d}_ok")
        ),
        identity_digest=identity_digest,
    )


@dataclass(frozen=True)
class GenerationTask:
    offset: int
    kind: int
    block: int

    @property
    def key(self):
        return f"day-{self.offset:05d}-kind-{self.kind}-block-{self.block:08d}"

    def ordinal(self, index: int) -> int:
        if index >= 2**32:
            raise ValueError("logical task exceeds purchase ordinal capacity")
        return ((self.block * 2 + self.kind) << 32) + index


def tasks_for_day(config: HistoricalConfig, offset: int):
    for block in range((config.counts.customers + config.customer_block_size - 1) // config.customer_block_size):
        yield GenerationTask(offset, 0, block)
    bursts = stage_rng(config.seed, CARD_TESTING_STAGE, offset).poisson(config.scenarios.card_testing.rate)
    for block in range((int(bursts) + config.card_testing_bursts_per_task - 1) // config.card_testing_bursts_per_task):
        yield GenerationTask(offset, 1, block)


def compromised_on(config: HistoricalConfig, scenario: int, offset: int, block: int | None = None) -> np.ndarray:
    settings = (
        config.scenarios.stolen_card_new_device
        if scenario == STOLEN_CARD_NEW_DEVICE
        else config.scenarios.account_takeover
    )
    if block is None:
        return np.concatenate(
            [
                compromised_on(config, scenario, offset, i)
                for i in range(
                    (config.counts.customers + config.customer_block_size - 1) // config.customer_block_size
                )
            ]
        )
    start = block * config.customer_block_size
    count = min(config.customer_block_size, config.counts.customers - start)
    random = stage_rng(config.seed, COMPROMISE_STAGE, scenario, offset, block)
    return start + np.flatnonzero(random.random(count) < settings.rate)


def active_compromises(config: HistoricalConfig, scenario: int, offset: int, block: int) -> np.ndarray:
    """Recover only this block's active windows without replaying preceding purchases."""
    settings = (
        config.scenarios.stolen_card_new_device
        if scenario == STOLEN_CARD_NEW_DEVICE
        else config.scenarios.account_takeover
    )
    starts = [
        np.column_stack([selected, np.full(len(selected), start)])
        for start in range(max(0, offset - settings.window_days + 1), offset + 1)
        if len(selected := compromised_on(config, scenario, start, block))
    ]
    return np.concatenate(starts) if starts else np.empty((0, 2), dtype=np.int64)


def schedule_for_task(config: HistoricalConfig, task: GenerationTask, profiles: Profiles) -> Schedule:
    behavior, scenarios = config.behavior, config.scenarios
    random = stage_rng(config.seed, SCHEDULE_STAGE, task.offset, task.kind, task.block)
    weights = zipf_weights(config.counts.merchants, behavior.merchant_zipf_exponent)
    parts = []
    if task.kind == 0:
        start = task.block * config.customer_block_size
        stop = min(start + config.customer_block_size, config.counts.customers)
        customers = np.repeat(
            np.arange(start, stop), random.poisson(behavior.purchases_per_customer_day, stop - start)
        )
        familiar = profiles.familiar_merchants[
            customers, random.integers(0, profiles.familiar_merchants.shape[1], len(customers))
        ]
        merchants = np.where(
            random.random(len(customers)) < behavior.familiar_merchant_probability,
            familiar,
            random.choice(len(weights), len(customers), p=weights),
        )
        parts.append(
            (
                customers,
                merchants,
                random.integers(0, 86400, len(customers)),
                np.where(
                    random.random(len(customers)) < scenarios.obvious_high_value.rate, OBVIOUS_HIGH_VALUE, LEGITIMATE
                ),
                np.full(len(customers), -1),
            )
        )
        for scenario, settings in (
            (STOLEN_CARD_NEW_DEVICE, scenarios.stolen_card_new_device),
            (ACCOUNT_TAKEOVER, scenarios.account_takeover),
        ):
            active = active_compromises(config, scenario, task.offset, task.block)
            attempts = random.poisson(settings.attempts_per_day, len(active))
            victims = np.repeat(active[:, 0], attempts)
            parts.append(
                (
                    victims,
                    profiles.familiar_merchants[victims, 0],
                    random.integers(0, 86400, len(victims)),
                    np.full(len(victims), scenario),
                    np.repeat(active[:, 1], attempts),
                )
            )
    else:
        bursts = int(stage_rng(config.seed, CARD_TESTING_STAGE, task.offset).poisson(scenarios.card_testing.rate))
        start = task.block * config.card_testing_bursts_per_task
        for burst in range(start, min(start + config.card_testing_bursts_per_task, bursts)):
            testing = scenarios.card_testing
            clock = int(random.integers(0, 86400 - testing.burst_minutes * 60))
            parts.append(
                (
                    np.full(testing.burst_size, int(random.integers(0, config.counts.customers))),
                    np.full(testing.burst_size, int(random.choice(len(weights), p=weights))),
                    np.sort(clock + random.integers(0, testing.burst_minutes * 60, testing.burst_size)),
                    np.full(testing.burst_size, CARD_TESTING),
                    np.full(testing.burst_size, burst),
                )
            )
    columns = [np.concatenate(column).astype(np.int64) for column in zip(*parts)]
    return Schedule(*columns)


def profiles_from_manifest(manifest: dict, directory: Path | None = None) -> Profiles:
    if manifest.get("algorithm_version") != ALGORITHM_VERSION:
        raise ValueError("unsupported historical generation algorithm for continuation")
    profiles = build_profiles(load_published_config(manifest["configuration"]), directory)
    if profiles.digest() != manifest["profile_sha256"]:
        raise ValueError("historical profile digest mismatch for continuation")
    return profiles
