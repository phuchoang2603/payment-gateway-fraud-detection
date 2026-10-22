import json
from datetime import date, timedelta
from hashlib import sha256
from pathlib import Path
from typing import Literal, Self
from urllib.parse import urlsplit
from uuid import UUID

import yaml
from pydantic import BaseModel, ConfigDict, Field, model_validator


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


class DataProblems(StrictModel):
    duplicate_rate: float = Field(ge=0, lt=1)
    schema_cutover_date: date


class LabelDelays(StrictModel):
    min_days: int = Field(ge=0)
    max_days: int = Field(ge=0)

    @model_validator(mode="after")
    def validate_range(self) -> Self:
        if self.min_days > self.max_days:
            raise ValueError("label delay min_days must not exceed max_days")
        return self


class StorageLayout(StrictModel):
    row_group_rows: int = Field(default=131072, gt=0)
    target_file_mb: int = Field(default=128, gt=0)
    sort_memory_limit_mb: int = Field(default=256, ge=64)


ALGORITHM_VERSION = "historical-blocks-v2"
RUN_STATE_VERSION = 2


class Execution(StrictModel):
    workers: int = Field(default=1, gt=0)
    max_in_flight: int = Field(default=1, gt=0)
    scratch_directory: Path = Path("/tmp")
    scratch_capacity_bytes: int = Field(default=8 * 1024**3, gt=0)
    task_scratch_bytes: int = Field(default=256 * 1024**2, gt=0)
    duckdb_memory_limit_mb: int = Field(default=512, ge=64)
    duckdb_threads: int = Field(default=1, gt=0)
    duckdb_spill_bytes: int = Field(default=2 * 1024**3, ge=0)
    transfer_concurrency: int = Field(default=1, gt=0)

    @model_validator(mode="after")
    def validate_capacity(self) -> Self:
        if self.max_in_flight < self.workers:
            raise ValueError("max_in_flight must be at least workers")
        if self.max_in_flight * self.task_scratch_bytes + self.duckdb_spill_bytes >= self.scratch_capacity_bytes:
            raise ValueError("scratch capacity must exceed in-flight task reservations plus DuckDB spill")
        if self.transfer_concurrency > self.max_in_flight:
            raise ValueError("transfer_concurrency must not exceed max_in_flight")
        return self


class AcceptanceBand(StrictModel):
    minimum: float = Field(ge=0)
    maximum: float = Field(ge=0)

    @model_validator(mode="after")
    def validate_range(self) -> Self:
        if self.minimum > self.maximum:
            raise ValueError("acceptance minimum must not exceed maximum")
        return self


class HistoricalConfig(StrictModel):
    dataset_version: str = Field(pattern=r"^[a-zA-Z0-9][a-zA-Z0-9._-]*$")
    seed: int = Field(ge=0)
    integration_id: UUID
    synthetic_key: str = Field(min_length=1)
    synthetic_key_version: str = Field(min_length=1)
    start_date: date
    duration_days: int = Field(default=90, gt=1)
    chunk_purchases: int = Field(default=2000, gt=0)
    customer_block_size: int = Field(default=1000, gt=0)
    card_testing_bursts_per_task: int = Field(default=16, gt=0)
    minimum_source_parquet_bytes: int = Field(default=0, ge=0)
    accepted_version: bool = False
    validation_gate: Literal["native", "dp1-spark-gx"] = "native"
    execution: Execution = Field(default_factory=Execution)
    acceptance_bands: dict[str, AcceptanceBand] = Field(default_factory=dict)
    counts: EntityCounts
    behavior: PurchaseBehavior
    scenarios: Scenarios
    data_problems: DataProblems
    label_delays: LabelDelays
    storage: StorageLayout = Field(default_factory=StorageLayout)
    output: str = Field(min_length=1)

    @model_validator(mode="before")
    @classmethod
    def legacy_sort_budget(cls, values):
        if isinstance(values, dict) and "sort_memory_limit_mb" in values.get("storage", {}):
            values = {
                **values,
                "execution": {
                    "duckdb_memory_limit_mb": values["storage"]["sort_memory_limit_mb"],
                    **values.get("execution", {}),
                },
            }
        return values

    @property
    def logical_configuration(self) -> dict:
        values = self.model_dump(
            mode="json",
            exclude={
                "execution",
                "output",
                "chunk_purchases",
                "accepted_version",
                "acceptance_bands",
                "validation_gate",
            },
        )
        values["storage"].pop("sort_memory_limit_mb")
        return values

    def generation_identity(self, contracts: dict, revision: str) -> str:
        return sha256(
            json.dumps(
                {
                    "algorithm": ALGORITHM_VERSION,
                    "configuration": self.logical_configuration,
                    "contracts": contracts,
                    "generator_revision": revision,
                },
                sort_keys=True,
            ).encode()
        ).hexdigest()

    @property
    def end_date(self) -> date:
        return self.start_date + timedelta(days=self.duration_days)

    def schema_version_on(self, day: date) -> int:
        return 1 if day < self.data_problems.schema_cutover_date else 2

    @model_validator(mode="after")
    def validate_dates(self) -> Self:
        if not self.start_date < self.data_problems.schema_cutover_date < self.end_date:
            raise ValueError("data_problems.schema_cutover_date must fall strictly inside the dataset")
        allowed = {"duplicate_rate", "merchant_max_share", "merchant_top_10_share"}
        allowed.update(
            f"cardinality.{name}.distinct_to_row_ratio"
            for name in ("customer", "device", "ip_network", "address", "payment_method")
        )
        allowed.update(
            f"scenario_attempt_shares.{name}"
            for name in (
                "legitimate",
                "friendly_fraud",
                "obvious_high_value",
                "stolen_card_new_device",
                "account_takeover",
                "card_testing",
            )
        )
        allowed.update(("schema_version_shares.1", "schema_version_shares.2"))
        if set(self.acceptance_bands) - allowed:
            raise ValueError("unknown observed acceptance metric")
        parsed = urlsplit(self.output)
        if parsed.scheme and parsed.scheme != "s3":
            raise ValueError("output must be a local directory or s3:// bucket")
        if parsed.username or parsed.password or parsed.query or parsed.fragment:
            raise ValueError("output must be without credentials or query")
        if parsed.scheme == "s3" and not parsed.netloc:
            raise ValueError("S3 output requires a bucket")
        if any(part in ("..", ".") for part in parsed.path.split("/")):
            raise ValueError("output must not contain relative path components")
        return self


def load_config(path: Path) -> HistoricalConfig:
    return HistoricalConfig.model_validate(yaml.safe_load(path.read_text(encoding="utf-8")))
