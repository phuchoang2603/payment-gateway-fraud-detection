"""Validate live traffic rates, delay bounds and committed-event redelivery settings."""

from pathlib import Path

import yaml
from pydantic import Field, model_validator

from simulator.world import StrictModel


class Redelivery(StrictModel):
    """Configure deterministic duplicate injection from committed Kafka events."""

    enabled: bool = False
    group: str = "simulator-redelivery-v1"
    topic: str = "payments.events.v1"
    rate: float = Field(default=0.015, ge=0, le=1)


class DriftStep(StrictModel):
    """From `after_seconds` of the run, draw purchases with these values instead of the manifest's."""

    after_seconds: float = Field(ge=0)
    foreign_ip_probability: float | None = Field(default=None, ge=0, le=1)
    familiar_context_probability: float | None = Field(default=None, ge=0, le=1)
    amount_scale: float | None = Field(default=None, gt=0)

    @model_validator(mode="after")
    def overrides(self):
        if (
            self.foreign_ip_probability is None
            and self.familiar_context_probability is None
            and self.amount_scale is None
        ):
            raise ValueError("a drift step must override at least one value")
        return self

    def behavior(self):
        """Returns the step's overrides of the world's behaviour values."""
        values = {
            "foreign_ip_probability": self.foreign_ip_probability,
            "familiar_context_probability": self.familiar_context_probability,
        }
        return {name: value for name, value in values.items() if value is not None}


def drift_at(steps, elapsed):
    """Returns the latest step that has started by `elapsed` seconds, or None before the first."""
    started = [step for step in steps if step.after_seconds <= elapsed]
    return max(started, key=lambda step: step.after_seconds, default=None)


class LiveConfig(StrictModel):
    """Bound checkout concurrency, label queues and store-and-forward delays."""

    manifest: str
    manifest_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    gateway_url: str = "http://edge.payment-gateway.svc:8080"
    return_url: str = Field(default="https://example.com/checkout/complete", pattern=r"^https://")
    kafka_brokers: str = "payment-gateway-kafka-kafka-bootstrap.payment-gateway.svc:9092"
    label_topic: str = "fraud.labels.v1"
    seed: int = Field(default=2603, ge=0)
    duration_seconds: float = Field(default=300, ge=0)
    payments_per_second: float = Field(default=1, gt=0)
    concurrency: int = Field(default=4, gt=0, le=128)
    request_timeout_seconds: float = Field(default=15, gt=0)
    request_retries: int = Field(default=4, ge=0, le=20)
    label_time_scale: float = Field(default=1, gt=0)
    max_pending_labels: int = Field(default=10000, gt=0)
    late_rate: float = Field(default=0, ge=0, le=1)
    late_min_seconds: float = Field(default=300, ge=0)
    late_max_seconds: float = Field(default=600, ge=0)
    max_held_attempts: int = Field(default=1000, gt=0)
    redelivery: Redelivery = Field(default_factory=Redelivery)
    drift: list[DriftStep] = Field(default_factory=list)

    @model_validator(mode="after")
    def late_range(self):
        """Reject an inverted store-and-forward delay interval."""
        if self.late_min_seconds > self.late_max_seconds:
            raise ValueError("invalid store-and-forward delay range")
        times = [step.after_seconds for step in self.drift]
        if times != sorted(set(times)):
            raise ValueError("drift steps must have strictly increasing offsets")
        return self


def load_live_config(path: Path) -> LiveConfig:
    """Parse YAML into the strict live-traffic configuration."""
    return LiveConfig.model_validate(yaml.safe_load(path.read_text()))
