from pathlib import Path

import yaml
from pydantic import Field, model_validator

from simulator.config import StrictModel


class Faults(StrictModel):
    enabled: bool = False
    group: str = "simulator-delivery-faults-v1"
    source: str = "payments.events.v1"
    destination: str = "payments.events.v1.faulty"
    duplicate_rate: float = Field(default=0, ge=0, le=1)
    late_rate: float = Field(default=0, ge=0, le=1)
    late_min_seconds: float = Field(default=1, ge=0)
    late_max_seconds: float = Field(default=30, ge=0)
    duplicate_delay_seconds: float = Field(default=1, ge=0)
    hold_interval_seconds: float = Field(default=60, gt=0)
    hold_seconds: float = Field(default=0, ge=0)
    max_records: int = Field(default=10000, gt=0)
    max_bytes: int = Field(default=64 * 1024**2, gt=0)
    max_record_bytes: int = Field(default=1024**2, gt=0)

    @model_validator(mode="after")
    def limits(self):
        if self.late_min_seconds > self.late_max_seconds or self.hold_seconds >= self.hold_interval_seconds:
            raise ValueError("invalid fault delay/hold range")
        if self.max_record_bytes > self.max_bytes or self.source == self.destination:
            raise ValueError("invalid fault capacity/topic")
        return self


class LiveConfig(StrictModel):
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
    faults: Faults = Field(default_factory=Faults)


def load_live_config(path: Path) -> LiveConfig:
    return LiveConfig.model_validate(yaml.safe_load(path.read_text()))
