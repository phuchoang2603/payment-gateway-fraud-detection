from collections import Counter
from dataclasses import asdict, dataclass, field
from datetime import datetime

from simulator.generation import GeneratedHistory

IDENTITIES = ("customer", "device", "ip_network", "address", "payment_method")


@dataclass
class DatasetStatistics:
    payments: int = 0
    unique_events: int = 0
    duplicate_events: int = 0
    labels: int = 0
    chargebacks: int = 0
    history_end: datetime | None = None
    field_present_before_cutover: int = 0
    field_present_after_cutover: int = 0
    scenarios: Counter = field(default_factory=Counter)
    merchants: Counter = field(default_factory=Counter)
    categories: Counter = field(default_factory=Counter)
    attempts: Counter = field(default_factory=Counter)

    def observe(self, history: GeneratedHistory, duplicates: int) -> None:
        self.payments += 1
        self.duplicate_events += duplicates
        self.scenarios[history.truth] += 1
        self.labels += len(history.labels)
        self.chargebacks += sum(label["source"] == "LABEL_SOURCE_CHARGEBACK" for label in history.labels)
        for event in history.events:
            self.unique_events += 1
            occurred = event["occurred_at"]
            self.history_end = occurred if self.history_end is None else max(self.history_end, occurred)
            if (created := event.get("payment_created")) is not None:
                self.merchants[created["merchant"]["merchant_id"]] += 1
                self.categories[created["snapshot"]["items"][0]["category"]] += 1
            elif (started := event.get("attempt_started")) is not None:
                self.attempts[history.truth] += 1
                signals = started["signals"]
                present = signals.get("user_agent_family") is not None
                if event["schema_version"] == 1:
                    self.field_present_before_cutover += present
                else:
                    self.field_present_after_cutover += present

    def observed(self) -> dict:
        exported = self.unique_events + self.duplicate_events
        return {
            "payments": self.payments,
            "unique_events": self.unique_events,
            "duplicate_events": self.duplicate_events,
            "duplicate_rate": self.duplicate_events / exported if exported else 0,
            "labels": self.labels,
            "chargebacks": self.chargebacks,
            "scenario_payments": dict(sorted(self.scenarios.items())),
            "category_payments": dict(sorted(self.categories.items())),
            "merchant_count": len(self.merchants),
            "merchant_max_share": max(self.merchants.values()) / self.payments if self.payments else 0,
            "scenario_attempts": dict(sorted(self.attempts.items())),
            "scenario_attempt_shares": {
                name: count / sum(self.attempts.values()) for name, count in sorted(self.attempts.items())
            },
            "attempts": sum(self.attempts.values()),
            "merchant_top_10_share": sum(sorted(self.merchants.values(), reverse=True)[:10]) / self.payments
            if self.payments
            else 0,
            "user_agent_family_present_before_cutover": self.field_present_before_cutover,
            "user_agent_family_present_after_cutover": self.field_present_after_cutover,
        }

    def to_dict(self) -> dict:
        result = asdict(self)
        for name in ("scenarios", "merchants", "categories", "attempts"):
            result[name] = dict(getattr(self, name))
        result["history_end"] = self.history_end.isoformat() if self.history_end else None
        return result

    @classmethod
    def from_dict(cls, values: dict):
        values = values.copy()
        values["history_end"] = datetime.fromisoformat(values["history_end"]) if values["history_end"] else None
        for name in ("scenarios", "merchants", "categories", "attempts"):
            values[name] = Counter(values[name])
        return cls(**values)

    def combine(self, other):
        for name in (
            "payments",
            "unique_events",
            "duplicate_events",
            "labels",
            "chargebacks",
            "field_present_before_cutover",
            "field_present_after_cutover",
        ):
            setattr(self, name, getattr(self, name) + getattr(other, name))
        for name in ("scenarios", "merchants", "categories", "attempts"):
            getattr(self, name).update(getattr(other, name))
        if other.history_end:
            self.history_end = max(self.history_end, other.history_end) if self.history_end else other.history_end
