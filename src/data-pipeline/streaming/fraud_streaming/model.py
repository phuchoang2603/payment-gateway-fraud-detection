import hashlib
import json
from dataclasses import dataclass, field

SECOND = 1_000_000
HOUR = 3600 * SECOND
DAY = 24 * HOUR
WINDOWS = (HOUR, DAY, 7 * DAY)
RETENTION = 8 * DAY


@dataclass(frozen=True)
class Fact:
    event_id: str
    digest: str
    integration: str
    payment: str
    attempt: str
    kind: str
    started_at: int
    recorded_at: int
    customer: str = ""
    method: str = ""
    amount: int = 0
    device: str | None = None

    @property
    def identity(self):
        return self.integration, self.payment, self.attempt


@dataclass(frozen=True)
class Progress:
    topic: str
    partition: int
    consumed: int
    high_watermark: int
    observed_at: int


@dataclass(frozen=True)
class Packet:
    fact: Fact | None
    now: int
    bootstrap: int
    progress: tuple[Progress, ...]


@dataclass(frozen=True)
class Contribution:
    start: Fact
    available_at: int
    bootstrap: int
    progress: tuple[Progress, ...]


@dataclass
class Attempt:
    start: Fact | None = None
    decision: Fact | None = None
    emitted: bool = False
    expires_at: int = 0

    def accept(self, packet):
        fact = packet.fact
        if packet.now - fact.started_at > RETENTION:
            raise ValueError("input exceeds retained correction range; use a new replay epoch")
        prior = getattr(self, fact.kind)
        if prior is not None:
            if prior != fact:
                raise ValueError("conflicting lifecycle fact for attempt")
            return None
        setattr(self, fact.kind, fact)
        self.expires_at = max(self.expires_at, packet.now + RETENTION)
        if self.start is None or self.decision is None or self.emitted:
            return None
        self.emitted = True
        return Contribution(self.start, packet.now, packet.bootstrap, packet.progress)

    def expire(self):
        if not self.emitted:
            raise ValueError("unmatched lifecycle facts expired; replay required")


@dataclass
class Entity:
    view: str
    integration: str
    entity: str
    bootstrap: int
    entries: dict[tuple[str, str, str], Fact] = field(default_factory=dict)
    progress: tuple[Progress, ...] = ()
    available_at: int = 0
    revision: int = 0
    write_at: int = 0
    anchor: int = 0
    previous: str = ""
    next_timer: int | None = None
    max_entries: int = 50_000

    def add(self, contribution):
        start = contribution.start
        expected = start.customer if self.view == "customer" else start.method
        if start.integration != self.integration or expected != self.entity:
            raise ValueError("entity contribution binding mismatch")
        if contribution.bootstrap != self.bootstrap:
            raise ValueError("bootstrap boundary changed within checkpoint state")
        prior = self.entries.get(start.identity)
        if prior is not None and prior != start:
            raise ValueError("conflicting assessed contribution")
        self.entries[start.identity] = start
        if len(self.entries) > self.max_entries:
            raise ValueError("entity state limit exceeded")
        self.progress = contribution.progress
        self.available_at = max(self.available_at, contribution.available_at)

    def advance(self, now):
        if now < self.anchor:
            raise ValueError("entity clock regressed")
        self.anchor = now
        horizon = 7 * DAY if self.view == "customer" else DAY
        self.entries = {
            key: entry for key, entry in self.entries.items() if entry.started_at >= now - horizon
        }
        windows = WINDOWS if self.view == "customer" else WINDOWS[:2]
        coverage = {window: now - window >= self.bootstrap for window in windows}
        values = {}
        for window, name in ((HOUR, "assessed_count_1h"), (DAY, "assessed_count_24h")):
            if coverage[window]:
                values[name] = sum(
                    now - window <= entry.started_at < now for entry in self.entries.values()
                )
        if self.view == "customer":
            if coverage[DAY]:
                values["assessed_amount_minor_24h"] = sum(
                    entry.amount
                    for entry in self.entries.values()
                    if now - DAY <= entry.started_at < now
                )
            if coverage[7 * DAY]:
                history = [entry for entry in self.entries.values() if entry.started_at < now]
                values["distinct_devices_7d"] = len(
                    {entry.device for entry in history if entry.device is not None}
                )
                values["last_assessed_start_at"] = max(
                    (entry.started_at for entry in history), default=None
                )
        boundaries = {
            self.bootstrap + window for window in windows if self.bootstrap + window > now
        }
        for entry in self.entries.values():
            boundaries.update(
                boundary
                for boundary in (
                    entry.started_at + 1,
                    *(entry.started_at + window + 1 for window in windows),
                )
                if boundary > now
            )
        self.next_timer = min(boundaries, default=None)
        signature = json.dumps([list(coverage.values()), values], sort_keys=True)
        if signature == self.previous:
            return None
        self.previous = signature
        self.revision += 1
        self.write_at = max(now, self.write_at + 1)
        return coverage, values


def entity_key(epoch, view, integration, entity):
    digest = hashlib.sha256()
    for component in (epoch, view, integration, entity):
        value = component.encode()
        digest.update(str(len(value)).encode() + b":" + value)
    return digest.hexdigest()
