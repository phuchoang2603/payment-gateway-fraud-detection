import hashlib
import heapq
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
        if self.decision.started_at < self.start.started_at:
            raise ValueError("decision precedes attempt start")
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
    pending: list = field(default_factory=list)
    expirations: dict[int, list] = field(default_factory=lambda: {window: [] for window in WINDOWS})
    counts: dict[int, int] = field(default_factory=lambda: {window: 0 for window in WINDOWS})
    amount_24h: int = 0
    devices: dict[str, int] = field(default_factory=dict)
    last_assessed: int | None = None
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
        identity = start.identity
        prior = self.entries.get(identity)
        if prior is not None and prior != start:
            raise ValueError("conflicting assessed contribution")
        if prior is None:
            if len(self.entries) >= self.max_entries:
                raise ValueError("entity state limit exceeded")
            self.entries[identity] = start
            heapq.heappush(self.pending, (start.started_at + 1, identity))
        self.progress = contribution.progress
        self.available_at = max(self.available_at, contribution.available_at)

    def advance(self, now):
        if now < self.anchor:
            raise ValueError("entity clock regressed")
        self.anchor = now
        windows = WINDOWS if self.view == "customer" else WINDOWS[:2]
        for window in windows:
            expirations = self.expirations[window]
            while expirations and expirations[0][0] <= now:
                _, identity = heapq.heappop(expirations)
                entry = self.entries[identity]
                self.counts[window] -= 1
                if window == DAY and self.view == "customer":
                    self.amount_24h -= entry.amount
                if window == 7 * DAY:
                    if entry.device is not None:
                        remaining = self.devices[entry.device] - 1
                        if remaining:
                            self.devices[entry.device] = remaining
                        else:
                            del self.devices[entry.device]
                    if not self.counts[window]:
                        self.last_assessed = None
                if window == windows[-1]:
                    del self.entries[identity]
        while self.pending and self.pending[0][0] <= now:
            _, identity = heapq.heappop(self.pending)
            entry = self.entries[identity]
            if entry.started_at < now - windows[-1]:
                del self.entries[identity]
                continue
            for window in windows:
                if entry.started_at < now - window:
                    continue
                self.counts[window] += 1
                # The inclusive lower boundary expires one microsecond after start + window.
                heapq.heappush(self.expirations[window], (entry.started_at + window + 1, identity))
                if window == DAY and self.view == "customer":
                    self.amount_24h += entry.amount
                if window == 7 * DAY:
                    if entry.device is not None:
                        self.devices[entry.device] = self.devices.get(entry.device, 0) + 1
                    if self.last_assessed is None or entry.started_at > self.last_assessed:
                        self.last_assessed = entry.started_at
        coverage = {window: now - window >= self.bootstrap for window in windows}
        values = {}
        for window, name in ((HOUR, "assessed_count_1h"), (DAY, "assessed_count_24h")):
            if coverage[window]:
                values[name] = self.counts[window]
        if self.view == "customer":
            if coverage[DAY]:
                values["assessed_amount_minor_24h"] = self.amount_24h
            if coverage[7 * DAY]:
                values["distinct_devices_7d"] = len(self.devices)
                values["last_assessed_start_at"] = self.last_assessed
        boundaries = [
            self.bootstrap + window for window in windows if self.bootstrap + window > now
        ]
        if self.pending:
            boundaries.append(self.pending[0][0])
        boundaries.extend(
            self.expirations[window][0][0] for window in windows if self.expirations[window]
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
