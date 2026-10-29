import hashlib
import heapq
import json
from dataclasses import dataclass, field

SECOND = 1_000_000
MINUTE = 60 * SECOND
HOUR = 60 * MINUTE
DAY = 24 * HOUR
WINDOWS = (5 * MINUTE, HOUR, DAY)
HORIZON = DAY
DEDUP_RETENTION = HORIZON + HOUR


@dataclass(frozen=True)
class Fact:
    event_id: str
    digest: str
    integration: str
    payment: str
    attempt: str
    started_at: int
    recorded_at: int
    customer: str
    method: str
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

    @property
    def too_late(self):
        return self.fact.started_at < self.now - HORIZON


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
    devices: dict[str, int] = field(default_factory=dict)
    progress: tuple[Progress, ...] = ()
    available_at: int = 0
    revision: int = 0
    write_at: int = 0
    anchor: int = 0
    previous: str = ""
    next_timer: int | None = None
    max_entries: int = 50_000

    def add(self, packet):
        start = packet.fact
        expected = start.customer if self.view == "customer" else start.method
        if start.integration != self.integration or expected != self.entity:
            raise ValueError("entity contribution binding mismatch")
        if packet.bootstrap != self.bootstrap:
            raise ValueError("bootstrap boundary changed within checkpoint state")
        prior = self.entries.get(start.identity)
        if prior is not None and prior != start:
            raise ValueError("conflicting attempt start")
        if prior is None:
            if len(self.entries) >= self.max_entries:
                raise ValueError("entity state limit exceeded")
            self.entries[start.identity] = start
            heapq.heappush(self.pending, (start.started_at + 1, start.identity))
        self.progress = packet.progress
        self.available_at = max(self.available_at, packet.now)

    def advance(self, now):
        if now < self.anchor:
            raise ValueError("entity clock regressed")
        self.anchor = now
        for window in WINDOWS:
            expirations = self.expirations[window]
            while expirations and expirations[0][0] <= now:
                _, identity = heapq.heappop(expirations)
                entry = self.entries[identity]
                self.counts[window] -= 1
                if window == DAY:
                    if self.view == "customer" and entry.device is not None:
                        remaining = self.devices[entry.device] - 1
                        if remaining:
                            self.devices[entry.device] = remaining
                        else:
                            del self.devices[entry.device]
                    del self.entries[identity]
        while self.pending and self.pending[0][0] <= now:
            _, identity = heapq.heappop(self.pending)
            entry = self.entries[identity]
            if entry.started_at < now - DAY:
                del self.entries[identity]
                continue
            for window in WINDOWS:
                if entry.started_at < now - window:
                    continue
                self.counts[window] += 1
                # The inclusive lower boundary expires one microsecond after start + window.
                heapq.heappush(self.expirations[window], (entry.started_at + window + 1, identity))
                if window == DAY and self.view == "customer" and entry.device is not None:
                    self.devices[entry.device] = self.devices.get(entry.device, 0) + 1
        coverage = {window: now - window >= self.bootstrap for window in WINDOWS}
        values = {
            f"attempt_count_{name}": self.counts[window]
            for window, name in zip(WINDOWS, ("5m", "1h", "24h"))
            if coverage[window]
        }
        if self.view == "customer" and coverage[DAY]:
            values["distinct_devices_24h"] = len(self.devices)
        boundaries = [
            self.bootstrap + window for window in WINDOWS if self.bootstrap + window > now
        ]
        if self.pending:
            boundaries.append(self.pending[0][0])
        boundaries.extend(
            self.expirations[window][0][0] for window in WINDOWS if self.expirations[window]
        )
        self.next_timer = min(boundaries, default=None)
        signature = json.dumps(
            [
                list(coverage.values()),
                values,
                [self.counts[window] for window in WINDOWS],
                [(progress.partition, progress.consumed) for progress in self.progress],
            ],
            sort_keys=True,
        )
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
