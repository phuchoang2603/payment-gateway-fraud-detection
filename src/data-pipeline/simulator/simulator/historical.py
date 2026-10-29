import hashlib
import json
import math
import re
import tempfile
from concurrent.futures import ProcessPoolExecutor
from contextlib import nullcontext
from datetime import date, datetime, timedelta
from functools import cache
from itertools import repeat
from pathlib import Path
from typing import Literal, Self
from urllib.parse import urlsplit

import pyarrow as pa
import pyarrow.parquet as pq
import yaml
from pydantic import Field, field_validator, model_validator

from simulator.arrow import LABEL_SCHEMA, event_schema
from simulator.calibration import provenance
from simulator.config import StrictModel
from simulator.lifecycle import payment_history
from simulator.store import ObjectExists, open_store
from simulator.world import World, WorldConfig

REVISION_PATTERN = re.compile(r"[a-f0-9]{40}")
EVENTS = "payment_events"
LABELS = "fraud_labels"
CONTRACTS = {
    EVENTS: "historical-payments/payment-events@1.0.0",
    LABELS: "historical-payments/fraud-labels@1.0.0",
}
EVENT_SCHEMA = event_schema(4).append(pa.field("physical_schema_version", pa.uint32(), nullable=False))
CONFIG_KEY = "config.json"
MANIFEST_KEY = "manifest.json"
ORDINAL_CHUNK_SHIFT = 48
ORDINAL_BURST_OFFSET = 1 << 47


class GenerationError(Exception):
    pass


class ConfigurationChanged(GenerationError):
    pass


class VersionPublished(GenerationError):
    pass


class SourceTooSmall(GenerationError):
    pass


class SchemaChange(StrictModel):
    effective_at: datetime
    version: Literal[3, 4]

    @field_validator("effective_at")
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError("schema change effective_at must include a timezone")
        return value


class GeneratorConfig(StrictModel):
    dataset_version: str = Field(pattern=r"^[a-zA-Z0-9][a-zA-Z0-9._-]*$")
    duration_days: int = Field(gt=0)
    chunk_customers: int = Field(gt=0)
    workers: int = Field(default=1, gt=0)
    row_group_rows: int = Field(default=131072, gt=0)
    duplicate_rate: float = Field(default=0, ge=0, lt=1)
    minimum_source_bytes: int = Field(default=0, ge=0)
    schema_changes: tuple[SchemaChange, ...] = ()
    output: str = Field(min_length=1)
    world: WorldConfig

    @model_validator(mode="after")
    def validate_schedule(self) -> Self:
        changes = self.schema_changes
        if any(later.effective_at <= earlier.effective_at for earlier, later in zip(changes, changes[1:])):
            raise ValueError("schema_changes must be in ascending effective_at order")
        versions = [3, *(change.version for change in changes)]
        if any(later < earlier for earlier, later in zip(versions, versions[1:])):
            raise ValueError("schema_changes must not decrease the physical schema version")
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

    @property
    def customer_chunks(self) -> int:
        return math.ceil(self.world.counts.customers / self.chunk_customers)

    def day(self, offset: int) -> date:
        return self.world.start_date + timedelta(days=offset)

    def logical(self) -> dict:
        return self.model_dump(mode="json", exclude={"output", "workers", "row_group_rows"})

    def identity(self, revision: str) -> str:
        document = {
            "configuration": self.logical(),
            "generator_revision": revision,
            "calibration_sha256": provenance()["sha256"],
        }
        return hashlib.sha256(json.dumps(document, sort_keys=True).encode()).hexdigest()

    def schema_version_at(self, occurred_at: datetime) -> int:
        version = 3
        for change in self.schema_changes:
            if occurred_at >= change.effective_at:
                version = change.version
        return version


def load_generator_config(path: Path) -> GeneratorConfig:
    return GeneratorConfig.model_validate(yaml.safe_load(path.read_text(encoding="utf-8")))


class _PartWriter:
    def __init__(self, path: Path, schema: pa.Schema, row_group_rows: int):
        self.schema = schema
        self.row_group_rows = row_group_rows
        self.pending: list[dict] = []
        self.rows = 0
        self.writer = pq.ParquetWriter(path, schema, compression="zstd")

    def extend(self, records: list[dict]) -> None:
        self.pending.extend(records)
        self.rows += len(records)
        if len(self.pending) >= self.row_group_rows:
            self.flush()

    def flush(self) -> None:
        if self.pending:
            self.writer.write_table(pa.Table.from_pylist(self.pending, schema=self.schema))
            self.pending = []

    def close(self) -> None:
        self.flush()
        self.writer.close()


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def _chunk_purchases(config: GeneratorConfig, world: World, offset: int, chunk: int):
    day = config.day(offset)
    first = chunk * config.chunk_customers
    customers = range(first, min(first + config.chunk_customers, config.world.counts.customers))
    for index, customer in enumerate(world.day_customers(day, customers)):
        ordinal = (chunk << ORDINAL_CHUNK_SHIFT) | index
        rng = world.purchase_rng(day, ordinal)
        yield world.purchase(rng, day, ordinal, int(customer)), ordinal, rng
    testing = config.world.scenarios.card_testing
    for burst in range(world.burst_count(day)):
        if burst % config.customer_chunks != chunk:
            continue
        for index in range(testing.burst_size):
            ordinal = ORDINAL_BURST_OFFSET | (burst * testing.burst_size + index)
            rng = world.purchase_rng(day, ordinal)
            yield world.card_test(rng, day, burst), ordinal, rng


@cache
def _world(config: WorldConfig) -> World:
    return World.build(config)


def write_part(config: GeneratorConfig, offset: int, chunk: int) -> list[dict]:
    world = _world(config.world)
    day = config.day(offset)
    store = open_store(config.output, config.dataset_version)
    with tempfile.TemporaryDirectory() as scratch:
        scratch_dir = Path(scratch)
        writers = {
            EVENTS: _PartWriter(scratch_dir / f"{EVENTS}.parquet", EVENT_SCHEMA, config.row_group_rows),
            LABELS: _PartWriter(scratch_dir / f"{LABELS}.parquet", LABEL_SCHEMA, config.row_group_rows),
        }
        for purchase, ordinal, rng in _chunk_purchases(config, world, offset, chunk):
            events, labels = payment_history(
                world,
                purchase,
                rng,
                day=day,
                ordinal=ordinal,
                schema_version_at=config.schema_version_at,
                duplicate_rate=config.duplicate_rate,
            )
            writers[EVENTS].extend(events)
            writers[LABELS].extend(labels)
        parts = []
        for table, writer in writers.items():
            writer.close()
            path = scratch_dir / f"{table}.parquet"
            key = f"{table}/date={day.isoformat()}/part-{chunk:05d}.parquet"
            store.publish_file(key, path)
            parts.append(
                {
                    "table": table,
                    "date": day.isoformat(),
                    "path": key,
                    "rows": writer.rows,
                    "bytes": path.stat().st_size,
                    "sha256": _sha256(path),
                }
            )
    return parts


def _marker_key(day: date) -> str:
    return f"date={day.isoformat()}/_day.json"


def _claim_configuration(config: GeneratorConfig, revision: str, store) -> None:
    document = json.dumps(
        {
            "configuration": config.logical(),
            "generator_revision": revision,
            "configuration_sha256": config.identity(revision),
        },
        sort_keys=True,
    ).encode()
    try:
        store.write(CONFIG_KEY, document, if_absent=True)
        return
    except ObjectExists:
        pass
    stored = store.read(CONFIG_KEY)
    if stored is None or json.loads(stored)["configuration_sha256"] != config.identity(revision):
        raise ConfigurationChanged(f"{CONFIG_KEY} differs from the requested configuration")


def _publish(config: GeneratorConfig, revision: str, store) -> dict:
    files: list[dict] = []
    rows = {EVENTS: 0, LABELS: 0}
    for offset in range(config.duration_days):
        day = config.day(offset)
        marker = store.read(_marker_key(day))
        if marker is None:
            raise GenerationError(f"day {day.isoformat()} is not complete")
        for part in json.loads(marker)["parts"]:
            files.append(part)
            rows[part["table"]] += part["rows"]
    physical_bytes = sum(part["bytes"] for part in files)
    if physical_bytes < config.minimum_source_bytes:
        raise SourceTooSmall(
            f"physical bytes {physical_bytes} are below minimum_source_bytes {config.minimum_source_bytes}"
        )
    manifest = {
        "dataset_version": config.dataset_version,
        "generator_revision": revision,
        "configuration_sha256": config.identity(revision),
        "configuration": config.logical(),
        "seed": config.world.seed,
        "start_date": config.world.start_date.isoformat(),
        "days": config.duration_days,
        "physical_schema_versions": sorted({3, *(change.version for change in config.schema_changes)}),
        "contracts": CONTRACTS,
        "rows": rows,
        "physical_bytes": physical_bytes,
        "files": files,
    }
    try:
        store.write(MANIFEST_KEY, json.dumps(manifest, sort_keys=True).encode(), if_absent=True)
    except ObjectExists as error:
        raise VersionPublished(f"{MANIFEST_KEY} already exists for {config.dataset_version}") from error
    return manifest


def generate(config: GeneratorConfig, revision: str) -> dict:
    if not REVISION_PATTERN.fullmatch(revision):
        raise GenerationError("generator revision must be a 40-character lowercase git commit")
    store = open_store(config.output, config.dataset_version)
    if store.read(MANIFEST_KEY) is not None:
        raise VersionPublished(f"{MANIFEST_KEY} already exists for {config.dataset_version}")
    _claim_configuration(config, revision, store)
    pending = [offset for offset in range(config.duration_days) if store.read(_marker_key(config.day(offset))) is None]
    pool = ProcessPoolExecutor(max_workers=config.workers) if config.workers > 1 else nullcontext()
    with pool as executor:
        mapper = executor.map if executor else map
        for offset in pending:
            chunks = mapper(write_part, repeat(config), repeat(offset), range(config.customer_chunks))
            parts = [part for chunk_parts in chunks for part in chunk_parts]
            marker = {"date": config.day(offset).isoformat(), "parts": parts}
            store.write(_marker_key(config.day(offset)), json.dumps(marker, sort_keys=True).encode())
    return _publish(config, revision, store)
