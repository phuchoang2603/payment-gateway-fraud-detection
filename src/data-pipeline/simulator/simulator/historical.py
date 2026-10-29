import hashlib
import json
import math
import tempfile
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from functools import cache
from pathlib import Path
from typing import Self
from urllib.parse import urlsplit

import pyarrow as pa
import pyarrow.parquet as pq
import yaml
from pydantic import Field, field_validator, model_validator

from simulator.arrow import LABEL_SCHEMA, payment_event_schema
from simulator.lifecycle import payment_history
from simulator.store import ObjectExists, open_store
from simulator.world import StrictModel, World, WorldConfig

EVENTS = "payment_events"
LABELS = "fraud_labels"
CONFIG_KEY = "config.json"
MANIFEST_KEY = "manifest.json"
PART_CUSTOMERS = 750_000
ROW_GROUP_ROWS = 131_072
GENERATOR_SOURCES = ("world.py", "lifecycle.py", "historical.py", "store.py", "us-calibration.json")
CONTRACT_FILES = {EVENTS: "payment-events.odcs.yaml", LABELS: "fraud-labels.odcs.yaml"}


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
    version: int = Field(ge=3)

    @field_validator("effective_at")
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError("schema change effective_at must include a timezone")
        return value


class GeneratorConfig(StrictModel):
    dataset_version: str = Field(pattern=r"^[a-zA-Z0-9][a-zA-Z0-9._-]*$")
    duration_days: int = Field(gt=0)
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
        versions = [2, *(change.version for change in changes)]
        if any(later < earlier for earlier, later in zip(versions, versions[1:])):
            raise ValueError("schema_changes must not decrease the schema version")
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
        return math.ceil(self.world.counts.customers / PART_CUSTOMERS)

    def day(self, offset: int) -> date:
        return self.world.start_date + timedelta(days=offset)

    def logical(self) -> dict:
        return self.model_dump(mode="json", exclude={"output"})

    def identity(self, generator_digest: str) -> str:
        document = {"configuration": self.logical(), "generator_digest": generator_digest}
        return hashlib.sha256(json.dumps(document, sort_keys=True).encode()).hexdigest()

    def schema_version_at(self, occurred_at: datetime) -> int:
        version = 2
        for change in self.schema_changes:
            if occurred_at >= change.effective_at:
                version = change.version
        return version


def load_generator_config(path: Path) -> GeneratorConfig:
    return GeneratorConfig.model_validate(yaml.safe_load(path.read_text(encoding="utf-8")))


@cache
def load_contract(directory: Path) -> tuple[tuple[tuple[tuple[str, ...], int], ...], dict[str, str]]:
    documents = {
        table: yaml.safe_load((directory / name).read_text(encoding="utf-8")) for table, name in CONTRACT_FILES.items()
    }
    declared: list[tuple[tuple[str, ...], int]] = []

    def walk(properties, prefix: tuple[str, ...]) -> None:
        for prop in properties or []:
            path = (*prefix, prop["name"])
            since = [
                item["value"]
                for item in prop.get("customProperties") or []
                if item["property"] == "sinceSchemaVersion"
            ]
            if since:
                declared.append((path, int(since[0])))
            walk(prop.get("properties"), path)

    walk(documents[EVENTS]["schema"][0]["properties"], ())
    identities = {table: f"{doc['id']}@{doc['version']}" for table, doc in documents.items()}
    return tuple(declared), identities


def generator_digest(contracts: Path) -> str:
    package = Path(__file__).parent
    digest = hashlib.sha256()
    for path in [package / name for name in GENERATOR_SOURCES] + [
        contracts / name for name in CONTRACT_FILES.values()
    ]:
        digest.update(path.name.encode())
        digest.update(path.read_bytes())
    return digest.hexdigest()


def _without(record: dict, path: tuple[str, ...]) -> dict:
    head, *rest = path
    if head not in record or record[head] is None:
        return record
    if not rest:
        return {key: value for key, value in record.items() if key != head}
    return {**record, head: _without(record[head], tuple(rest))}


def _project(record: dict, version: int, declared) -> dict:
    result = record
    for path, since in declared:
        if version < since:
            result = _without(result, path)
    return result


def _drop_field(struct: pa.StructType, path: tuple[str, ...]) -> pa.StructType:
    head, *rest = path
    fields = []
    for field in struct:
        if field.name != head:
            fields.append(field)
        elif rest:
            fields.append(pa.field(field.name, _drop_field(field.type, tuple(rest)), field.nullable))
    return pa.struct(fields)


@cache
def _schema_for(version: int, declared) -> pa.Schema:
    struct = pa.struct(list(payment_event_schema()))
    for path, since in declared:
        if version < since:
            struct = _drop_field(struct, path)
    return pa.schema(list(struct))


class _PartWriter:
    def __init__(self, path: Path, schema: pa.Schema):
        self.path = path
        self.schema = schema
        self.pending: list[dict] = []
        self.rows = 0
        self.writer = pq.ParquetWriter(path, schema, compression="zstd")

    def extend(self, records: list[dict]) -> None:
        self.pending.extend(records)
        self.rows += len(records)
        if len(self.pending) >= ROW_GROUP_ROWS:
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


@cache
def _world(config: WorldConfig) -> World:
    return World.build(config)


def _chunk_purchases(config: GeneratorConfig, world: World, offset: int, chunk: int):
    day = config.day(offset)
    first = chunk * PART_CUSTOMERS
    customers = range(first, min(first + PART_CUSTOMERS, config.world.counts.customers))
    for customer, number in world.day_purchases(day, customers):
        rng = world.purchase_rng(day, customer, number)
        yield world.purchase(rng, day, customer), f"{customer}:{number}", rng
    testing = config.world.scenarios.card_testing
    for burst in range(world.burst_count(day)):
        if burst % config.customer_chunks != chunk:
            continue
        for number in range(testing.burst_size):
            rng = world.burst_rng(day, burst, number)
            yield world.card_test(rng, day, burst), f"burst{burst}:{number}", rng


def write_part(config: GeneratorConfig, contracts: Path, offset: int, chunk: int) -> list[dict]:
    world = _world(config.world)
    day = config.day(offset)
    declared, _ = load_contract(contracts)
    store = open_store(config.output, config.dataset_version)
    with tempfile.TemporaryDirectory() as scratch:
        scratch_dir = Path(scratch)
        writers: dict[tuple[str, int | None], _PartWriter] = {}

        def writer(table: str, version: int | None) -> _PartWriter:
            if (table, version) not in writers:
                schema = _schema_for(version, declared) if table == EVENTS else LABEL_SCHEMA
                name = f"{table}-{version}.parquet" if version is not None else f"{table}.parquet"
                writers[(table, version)] = _PartWriter(scratch_dir / name, schema)
            return writers[(table, version)]

        for purchase, key, rng in _chunk_purchases(config, world, offset, chunk):
            events, labels = payment_history(
                world,
                purchase,
                rng,
                day=day,
                key=key,
                schema_version_at=config.schema_version_at,
                duplicate_rate=config.duplicate_rate,
            )
            by_version: dict[int, list[dict]] = defaultdict(list)
            for event in events:
                version = event["schema_version"]
                by_version[version].append(_project(event, version, declared))
            for version, rows in by_version.items():
                writer(EVENTS, version).extend(rows)
            if labels:
                writer(LABELS, None).extend(labels)
        parts = []
        for (table, version), part in sorted(writers.items(), key=lambda item: (item[0][0], item[0][1] or 0)):
            part.close()
            local = part.path
            suffix = f".v{version}" if version is not None else ""
            key = f"{table}/date={day.isoformat()}/part-{chunk:05d}{suffix}.parquet"
            store.publish_file(key, local)
            parts.append(
                {
                    "table": table,
                    "schema_version": version,
                    "date": day.isoformat(),
                    "path": key,
                    "rows": part.rows,
                    "bytes": local.stat().st_size,
                    "sha256": _sha256(local),
                }
            )
    return parts


def _marker_key(day: date) -> str:
    return f"date={day.isoformat()}/_day.json"


def _claim_configuration(config: GeneratorConfig, digest: str, store) -> None:
    document = json.dumps(
        {
            "configuration": config.logical(),
            "generator_digest": digest,
            "configuration_sha256": config.identity(digest),
        },
        sort_keys=True,
    ).encode()
    try:
        store.write(CONFIG_KEY, document, if_absent=True)
        return
    except ObjectExists:
        pass
    stored = store.read(CONFIG_KEY)
    if stored is None or json.loads(stored)["configuration_sha256"] != config.identity(digest):
        raise ConfigurationChanged(f"{CONFIG_KEY} differs from the requested configuration")


def _check_schema_versions(config: GeneratorConfig, contracts: Path) -> None:
    declared, _ = load_contract(contracts)
    allowed = {2, *(since for _, since in declared)}
    undeclared = {change.version for change in config.schema_changes} - allowed
    if undeclared:
        raise GenerationError(f"schema versions {sorted(undeclared)} are not declared by the payment-events contract")


def _refuse_published(store, config: GeneratorConfig) -> None:
    if store.read(MANIFEST_KEY) is not None:
        raise VersionPublished(f"{MANIFEST_KEY} already exists for {config.dataset_version}")


def _publish(config: GeneratorConfig, contracts: Path, digest: str, store) -> dict:
    _, identities = load_contract(contracts)
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
        "generator_digest": digest,
        "configuration_sha256": config.identity(digest),
        "configuration": config.logical(),
        "world_digest": _world(config.world).digest,
        "contracts": identities,
        "rows": rows,
        "physical_bytes": physical_bytes,
        "files": files,
    }
    try:
        store.write(MANIFEST_KEY, json.dumps(manifest, sort_keys=True).encode(), if_absent=True)
    except ObjectExists as error:
        raise VersionPublished(f"{MANIFEST_KEY} already exists for {config.dataset_version}") from error
    return manifest


def _results(config: GeneratorConfig, contracts: Path, pairs, workers: int):
    if workers > 1:
        with ProcessPoolExecutor(max_workers=workers) as pool:
            futures = {
                pool.submit(write_part, config, contracts, offset, chunk): (offset, chunk) for offset, chunk in pairs
            }
            try:
                for future in as_completed(futures):
                    offset, chunk = futures[future]
                    yield offset, chunk, future.result()
            except BaseException:
                pool.shutdown(wait=False, cancel_futures=True)
                raise
    else:
        for offset, chunk in pairs:
            yield offset, chunk, write_part(config, contracts, offset, chunk)


def produce(config: GeneratorConfig, contracts: Path, shard: tuple[int, int] = (0, 1), workers: int = 1) -> None:
    digest = generator_digest(contracts)
    _check_schema_versions(config, contracts)
    index, count = shard
    if not 0 <= index < count:
        raise GenerationError("shard index must be within the shard count")
    store = open_store(config.output, config.dataset_version)
    _refuse_published(store, config)
    _claim_configuration(config, digest, store)
    chunks = config.customer_chunks
    offsets = [
        offset
        for offset in range(config.duration_days)
        if offset % count == index and store.read(_marker_key(config.day(offset))) is None
    ]
    pairs = [(offset, chunk) for offset in offsets for chunk in range(chunks)]
    collected: dict[int, dict[int, list[dict]]] = defaultdict(dict)
    for offset, chunk, parts in _results(config, contracts, pairs, workers):
        collected[offset][chunk] = parts
        if len(collected[offset]) == chunks:
            day = config.day(offset)
            ordered = [part for index in sorted(collected[offset]) for part in collected[offset][index]]
            marker = {"date": day.isoformat(), "parts": ordered}
            store.write(_marker_key(day), json.dumps(marker, sort_keys=True).encode())
            del collected[offset]


def publish(config: GeneratorConfig, contracts: Path) -> dict:
    digest = generator_digest(contracts)
    _check_schema_versions(config, contracts)
    store = open_store(config.output, config.dataset_version)
    _refuse_published(store, config)
    _claim_configuration(config, digest, store)
    return _publish(config, contracts, digest, store)


def generate(config: GeneratorConfig, contracts: Path, workers: int = 1) -> dict:
    produce(config, contracts, workers=workers)
    return publish(config, contracts)
