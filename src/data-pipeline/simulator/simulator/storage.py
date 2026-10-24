import importlib.resources
import json
import multiprocessing
import os
import shutil
import time
from collections import Counter, defaultdict
from concurrent.futures import FIRST_COMPLETED, ProcessPoolExecutor, ThreadPoolExecutor, wait
from dataclasses import dataclass
from datetime import date, timedelta
from hashlib import sha256
from pathlib import Path
from tempfile import TemporaryDirectory
from urllib.parse import urlsplit

import duckdb
import numpy as np
import pyarrow as pa
import pyarrow.fs as fs
import pyarrow.parquet as pq
import yaml

from simulator.arrow import LABEL_SCHEMA, event_schema
from simulator.config import ALGORITHM_VERSION, RUN_STATE_VERSION, HistoricalConfig
from simulator.fragment_index import FragmentIndex
from simulator.generation import Record, build_history
from simulator.object_store import ObjectStore, digest_file
from simulator.profiles import (
    DUPLICATE_STAGE,
    HISTORY_STAGE,
    build_profiles,
    schedule_for_task,
    stage_rng,
    tasks_for_day,
)
from simulator.publication import PUBLICATION_KEY, complete_publication, finish_publication, publication_receipt
from simulator.resources import CapacityExceeded, LimitedWriter, ResourceMonitor, directory_bytes
from simulator.run_state import RunState
from simulator.statistics import DatasetStatistics

PRODUCT_DIR = Path(__file__).resolve().parents[4] / "contracts" / "data-products" / "historical-payments"
COMPRESSION = "zstd"
COMPRESSION_LEVEL = 3


@dataclass(frozen=True)
class Table:
    name: str
    partition: str
    time_column: str
    sort_columns: tuple[str, ...]
    contract: str


EVENTS = Table(
    "payment_events",
    "occurred_date",
    "occurred_at",
    ("occurred_at", "payment_id", "aggregate_version", "event_id"),
    "payment-events",
)
LABELS = Table(
    "fraud_labels",
    "label_available_date",
    "label_available_at",
    ("label_available_at", "payment_id", "attempt_id", "source"),
    "fraud-labels",
)


def _duplicates(events: tuple[Record, ...], rate: float, random: np.random.Generator) -> list[Record]:
    if not rate:
        return []
    copies = random.poisson(rate / (1 - rate), len(events))
    return [event for event, count in zip(events, copies) for _ in range(count)]


def _descriptor() -> dict:
    product = yaml.safe_load((PRODUCT_DIR / "product.odps.yaml").read_text())
    contracts = {}
    for path in sorted(PRODUCT_DIR.glob("*.odcs.yaml")):
        content = path.read_bytes()
        contract = yaml.safe_load(content)
        contracts[path.name.removesuffix(".odcs.yaml")] = {
            "id": contract["id"],
            "version": contract["version"],
            "sha256": sha256(content).hexdigest(),
        }
    return {"id": product["id"], "version": product["version"], "contracts": contracts}


def _generator_revision() -> str:
    digest = sha256()
    for path in sorted(Path(__file__).parent.glob("*.py")):
        digest.update(path.name.encode())
        digest.update(path.read_bytes())
    return digest.hexdigest()


def _file_item(path, key, table, **extra):
    metadata = pq.ParquetFile(path).metadata
    return {
        "key": key,
        "table": table.name,
        "bytes": path.stat().st_size,
        "sha256": digest_file(path),
        "rows": metadata.num_rows,
        "row_groups": metadata.num_row_groups,
        "uncompressed_parquet_bytes": sum(
            metadata.row_group(i).column(j).total_uncompressed_size
            for i in range(metadata.num_row_groups)
            for j in range(metadata.num_columns)
        ),
        **extra,
    }


_WORKER = None


def _initialize_worker(config, profiles_directory):
    global _WORKER
    _WORKER = (config, build_profiles(config, Path(profiles_directory), reopen=True))


def _generate_task(config, profiles, task, directory):
    directory.mkdir(parents=True, exist_ok=True)
    schedule = schedule_for_task(config, task, profiles)
    day = config.start_date + timedelta(days=task.offset)
    stats = DatasetStatistics()
    writers = {}
    partitions = defaultdict(Counter)
    bounds = {}
    accounting = [0]
    try:
        for start in range(0, len(schedule), config.chunk_purchases):
            events, labels = [], []
            for index in range(start, min(start + config.chunk_purchases, len(schedule))):
                random = stage_rng(config.seed, HISTORY_STAGE, task.offset, task.kind, task.block, index)
                duplicate_random = stage_rng(config.seed, DUPLICATE_STAGE, task.offset, task.kind, task.block, index)
                history = build_history(config, profiles, schedule, index, day, task.ordinal(index), random)
                copies = _duplicates(history.events, config.data_problems.duplicate_rate, duplicate_random)
                stats.observe(history, len(copies))
                events.extend((*history.events, *copies))
                labels.extend(history.labels)
            for table, records in ((EVENTS, events), (LABELS, labels)):
                groups = defaultdict(list)
                for record in records:
                    groups[record["schema_version"] if table is EVENTS else 1].append(record)
                for version, rows in groups.items():
                    name = (table.name, version)
                    if name not in writers:
                        path = directory / f"{table.name}-v{version}.parquet"
                        schema = event_schema(version) if table is EVENTS else LABEL_SCHEMA
                        output = LimitedWriter(path, accounting, config.execution.task_scratch_bytes)
                        writers[name] = (
                            pq.ParquetWriter(
                                output,
                                schema,
                                compression=COMPRESSION,
                                compression_level=COMPRESSION_LEVEL,
                            ),
                            output,
                            path,
                            table,
                            schema,
                        )
                    writer, _, _, _, schema = writers[name]
                    writer.write_table(
                        pa.Table.from_pylist(rows, schema=schema), row_group_size=config.storage.row_group_rows
                    )
                    partitions[name].update(row[table.time_column].date().isoformat() for row in rows)
                    low = min(row[table.time_column] for row in rows)
                    high = max(row[table.time_column] for row in rows)
                    previous = bounds.get(name)
                    bounds[name] = (
                        low if previous is None else min(previous[0], low),
                        high if previous is None else max(previous[1], high),
                    )
    finally:
        for writer, output, _, _, _ in writers.values():
            writer.close()
            output.close()
    fragments = []
    for name in sorted(writers):
        _, _, path, table, _ = writers[name]
        fragments.append(
            _file_item(
                path,
                f"_run/fragments/{task.key}/{path.name}",
                table,
                schema_version=name[1],
                partitions=dict(sorted(partitions[name].items())),
                min_time=bounds[name][0].isoformat(),
                max_time=bounds[name][1].isoformat(),
                local_path=str(path),
            )
        )
    return {"task": task.key, "files": fragments, "statistics": stats.to_dict()}


def _worker_task(task, directory):
    return _generate_task(*_WORKER, task, Path(directory))


def _receipt(store, key):
    try:
        receipt = store.json(key)
    except FileNotFoundError:
        return None
    return receipt if all(store.verify(item) for item in receipt["files"]) else None


def _upload_receipt(store, run, receipt_key, receipt, transfers):
    def upload(item):
        path = Path(item["local_path"])
        run.check_owner()
        store.put_file(item["key"], path)
        if not store.verify(item):
            raise OSError(f"uploaded object integrity mismatch: {item['key']}")

    list(transfers.map(upload, receipt["files"]))
    value = {**receipt, "files": [{k: v for k, v in item.items() if k != "local_path"} for item in receipt["files"]]}
    run.receipt(receipt_key, value)
    for item in receipt["files"]:
        Path(item["local_path"]).unlink()
    return value


def _tasks(config):
    for offset in range(config.duration_days):
        yield from tasks_for_day(config, offset)


def _generate(config, profiles, scratch, store, run, monitor, transfers, index):
    stats = DatasetStatistics()
    pending = {}
    queue = iter(_tasks(config))
    pool = None
    if config.execution.workers > 1:
        for name in ("OPENBLAS_NUM_THREADS", "OMP_NUM_THREADS"):
            os.environ[name] = "1"
        pool = ProcessPoolExecutor(
            config.execution.workers,
            mp_context=multiprocessing.get_context("spawn"),
            initializer=_initialize_worker,
            initargs=(config, str(scratch / "profiles")),
        )

    def accept(receipt):
        stats.combine(DatasetStatistics.from_dict(receipt["statistics"]))
        index.add(receipt["files"])

    try:
        exhausted = False
        while not exhausted or pending:
            while not exhausted and len(pending) < config.execution.max_in_flight:
                task = next(queue, None)
                if task is None:
                    exhausted = True
                    break
                monitor.check()
                key = f"_run/tasks/{task.key}.json"
                receipt = _receipt(store, key)
                if receipt:
                    accept(receipt)
                    continue
                directory = scratch / "tasks" / task.key
                if pool:
                    pending[pool.submit(_worker_task, task, str(directory))] = (key, directory)
                else:
                    receipt = _generate_task(config, profiles, task, directory)
                    monitor.check()
                    accept(_upload_receipt(store, run, key, receipt, transfers))
                    directory.rmdir()
            if pending:
                done, _ = wait(pending, timeout=0.2, return_when=FIRST_COMPLETED)
                monitor.check()
                for future in done:
                    key, directory = pending.pop(future)
                    accept(_upload_receipt(store, run, key, future.result(), transfers))
                    directory.rmdir()
    finally:
        if pool:
            for future in pending:
                future.cancel()
            pool.shutdown(wait=True, cancel_futures=True)
    return stats


def _connection(config, scratch, store):
    execution = config.execution
    connection = duckdb.connect(
        config={
            "memory_limit": f"{execution.duckdb_memory_limit_mb}MiB",
            "threads": execution.duckdb_threads,
            "temp_directory": str(scratch / "spill"),
            "max_temp_directory_size": f"{execution.duckdb_spill_bytes}B",
            "preserve_insertion_order": True,
        }
    )
    if store.remote:
        extension = (
            importlib.resources.files("duckdb_extension_httpfs")
            / "extensions"
            / f"v{duckdb.__version__}"
            / "httpfs.duckdb_extension"
        )
        connection.execute(f"LOAD '{str(extension).replace(chr(39), chr(39) * 2)}'")
        endpoint = urlsplit(os.environ.get("AWS_ENDPOINT_URL_S3", "https://minio.home.phuchoang.sbs"))
        connection.execute(
            "CREATE SECRET historical_source (TYPE s3, KEY_ID $key, SECRET $secret, SESSION_TOKEN $token, "
            "REGION $region, ENDPOINT $endpoint, URL_STYLE 'path', USE_SSL $ssl)",
            {
                "key": os.environ["AWS_ACCESS_KEY_ID"],
                "secret": os.environ["AWS_SECRET_ACCESS_KEY"],
                "token": os.environ.get("AWS_SESSION_TOKEN", ""),
                "region": os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
                "endpoint": endpoint.netloc,
                "ssl": endpoint.scheme == "https",
            },
        )
    connection.execute("SET TimeZone = 'UTC'")
    return connection


def _finalize(config, index, scratch, store, run, connection, monitor, transfers):
    inventory = []
    partitions = {}
    for table in (EVENTS, LABELS):
        partitions[table.name] = {}
        for day in index.days(table.name):
            count = 0
            paths = []
            source_digest = sha256()
            for item, rows in index.sources(table.name, day):
                paths.append(store.uri(item["key"]))
                source_digest.update(json.dumps(item, sort_keys=True).encode())
                count += rows
            partitions[table.name][day] = count
            key = f"_run/partitions/{table.name}/{day}.json"
            receipt = _receipt(store, key)
            source_identity = source_digest.hexdigest()
            if receipt and receipt.get("source_identity") == source_identity:
                inventory.extend(receipt["files"])
                continue
            run.check_owner()
            output = scratch / "partition"
            shutil.rmtree(output, ignore_errors=True)
            sql_path = str(output).replace("'", "''")
            connection.execute(
                f"COPY (SELECT * FROM read_parquet($sources, hive_partitioning=false) "
                f"WHERE CAST({table.time_column} AS DATE) = $day ORDER BY {', '.join(table.sort_columns)}) "
                f"TO '{sql_path}' (FORMAT parquet, COMPRESSION {COMPRESSION}, COMPRESSION_LEVEL {COMPRESSION_LEVEL}, "
                f"ROW_GROUP_SIZE {config.storage.row_group_rows}, FILE_SIZE_BYTES {config.storage.target_file_mb * 1024**2}, "
                "FILENAME_PATTERN 'part-{i}', PER_THREAD_OUTPUT false)",
                {"sources": paths, "day": date.fromisoformat(day)},
            )
            monitor.check()
            items = [
                _file_item(
                    path,
                    f"{table.name}/{table.partition}={day}/{path.name}",
                    table,
                    partition=day,
                    local_path=str(path),
                )
                for path in sorted(output.glob("*.parquet"))
            ]
            if sum(item["rows"] for item in items) != count:
                raise ValueError(f"final partition row count mismatch: {table.name}/{day}")
            receipt = _upload_receipt(store, run, key, {"source_identity": source_identity, "files": items}, transfers)
            inventory.extend(receipt["files"])
            output.rmdir()
    return inventory, partitions


def _views(connection, inventory, store, scratch):
    for table, schema in ((EVENTS, event_schema(2)), (LABELS, LABEL_SCHEMA)):
        empty = scratch / f"{table.name}-empty.parquet"
        pq.write_table(schema.empty_table(), empty)
        paths = [str(empty), *[store.uri(item["key"]) for item in inventory if item["table"] == table.name]]
        connection.read_parquet(paths, union_by_name=True, hive_partitioning=False).create_view(
            table.name, replace=True
        )


def _cardinality(connection):
    columns = {
        "customer": "payment_created.customer.customer_id",
        "address": "payment_created.snapshot.shipping.address_fingerprint",
        "device": "attempt_started.signals.device_fingerprint",
        "ip_network": "attempt_started.signals.ip_network_fingerprint",
        "payment_method": "attempt_started.signals.payment_method.fingerprint",
    }
    result = {}
    for name, column in columns.items():
        estimated, rows = connection.execute(
            f"SELECT approx_count_distinct({column}), count({column}) FROM payment_events"
        ).fetchone()
        result[name] = {
            "estimate": estimated,
            "method": "duckdb.approx_count_distinct",
            "denominator": "exported non-null field rows (includes identical duplicate exports)",
            "rows": rows,
            "distinct_to_row_ratio": estimated / rows if rows else 0,
        }
    return result


def _observations(connection, stats):
    observed = stats.observed()
    observed["cardinality"] = _cardinality(connection)
    counts = dict(
        connection.execute(
            "SELECT CAST(schema_version AS VARCHAR), count(*) FROM payment_events GROUP BY 1"
        ).fetchall()
    )
    observed["schema_version_rows"] = {version: counts.get(version, 0) for version in ("1", "2")}
    rows = sum(counts.values())
    observed["schema_version_shares"] = {
        version: count / rows if rows else 0 for version, count in observed["schema_version_rows"].items()
    }
    return observed


def _gate_observed(config, observed):
    for metric, band in config.acceptance_bands.items():
        value = observed
        for part in metric.split("."):
            value = value.get(part, 0) if isinstance(value, dict) else None
        if value is None or not band.minimum <= value <= band.maximum:
            raise ValueError(
                f"observed acceptance gate failed: {metric}={value}, required [{band.minimum}, {band.maximum}]"
            )


def _manifest(config, run, profiles, stats, inventory, partitions, validation, observed, revision, descriptor):
    configuration = config.model_dump(mode="json")
    storage = {}
    for table in (EVENTS, LABELS):
        files = [item for item in inventory if item["table"] == table.name]
        storage[table.name] = {
            name: sum(item[name] for item in files)
            for name in ("bytes", "rows", "row_groups", "uncompressed_parquet_bytes")
        }
        storage[table.name].update(files=len(files), sort_columns=list(table.sort_columns))
    physical = sum(item["bytes"] for item in inventory)
    return {
        "manifest_version": RUN_STATE_VERSION,
        "algorithm_version": ALGORITHM_VERSION,
        "generation_identity": run.state["generation_identity"],
        "dataset_version": config.dataset_version,
        "seed": config.seed,
        "configuration": configuration,
        "config_sha256": sha256(json.dumps(configuration, sort_keys=True).encode()).hexdigest(),
        "generator_revision": revision,
        "data_product": descriptor,
        "profile_sha256": profiles.digest(),
        "synthetic_key_version": config.synthetic_key_version,
        "history_end": stats.history_end.isoformat() if stats.history_end else None,
        "schema_versions": {EVENTS.name: [1, 2], LABELS.name: [1]},
        "partitions": partitions,
        "storage": {
            "format": "parquet",
            "compression": COMPRESSION,
            "compression_level": COMPRESSION_LEVEL,
            "row_group_rows": config.storage.row_group_rows,
            "target_file_mb": config.storage.target_file_mb,
            **storage,
        },
        "source_bytes": {
            "physical_parquet": physical,
            "decimal_GB": physical / 10**9,
            "GiB": physical / 1024**3,
            "uncompressed_parquet": sum(item["uncompressed_parquet_bytes"] for item in inventory),
            "minimum_physical_parquet": config.minimum_source_parquet_bytes,
        },
        "inventory": inventory,
        "validation": validation,
        "execution_attempts": run.state["attempts"],
        "configured": {
            "duplicate_rate": config.data_problems.duplicate_rate,
            "acceptance_bands": {name: band.model_dump() for name, band in config.acceptance_bands.items()},
            "schema_cutover_date": config.data_problems.schema_cutover_date.isoformat(),
            "merchant_zipf_exponent": config.behavior.merchant_zipf_exponent,
            "category_zipf_exponent": config.behavior.category_zipf_exponent,
            "cardinality": config.counts.model_dump(),
            "scenario_rates": {name: settings.rate for name, settings in config.scenarios},
        },
        "observed": observed,
    }


def generate(
    config: HistoricalConfig,
    filesystem: fs.FileSystem,
    root: str,
    *,
    resume=False,
    prepare_only=False,
    takeover_stopped_writer=False,
    cleanup_intermediates=False,
    allow_small_dataset=False,
    store=None,
) -> dict:
    prefix = f"{root.rstrip('/')}/historical/{config.dataset_version}"
    store = store or ObjectStore(prefix, remote=not isinstance(filesystem, fs.LocalFileSystem))
    if store.exists("manifest.json"):
        if resume and not prepare_only:
            return finish_publication(config, store, cleanup_intermediates=cleanup_intermediates)
        raise FileExistsError(f"dataset version already published: {prefix}/manifest.json")
    has_state = store.exists("_run/state.json")
    if has_state and not resume:
        raise FileExistsError("incomplete dataset requires resume")
    resume = resume and has_state
    descriptor, revision = _descriptor(), _generator_revision()
    identity = config.generation_identity(descriptor, revision)
    if has_state:
        existing = store.json("_run/state.json")
        if existing.get("version") != RUN_STATE_VERSION or existing.get("generation_identity") != identity:
            raise ValueError("incompatible run state: format or generation/contract/revision identity mismatch")
        if existing.get("owner") and not takeover_stopped_writer:
            raise FileExistsError(
                "another writer owns this dataset; stop it before explicit --takeover-stopped-writer"
            )
    elif store.has_objects():
        raise FileExistsError("incomplete dataset has no supported run state; use a new dataset version")
    execution = config.execution
    execution.scratch_directory.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(prefix="historical-", dir=execution.scratch_directory) as directory:
        scratch = Path(directory)
        profile_bytes = (config.counts.customers * 13 + config.counts.merchants * 2) * 8 + 16384
        reserved = execution.max_in_flight * execution.task_scratch_bytes + execution.duckdb_spill_bytes
        if profile_bytes + reserved >= execution.scratch_capacity_bytes:
            raise CapacityExceeded("profile mappings plus task/spill reservations exceed combined scratch capacity")
        profile_start = time.monotonic()
        with ResourceMonitor(scratch, execution) as profile_monitor:
            profiles = build_profiles(config, scratch / "profiles")
            profile_monitor.check()
        profile_resources = profile_monitor.report()
        profile_seconds = time.monotonic() - profile_start
        capabilities = (
            store.probe()
            if store.remote
            else {"create_if_absent": True, "etag_update": True, "integrity": "streamed-sha256"}
        )
        run = RunState(store, identity, profiles.digest(), resume=resume, takeover=takeover_stopped_writer)
        with (
            ResourceMonitor(scratch, execution) as monitor,
            ThreadPoolExecutor(execution.transfer_concurrency) as transfers,
        ):
            configuration = config.model_dump(mode="json")
            run.state.setdefault("configuration", configuration)
            run.state.setdefault(
                "config_sha256", sha256(json.dumps(configuration, sort_keys=True).encode()).hexdigest()
            )
            run.state.setdefault("algorithm_version", ALGORITHM_VERSION)
            run.state.setdefault("generator_revision", revision)
            run.state.setdefault("data_product", descriptor)
            run.attempt(
                {
                    **execution.model_dump(mode="json"),
                    "chunk_purchases": config.chunk_purchases,
                    "destination": store.uri(""),
                }
            )
            timings = {"profile_setup_seconds": profile_seconds}
            index = None
            connection = None
            try:
                monitor.check()
                connection = _connection(config, scratch, store)
                monitor.connection = connection
                prepared = _receipt(store, "_run/prepared.json") if resume else None
                if prepared and prepared.get("generation_identity") == identity:
                    stats = DatasetStatistics.from_dict(prepared["statistics"])
                    inventory, partitions = prepared["inventory"], prepared["partitions"]
                    intermediate_bytes = prepared["intermediate_bytes"]
                    timings["generation_seconds"] = timings["finalization_seconds"] = 0
                else:
                    start = time.monotonic()
                    index = FragmentIndex(scratch / "fragments.sqlite")
                    stats = _generate(config, profiles, scratch, store, run, monitor, transfers, index)
                    timings["generation_seconds"] = time.monotonic() - start
                    intermediate_bytes = index.total_bytes()
                    run.state["phase"] = "finalization"
                    run.checkpoint()
                    start = time.monotonic()
                    inventory, partitions = _finalize(
                        config, index, scratch, store, run, connection, monitor, transfers
                    )
                    timings["finalization_seconds"] = time.monotonic() - start
                    prepared = {
                        "generation_identity": identity,
                        "inventory": inventory,
                        "partitions": partitions,
                        "statistics": stats.to_dict(),
                        "profile_sha256": profiles.digest(),
                        "intermediate_bytes": intermediate_bytes,
                    }
                    run.receipt("_run/prepared.json", {**prepared, "files": inventory})
                run.state["phase"] = "prepared"
                run.checkpoint()
                if prepare_only:
                    return {
                        "dataset_version": config.dataset_version,
                        "generation_identity": identity,
                        "phase": "prepared",
                        "config_sha256": sha256(
                            json.dumps(config.model_dump(mode="json"), sort_keys=True).encode()
                        ).hexdigest(),
                        "history_end": stats.history_end.isoformat() if stats.history_end else None,
                    }
                if config.counts.customers < config.customer_block_size and not allow_small_dataset:
                    raise ValueError(
                        f"publication refused: {config.counts.customers} customers is below one "
                        f"{config.customer_block_size}-customer block; pass --allow-small-dataset for a contract "
                        "probe or generate locally with --output"
                    )
                _views(connection, inventory, store, scratch)
                run.state["phase"] = "validation"
                run.checkpoint()
                validation = {
                    "result": "skipped",
                    "gate": "dp1-spark-gx",
                    "reason": "validation deferred to dp1-spark-gx; the downstream pipeline is the contract gate",
                }
                observed = _observations(connection, stats)
                _gate_observed(config, observed)
                monitor.check()
                physical = sum(item["bytes"] for item in inventory)
                if physical < config.minimum_source_parquet_bytes:
                    raise ValueError(
                        f"physical source byte gate failed: observed {physical}, required {config.minimum_source_parquet_bytes}"
                    )
                if not all(store.verify(item) for item in inventory):
                    raise ValueError("final object integrity changed after validation; no publication")
                report = {
                    **monitor.report(),
                    "timings": timings,
                    "remote_intermediate_bytes": intermediate_bytes,
                    "source_bytes": physical,
                    "capabilities": capabilities,
                    "local_profile_bytes": directory_bytes(scratch / "profiles"),
                    "transfers": store.transfer_report(),
                    "profile_setup": profile_resources,
                }
                run.state["attempts"][-1]["resources"] = report
                run.state["phase"] = "validated"
                run.checkpoint()
                manifest = _manifest(
                    config, run, profiles, stats, inventory, partitions, validation, observed, revision, descriptor
                )
                run.check_owner()
                store.put_json(PUBLICATION_KEY, publication_receipt(store, manifest))
                store.put_json("manifest.json", manifest, absent=True)
                complete_publication(store, manifest, cleanup_intermediates=cleanup_intermediates)
                return manifest
            except BaseException as error:
                run.state["failure"] = {"phase": run.state["phase"], "type": type(error).__name__}
                raise
            finally:
                if index:
                    index.close()
                if connection:
                    monitor.connection = None
                    connection.close()
                run.state["attempts"][-1].setdefault(
                    "resources",
                    {
                        **monitor.report(),
                        "timings": timings,
                        "transfers": store.transfer_report(),
                        "profile_setup": profile_resources,
                    },
                )
                run.release()
