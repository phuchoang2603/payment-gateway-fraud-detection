import json
import re
from hashlib import sha256
from itertools import zip_longest

from fraud_batch.common.landing import identity, selection, state_key
from fraud_batch.common.manifest import supported_versions
from fraud_batch.common.products import PORTS

FORMAT = "decoded-payments-parquet-v1"
TOPICS = {"payments.events.v1": "payment_events", "fraud.labels.v1": "fraud_labels"}


def validate_batch(key, body, epoch):
    batch = json.loads(body)
    if batch.get("format") != FORMAT or batch.get("source_epoch") != epoch:
        raise ValueError("unsupported landing format or epoch")
    topic = batch.get("topic")
    if topic not in TOPICS:
        raise ValueError("only authoritative clean topics may enter bronze")
    for field in ("partition", "first_offset", "last_offset", "rows"):
        if type(batch.get(field)) is not int or batch[field] < 0:
            raise ValueError("invalid landing transport range")
    first, last = batch["first_offset"], batch["last_offset"]
    if last < first or not 0 < batch["rows"] <= last - first + 1:
        raise ValueError("invalid landing row accounting")
    stem = f"epoch={epoch}/topic={topic}/partition={batch['partition']}/{first:020d}-{last:020d}"
    digest = sha256(body).hexdigest()
    if key != f"streaming/_batches/{stem}-{digest}.json":
        raise ValueError("landing manifest identity changed")
    seen = set()
    for entry in batch["files"]:
        table, version = TOPICS[topic], entry.get("schema_version")
        if entry.get("table") != table or type(version) is not int or version not in supported_versions(table):
            raise ValueError("unsupported landing table/schema")
        if not re.fullmatch("[0-9a-f]{64}", entry.get("sha256", "")):
            raise ValueError("landing file requires SHA-256")
        if any(type(entry.get(f)) is not int or entry[f] <= 0 for f in ("bytes", "rows")):
            raise ValueError("invalid landing file sizes")
        if entry["key"] != f"streaming/{table}/{stem}-v{version}-{entry['sha256']}.parquet":
            raise ValueError("landing file escapes declared transport range")
        if entry["key"] in seen:
            raise ValueError("duplicate landing file")
        seen.add(entry["key"])
    if not seen or sum(f["rows"] for f in batch["files"]) != batch["rows"]:
        raise ValueError("landing batch rows differ from files")
    return batch, digest


def select(source, store, epoch, max_batches=100):
    identity(epoch)
    progress = store.get(state_key(epoch)) or {"source_root": source.root, "accepted": {}, "ranges": []}
    if progress["source_root"] != source.root:
        raise ValueError("landing source changed under an existing epoch")
    if progress.get("pending"):
        manifest, _ = selection(store, progress["pending"])
        for item in manifest["batches"]:
            if sha256(source.read(item["key"], max_bytes=1024 * 1024)).hexdigest() != item["sha256"]:
                raise ValueError("pending landing manifest changed")
        return manifest
    picked, ranges = [], list(progress["ranges"])
    keys = list(source.list(f"streaming/_batches/epoch={epoch}/"))
    groups = [[k for k in keys if f"/topic={topic}/" in k] for topic in TOPICS]
    if sum(map(len, groups)) != len(keys):
        raise ValueError("only authoritative clean topics may enter bronze")
    for key in (k for pair in zip_longest(*groups) for k in pair if k is not None):
        body = source.read(key, max_bytes=1024 * 1024)
        digest = sha256(body).hexdigest()
        if key in progress["accepted"]:
            if progress["accepted"][key] != digest:
                raise ValueError("accepted landing manifest changed")
            continue
        if len(picked) >= max_batches:
            continue
        batch, digest = validate_batch(key, body, epoch)
        span = [batch["topic"], batch["partition"], batch["first_offset"], batch["last_offset"]]
        if any(span[:2] == r[:2] and span[2] <= r[3] and r[2] <= span[3] for r in ranges):
            raise ValueError("overlapping landing transport positions")
        ranges.append(span)
        picked.append({"key": key, "sha256": digest, "range": span, "files": batch["files"]})
    if not picked:
        return None
    digest = sha256(json.dumps(picked, sort_keys=True).encode()).hexdigest()
    version = f"landing-{digest}"
    manifest = {
        "input_mode": "landing",
        "dataset_version": version,
        "source_epoch": epoch,
        "source_root": source.root,
        "batches": picked,
        "inventory": [{**f, "batch": b["key"], "range": b["range"]} for b in picked for f in b["files"]],
        "totals": {t: sum(f["rows"] for b in picked for f in b["files"] if f["table"] == t) for t in PORTS},
    }
    existing = store.get(f"selections/{version}.json")
    if existing is not None and existing != manifest:
        raise ValueError("landing selection collision")
    if existing is None:
        store.put(f"selections/{version}.json", manifest, create=True)
    progress["pending"] = version
    store.put(state_key(epoch), progress)
    return manifest


def advance(store, run):
    manifest, digest = selection(store, run["dataset_version"])
    if digest != run["manifest_sha256"]:
        raise ValueError("landing selection changed")
    key = state_key(manifest["source_epoch"])
    progress = store.get(key)
    if progress.get("pending") not in (None, run["dataset_version"]):
        raise ValueError("another landing candidate owns progress")
    for batch in manifest["batches"]:
        if batch["key"] not in progress["accepted"]:
            progress["accepted"][batch["key"]] = batch["sha256"]
            progress["ranges"].append(batch["range"])
    progress.pop("pending", None)
    progress["latest_version"] = run["dataset_version"]
    progress["snapshots"] = run["snapshots"]
    store.put(key, progress)
