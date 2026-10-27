import os
from datetime import datetime
from hashlib import sha256

import yaml

from data_platform.products import PORTS, PRODUCTS, contract_path
from data_platform.quality import entity

DOMAIN = "fraud-detection"
PRODUCT = "bronze-payments"
PIPELINE = "argo.historical-dp1-bronze"
STORAGE = "rustfs"
TASKS = {
    "select": ["ingest"],
    "ingest": ["discover"],
    "discover": ["validate", "profile"],
    "validate": ["publish"],
    "profile": ["publish"],
    "publish": [],
}
TYPES = {
    "string": "STRING",
    "binary": "BINARY",
    "long": "BIGINT",
    "int": "INT",
    "double": "DOUBLE",
    "boolean": "BOOLEAN",
    "timestamp": "TIMESTAMP",
}


def product_document(product=PRODUCT):
    return yaml.safe_load((PRODUCTS / product / "product.odps.yaml").read_text())


def columns(properties):
    result = []
    for prop in properties:
        logical = prop["logicalType"]
        dtype = "STRUCT" if logical == "object" else "ARRAY" if logical == "array" else TYPES[prop["physicalType"]]
        col = {"name": prop["name"], "dataType": dtype, "description": prop.get("description", prop["name"])}
        if prop.get("required"):
            col["constraint"] = "NOT_NULL"
        if logical == "object":
            col["children"] = columns(prop["properties"])
        elif logical == "array":
            item = prop["items"]
            if item["logicalType"] == "array":
                raise ValueError("nested array contract mapping is unsupported")
            col["arrayDataType"] = "STRUCT" if item["logicalType"] == "object" else TYPES[item["physicalType"]]
            if item["logicalType"] == "object":
                col["children"] = columns(item["properties"])
        result.append(col)
    return result


def contract_body(table, entity_id, owners, tests):
    artifact = contract_path(table)
    original = artifact.read_bytes()
    contract = yaml.safe_load(original)
    if not owners or not tests:
        raise ValueError("native contract requires resolved ownership and active quality references")
    body = {
        "name": contract["id"],
        "displayName": contract["name"],
        "description": f"ODCS id={contract['id']} version={contract['version']} sha256={sha256(original).hexdigest()}\n\n```yaml\n{original.decode()}\n```",
        "entity": {"id": entity_id, "type": "table"},
        "schema": columns(contract["schema"][0]["properties"]),
        "qualityExpectations": tests,
        "owners": owners,
        "sourceUrl": f"{os.environ.get('GIT_SOURCE_URL', 'https://github.com/phuchoang2603/payment-gateway-fraud-detection/blob/t3/historical-dp1-bronze')}/contracts/data-products/{PRODUCT}/{artifact.name}",
        "termsOfUse": "\n\n".join(contract["description"].values()),
    }
    classification = next(
        (p["value"] for p in contract.get("customProperties", []) if p["property"] == "classification"), None
    )
    if classification:
        body["security"] = {"dataClassification": classification}
    if contract.get("team") != product_document()["team"]:
        raise ValueError("contract requires separately resolved declared ownership")
    for field in ("semantics", "security", "sla"):
        if field in contract:
            body[field] = contract[field]
    if "slaProperties" in contract:
        raise ValueError("ODCS slaProperties require an explicit supported native SLA mapping")
    body["termsOfUse"] = contract.get("termsOfUse", body["termsOfUse"])
    return body


def source_paths(run, manifest):
    from urllib.parse import urlsplit

    root = urlsplit(run["source_root"])
    if root.scheme != "s3" or not root.netloc:
        raise ValueError("catalog source assets require actual S3 source locations")
    result = {}
    for table in PORTS:
        files = [f for f in manifest["inventory"] if f["table"] == table]
        if not files:
            continue
        relative = f"streaming/{table}/" if run.get("input_mode") == "landing" else f"raw/{table}/"
        if any(not f["key"].startswith(relative) for f in files):
            raise ValueError("source files do not belong to the declared port path")
        prefix = root.path.strip("/")
        prefix = f"{prefix}/{relative}" if prefix else relative
        name = "source_" + sha256(f"{root.netloc}/{prefix}".encode()).hexdigest()[:20]
        result[table] = {
            "name": name,
            "fullPath": f"s3://{root.netloc}/{prefix}",
            "prefix": prefix,
            "rows": sum(f["rows"] for f in files),
            "bytes": sum(f["bytes"] for f in files),
            "files": len(files),
        }
    return result


def execution(run, manifest, sources, stages, outcome):
    started = int(datetime.fromisoformat(run["ingested_at"]).timestamp() * 1000)
    inputs = [
        {
            "datasetFQN": f"{STORAGE}.{s['name']}",
            "version": run["manifest_sha256"],
            "rowCount": s["rows"],
            "sizeBytes": s["bytes"],
        }
        for s in sources.values()
    ]
    outputs = [
        {
            "datasetFQN": entity(t),
            "version": str(snapshot),
            "mode": "append" if run.get("input_mode") == "landing" else "overwrite",
            "rowCount": sum(f["rows"] for f in manifest["inventory"] if f["table"] == t),
        }
        for t, snapshot in run["snapshots"].items()
    ]
    task_status = []
    for name in TASKS:
        if name in ("select", "ingest"):
            state = run.get("stages", {}).get(name, {"outcome": "missing"})
        elif name == "publish":
            state = {**stages.get("publish", {}), "outcome": outcome}
        else:
            state = stages.get(name, {"outcome": "missing"})
        status = {
            "name": name,
            "executionStatus": {"passed": "Successful", "skipped": "Skipped", "pending": "Pending"}.get(
                state["outcome"], "Failed"
            ),
            **({"startTime": state["started_at"]} if "started_at" in state else {}),
            **({"endTime": state["ended_at"]} if "ended_at" in state else {}),
        }
        if name == "ingest":
            status.update(inputs=inputs, outputs=outputs)
        task_status.append(status)
    return {
        "timestamp": started,
        "executionId": run["run_id"],
        "executionStatus": {"passed": "Successful", "pending": "Pending"}.get(outcome, "Failed"),
        "taskStatus": task_status,
        "endTime": max(s.get("endTime", started) for s in task_status),
        "inputs": inputs,
        "outputs": outputs,
        "parameters": {
            "manifest_sha256": run["manifest_sha256"],
            "snapshots": run["snapshots"],
            "workflow": run["workflow"],
            "publication_workflow": os.environ.get("WORKFLOW_ID", run["workflow"]),
        },
        "metrics": {
            "totalRowsProcessed": sum(s["rows"] for s in sources.values()),
            "totalBytesProcessed": sum(s["bytes"] for s in sources.values()),
        },
    }
