import json
import os
import time
from datetime import datetime
from hashlib import sha256
from urllib.parse import quote

import yaml

from data_platform.catalog import discover, load_contract, register_source
from data_platform.openmetadata import ApiError, Client
from data_platform.products import PORTS, PRODUCTS, contract_path
from data_platform.runs import report_key, save_run

DOMAIN = "fraud-detection"
PIPELINE_SERVICE = "argo"
PIPELINE = "historical-dp1-bronze"
TEST_DEFINITION = "dp1GreatExpectationsRule"
PIPELINE_FQN = f"{PIPELINE_SERVICE}.{PIPELINE}"


def link(fqn):
    return f"<#E::table::{fqn}>"


def reference(identity, kind="table"):
    return {"id": identity["id"], "type": kind}


def rule_results(report, table):
    """Yield (test name, status, detail) for each persisted GX rule, then unresolved execution errors."""
    seen = {}
    for raw in report["tables"].get(table, {}).get("gx", {}).get("results", []):
        base = str(raw.get("expectation_config", {}).get("meta", {}).get("id", "schema"))
        seen[base] = seen.get(base, 0) + 1
        name = base if seen[base] == 1 else f"{base}-{seen[base]}"
        if raw.get("exception_info", {}).get("raised_exception"):
            status = "Aborted"
        else:
            status = "Success" if raw.get("success") else "Failed"
        unexpected = (raw.get("result") or {}).get("unexpected_count")
        yield name, status, {"unexpected_count": "" if unexpected is None else str(unexpected)}
    if report.get("execution_errors"):
        yield "dp1.execution", "Aborted", {"stage": ",".join(e["stage"] for e in report["execution_errors"])}


def evidence_text(report, table):
    return (
        f"run_id={report['run_id']} manifest_sha256={report['manifest_sha256']} "
        f"contract_sha256={report['contracts'][table]} snapshot_id={report['snapshots'][table]}"
    )


def product_document(product):
    return yaml.safe_load((PRODUCTS / product / "product.odps.yaml").read_text())


def column_lineage(table, source, lake):
    contract = load_contract(table, "bronze-payments")
    names = [p["name"] for p in contract["schema"][0]["properties"] if not p["name"].startswith("_")]
    return [{"fromColumns": [f"{source['fqn']}.{n}"], "toColumn": f"{lake['fqn']}.{n}"} for n in names]


def contract_document(table, product, identity):
    artifact = contract_path(table, product)
    original = artifact.read_bytes()
    contract = yaml.safe_load(original)
    rules = []
    for element in contract["schema"]:
        for rule in element.get("quality", []):
            rule = dict(rule)
            if isinstance(rule.get("implementation"), (dict, list)):
                rule["implementation"] = json.dumps(rule["implementation"], sort_keys=True)
            rules.append(rule)
    provenance = (
        f"ODCS id={contract['id']} version={contract['version']} sha256={sha256(original).hexdigest()} "
        f"artifact=contracts/data-products/{product}/{artifact.name}"
    )
    return {
        "name": contract["id"],
        "displayName": contract["name"],
        "description": provenance + "\n\n```yaml\n" + original.decode() + "\n```",
        "entity": reference(identity),
        "schema": identity["columns"],
        "odcsQualityRules": rules,
        "termsOfUse": contract["description"]["usage"],
    }


def contains(actual, expected):
    if isinstance(expected, dict):
        return isinstance(actual, dict) and all(k in actual and contains(actual[k], v) for k, v in expected.items())
    if isinstance(expected, list):
        return (
            isinstance(actual, list)
            and len(actual) == len(expected)
            and all(contains(a, e) for a, e in zip(actual, expected))
        )
    return actual == expected


def publish_result(client, path, result):
    try:
        client.post(path, result)
    except ApiError as failure:
        if failure.status != 409:
            raise
        found = client.get(path, {"startTs": result["timestamp"], "endTs": result["timestamp"]})
        if not found.get("data"):
            raise RuntimeError("existing quality result is not available for readback") from None
        if not any(contains(existing, result) for existing in found.get("data", [])):
            raise ValueError("existing quality result differs from persisted execution evidence") from None


def write(client, run, report, lake, source, report_uri, owner):
    owners = [{"type": "user", "name": owner}] if owner else []
    timestamp = int(datetime.fromisoformat(report["run_time"]).timestamp() * 1000)
    domain = client.put(
        "/v1/domains",
        {"name": DOMAIN, "domainType": "Aggregate", "description": "Fraud detection data platform"},
    )
    assets = {"historical-payments": source, "bronze-payments": lake}
    contracts = {}
    for product, members in assets.items():
        doc = product_document(product)
        for identity in members.values():
            client.patch(
                f"/v1/tables/{identity['id']}",
                [{"op": "add", "path": "/domains", "value": [reference(domain, "domain")]}],
            )
        client.put(
            "/v1/dataProducts",
            {
                "name": product,
                "displayName": doc["name"],
                "description": f"{doc['description']['purpose']} ODPS {doc['id']} version {doc['version']}.",
                "domains": [DOMAIN],
                "owners": owners,
            },
        )
        client.put(
            f"/v1/dataProducts/{quote(product, safe='')}/assets/add",
            {"assets": [reference(i) for i in members.values()]},
        )
        for table, identity in members.items():
            document = contract_document(table, product, identity)
            client.put("/v1/dataContracts", document)
            contracts[identity["id"]] = document
    client.put("/v1/services/pipelineServices", {"name": PIPELINE_SERVICE, "serviceType": "CustomPipeline"})
    pipeline = client.put(
        "/v1/pipelines",
        {
            "name": PIPELINE,
            "displayName": "Historical DP1 bronze",
            "service": PIPELINE_SERVICE,
            "tasks": [{"name": "ingest"}, {"name": "validate"}],
            "sourceUrl": run["workflow"] if run["workflow"].startswith("https://") else None,
        },
    )
    description = (
        f"workflow={run['workflow']} run_id={run['run_id']} manifest_sha256={run['manifest_sha256']} "
        f"snapshots={run['snapshots']} report={report_uri}"
    )
    for table in PORTS:
        edge = {
            "fromEntity": reference(source[table]),
            "toEntity": reference(lake[table]),
            "lineageDetails": {
                "description": description,
                "pipeline": reference(pipeline, "pipeline"),
                "columnsLineage": column_lineage(table, source[table], lake[table]),
            },
        }
        client.put("/v1/lineage", {"edge": edge})
    client.put(
        "/v1/lineage",
        {
            "edge": {
                "fromEntity": reference(lake["payment_events"]),
                "toEntity": reference(lake["fraud_labels"]),
                "lineageDetails": {"description": "label validation depends on payment events; " + description},
            }
        },
    )
    ingest_ok = set(run["snapshots"]) == set(PORTS)
    validate_ok = report["data_outcome"] == "passed"
    client.put(
        f"/v1/pipelines/{quote(PIPELINE_FQN, safe='')}/status",
        {
            "timestamp": timestamp,
            "executionStatus": "Successful" if ingest_ok and validate_ok else "Failed",
            "taskStatus": [
                {"name": "ingest", "executionStatus": "Successful" if ingest_ok else "Failed", "logLink": report_uri},
                {
                    "name": "validate",
                    "executionStatus": "Successful" if validate_ok else "Failed",
                    "logLink": report_uri,
                },
            ],
        },
    )
    client.put(
        "/v1/dataQuality/testDefinitions",
        {
            "name": TEST_DEFINITION,
            "displayName": "DP1 Great Expectations rule",
            "description": "Result of a contract rule evaluated by the DP1 Spark/GX validation gate.",
            "entityType": "TABLE",
            "testPlatforms": ["GreatExpectations"],
            "parameterDefinition": [],
        },
    )
    results = {}
    for table, identity in lake.items():
        for name, status, values in rule_results(report, table):
            case = client.put(
                "/v1/dataQuality/testCases",
                {
                    "name": name,
                    "entityLink": link(identity["fqn"]),
                    "testDefinition": TEST_DEFINITION,
                    "description": f"Contract rule {name} from {contract_path(table)}.",
                    "parameterValues": [],
                },
            )
            result = {
                "timestamp": timestamp,
                "testCaseStatus": status,
                "result": evidence_text(report, table) + f" report={report_uri}",
                "testResultValue": [{"name": k, "value": v} for k, v in values.items()],
            }
            fqn = case["fullyQualifiedName"]
            publish_result(client, f"/v1/dataQuality/testCases/testCaseResults/{quote(fqn, safe='')}", result)
            results[fqn] = result
    return {
        "timestamp": timestamp,
        "results": results,
        "pipeline_status": "Successful" if ingest_ok and validate_ok else "Failed",
        "assets": assets,
        "contracts": contracts,
    }


def missing(client, expected, lake, source):
    gaps = []
    window = {"startTs": expected["timestamp"], "endTs": expected["timestamp"]}
    for product, members in expected["assets"].items():
        found = client.find(f"/v1/dataProducts/name/{quote(product, safe='')}/assets", {"limit": 100})
        have = {a["id"] for a in (found or {}).get("data", [])}
        gaps += [("dataProduct", product, i["fqn"]) for i in members.values() if i["id"] not in have]
        for identity in members.values():
            table = client.find(f"/v1/tables/name/{quote(identity['fqn'], safe='')}", {"fields": "columns"})
            if not table or not table.get("columns") or not contains(table["columns"], identity["columns"]):
                gaps.append(("table", identity["fqn"], "schema"))
            contract = client.find("/v1/dataContracts/entity", {"entityId": identity["id"], "entityType": "table"})
            document = expected["contracts"][identity["id"]]
            if (
                not contract
                or contract.get("name") != document["name"]
                or contract.get("description") != document["description"]
                or not contains(contract.get("entity"), document["entity"])
                or not contains(contract.get("odcsQualityRules", []), document["odcsQualityRules"])
                or not contains(contract.get("schema"), document["schema"])
                or contract.get("termsOfUse") != document["termsOfUse"]
            ):
                gaps.append(("contract", identity["fqn"], product))
    for table in PORTS:
        found = client.find(
            f"/v1/lineage/table/name/{quote(lake[table]['fqn'], safe='')}",
            {"upstreamDepth": 1, "downstreamDepth": 0},
        )
        sources = {(e["fromEntity"], e["toEntity"]) for e in (found or {}).get("upstreamEdges", [])}
        needed = {(source[table]["id"], lake[table]["id"])}
        if table == "fraud_labels":
            needed.add((lake["payment_events"]["id"], lake["fraud_labels"]["id"]))
        gaps += [("lineage", lake[table]["fqn"], str(pair)) for pair in needed - sources]
    for fqn, result in expected["results"].items():
        found = client.find(f"/v1/dataQuality/testCases/testCaseResults/{quote(fqn, safe='')}", window)
        if not any(contains(r, result) for r in (found or {}).get("data", [])):
            gaps.append(("testResult", fqn, result["testCaseStatus"]))
    found = client.find(f"/v1/pipelines/{quote(PIPELINE_FQN, safe='')}/status", window)
    if not any(r["executionStatus"] == expected["pipeline_status"] for r in (found or {}).get("data", [])):
        gaps.append(("pipelineStatus", PIPELINE_FQN, expected["pipeline_status"]))
    return gaps


def publish(store, run, contracts, client=None, discover_fn=discover, sleep=time.sleep):
    key = report_key(run, contracts)
    report = store.get(key)
    outcome = {
        "outcome": "error",
        "readback_verified": False,
        "data_outcome": "error" if not report else report["data_outcome"],
    }
    owner = os.environ.get("OPENMETADATA_OWNER")
    try:
        if not report:
            raise ValueError("missing validation report is an execution error")
        client = client or Client()
        identities = store.get(f"runs/{run['run_id']}/catalog-identities.json")
        if identities is None:
            identities = discover_fn(client, run["run_id"], owner)
            store.put(f"runs/{run['run_id']}/catalog-identities.json", identities, create=True)
        if set(identities) != set(PORTS):
            raise ValueError("missing canonical identities")
        for attempt in range(3):
            try:
                source = register_source(client, run, owner)
                expected = write(client, run, report, identities, source, store.root + "/" + key, owner)
                for read_attempt in range(4):
                    gaps = missing(client, expected, identities, source)
                    if not gaps:
                        break
                    sleep(min(2**read_attempt, 8))
                if gaps:
                    raise RuntimeError("catalog readback is incomplete")
                outcome.update(outcome="passed", readback_verified=True, identities=identities)
                break
            except Exception:
                if attempt == 2:
                    raise
                sleep(2**attempt)
    except Exception as error:
        outcome["error_type"] = type(error).__name__
    finally:
        store.put(key.replace("validation.json", "governance.json"), outcome)
        run["governance"] = outcome["outcome"]
        save_run(store, run)
    return outcome
