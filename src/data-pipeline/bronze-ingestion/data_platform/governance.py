import os
import time
from datetime import datetime
from urllib.parse import quote

import yaml

from data_platform.catalog import discover, load_contract, register_source
from data_platform.openmetadata import Client
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


def write(client, run, report, lake, source, report_uri, owner):
    owners = [{"type": "user", "name": owner}] if owner else []
    timestamp = int(datetime.fromisoformat(report["run_time"]).timestamp() * 1000)
    client.put(
        "/v1/domains",
        {"name": DOMAIN, "domainType": "Aggregate", "description": "Fraud detection data platform"},
    )
    assets = {"historical-payments": source, "bronze-payments": lake}
    for product, members in assets.items():
        doc = product_document(product)
        client.put(
            "/v1/dataProducts",
            {
                "name": product,
                "displayName": doc["name"],
                "description": f"{doc['description']['purpose']} ODPS {doc['id']} version {doc['version']}.",
                "domains": [DOMAIN],
                "owners": owners,
                "assets": [reference(i) for i in members.values()],
            },
        )
        for table, identity in members.items():
            contract_file = contract_path(table, product)
            client.put(
                "/v1/dataContracts/odcs/yaml",
                contract_file.read_bytes(),
                {
                    "entityId": identity["id"],
                    "entityType": "table",
                    "mode": "replace",
                    "createTestCases": "false",
                    "objectName": load_contract(table, product)["schema"][0]["name"],
                },
                "application/yaml",
            )
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
            client.put(f"/v1/dataQuality/testCases/testCaseResults/{quote(fqn, safe='')}", result)
            results[fqn] = status
    return {
        "timestamp": timestamp,
        "results": results,
        "pipeline_status": "Successful" if ingest_ok and validate_ok else "Failed",
        "assets": assets,
    }


def missing(client, expected, lake, source):
    gaps = []
    window = {"startTs": expected["timestamp"], "endTs": expected["timestamp"]}
    for product, members in expected["assets"].items():
        found = client.find(f"/v1/dataProducts/name/{quote(product, safe='')}", {"fields": "assets"})
        have = {a["id"] for a in (found or {}).get("assets", [])}
        gaps += [("dataProduct", product, i["fqn"]) for i in members.values() if i["id"] not in have]
        for identity in members.values():
            table = client.find(f"/v1/tables/name/{quote(identity['fqn'], safe='')}", {"fields": "columns"})
            if not table or not table.get("columns"):
                gaps.append(("table", identity["fqn"], "schema"))
            contract = client.find("/v1/dataContracts/entity", {"entityId": identity["id"], "entityType": "table"})
            if not contract:
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
    for fqn, status in expected["results"].items():
        found = client.find(f"/v1/dataQuality/testCases/testCaseResults/{quote(fqn, safe='')}", window)
        if not any(r["testCaseStatus"] == status for r in (found or {}).get("data", [])):
            gaps.append(("testResult", fqn, status))
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
