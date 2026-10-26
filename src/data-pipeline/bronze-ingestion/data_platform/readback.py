import json
import os
import urllib.parse
import urllib.request

from data_platform.quality import entity

PRODUCT = "bronze-payments"
PAGE = 1000


def get(path, query=None):
    base = os.environ["OPENMETADATA_URL"].rstrip("/")
    url = f"{base}/v1/{path}" + (f"?{urllib.parse.urlencode(query)}" if query else "")
    request = urllib.request.Request(url, headers={"Authorization": f"Bearer {os.environ['OPENMETADATA_TOKEN']}"})
    with urllib.request.urlopen(request, timeout=60) as response:
        return json.load(response)


def observed(table, fetch=get):
    link = f"<#E::table::{entity(table)}>"
    cases, after = {}, None
    while True:
        query = {"entityLink": link, "fields": "testCaseResult", "limit": PAGE}
        if after:
            query["after"] = after
        page = fetch("dataQuality/testCases", query)
        cases.update({case["fullyQualifiedName"]: case for case in page.get("data", [])})
        after = page.get("paging", {}).get("after")
        if not after:
            return cases


def verify(expected, found, prepared_at):
    """Return {case: problem} for every expected native result that is not a fresh, matching Success."""
    from hashlib import sha256

    problems = {}
    for fqn, rule in expected["cases"].items():
        case = found.get(fqn)
        if case is None:
            problems[fqn] = "missing"
            continue
        sql = next((p["value"] for p in case.get("parameterValues", []) if p["name"] == "sqlExpression"), None)
        result = case.get("testCaseResult") or {}
        values = {v["name"]: v["value"] for v in result.get("testResultValue", [])}
        if sql is None or sha256(sql.encode()).hexdigest() != rule["sql_sha256"]:
            problems[fqn] = "stale definition"
        elif result.get("timestamp", 0) < prepared_at:
            problems[fqn] = "stale result"
        elif result.get("testCaseStatus") != "Success":
            problems[fqn] = result.get("testCaseStatus", "missing result")
        elif values.get("resultRowCount") != "0":
            problems[fqn] = "nonzero violations"
    return problems


def governance_gaps(tables, hashes, fetch=get):
    """Read back the product membership, contract provenance and lineage written by the workflow."""
    gaps = []
    ids = {}
    for table in tables:
        found = fetch(f"tables/name/{urllib.parse.quote(entity(table), safe='')}")
        ids[table] = found["id"]
    members = {a["id"] for a in fetch(f"dataProducts/name/{PRODUCT}/assets", {"limit": 100}).get("data", [])}
    gaps += [f"product:{table}" for table, identity in ids.items() if identity not in members]
    for table, identity in ids.items():
        contract = fetch("dataContracts/entity", {"entityId": identity, "entityType": "table"})
        if hashes[table] not in contract.get("description", ""):
            gaps.append(f"contract:{table}")
    upstream = fetch(
        f"lineage/table/name/{urllib.parse.quote(entity('fraud_labels'), safe='')}",
        {"upstreamDepth": 1, "downstreamDepth": 0},
    )
    edges = {(e["fromEntity"], e["toEntity"]) for e in upstream.get("upstreamEdges", [])}
    if (ids["payment_events"], ids["fraud_labels"]) not in edges:
        gaps.append("lineage:payment_events->fraud_labels")
    return gaps
