import urllib.parse

from data_platform.quality import entity

PRODUCT = "bronze-payments"
PAGE = 1000


def get(path, query=None):
    from data_platform.catalog import client

    return client().client.get(f"/{path}", query)


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
        values = {v["name"]: v.get("value") for v in result.get("testResultValue", [])}
        if sql is None or sha256(sql.encode()).hexdigest() != rule["sql_sha256"]:
            problems[fqn] = "stale definition"
        elif result.get("timestamp", 0) < prepared_at:
            problems[fqn] = "stale result"
        elif result.get("testCaseStatus") != "Success":
            problems[fqn] = result.get("testCaseStatus", "missing result")
        elif values.get("resultRowCount") != "0":
            problems[fqn] = "nonzero violations"
    return problems


def contains(found, expected):
    if isinstance(expected, dict):
        return isinstance(found, dict) and all(
            k in found and contains(found[k], value) for k, value in expected.items()
        )
    if isinstance(expected, list):
        return (
            isinstance(found, list)
            and len(found) == len(expected)
            and all(contains(a, b) for a, b in zip(found, expected))
        )
    return found == expected


def ids(values):
    return {value["id"] for value in values or []}


def governance_gaps(published, hashes, fetch=get):
    from data_platform.governance import DOMAIN, PIPELINE, PRODUCT, TASKS

    gaps = []
    owners = ids(published["owners"])
    source_product = published["source_product"]
    domain = fetch(f"domains/name/{DOMAIN}")
    products = {
        name: fetch(f"dataProducts/name/{name}", {"fields": "owners,domains"}) for name in (PRODUCT, source_product)
    }
    for name, product in products.items():
        if ids(product.get("owners")) != owners or domain["id"] not in ids(product.get("domains")):
            gaps.append(f"product:{name}:ownership-domain")
    sources, tables = published["sources"], published["tables"]
    for product, relation, expected_ids in (
        (PRODUCT, "assets", ids(list(tables.values())) | {published["pipeline"]["id"]}),
        (PRODUCT, "inputPorts", ids(list(sources.values()))),
        (PRODUCT, "outputPorts", ids(list(tables.values()))),
        (source_product, "assets", ids(list(sources.values()))),
        (source_product, "outputPorts", ids(list(sources.values()))),
    ):
        found = fetch(f"dataProducts/name/{product}/{relation}", {"limit": PAGE})
        if not expected_ids <= ids(found.get("data")):
            gaps.append(f"product:{product}:{relation}")
    for table, identity in tables.items():
        fqn = urllib.parse.quote(entity(table), safe="")
        found = fetch(f"tables/name/{fqn}", {"fields": "owners,dataProducts,domains"})
        if (
            found["id"] != identity["id"]
            or ids(found.get("owners")) != owners
            or products[PRODUCT]["id"] not in ids(found.get("dataProducts"))
            or domain["id"] not in ids(found.get("domains"))
        ):
            gaps.append(f"table:{table}:membership")
        contract = fetch(
            "dataContracts/entity", {"entityId": identity["id"], "entityType": "table", "fields": "owners"}
        )
        expected = published["contracts"][table]
        if (
            hashes[table] not in contract.get("description", "")
            or not contains(
                contract,
                {
                    **{k: v for k, v in expected["body"].items() if k != "qualityExpectations"},
                    "termsOfUse": {"content": expected["body"]["termsOfUse"]},
                },
            )
            or ids(contract.get("qualityExpectations")) != ids(expected["body"]["qualityExpectations"])
            or contract["id"] != expected["id"]
        ):
            gaps.append(f"contract:{table}")
        edges = fetch(f"lineage/table/name/{fqn}", {"upstreamDepth": 1, "downstreamDepth": 0}).get("upstreamEdges", [])
        if any(e["fromEntity"] in ids(list(tables.values())) and e["toEntity"] == identity["id"] for e in edges):
            gaps.append(f"lineage:{table}:fabricated-table-dependency")
        if table in sources:
            source = sources[table]
            asset = fetch(f"containers/{source['id']}", {"fields": "owners,dataProducts,domains"})
            if (
                asset.get("fullPath") != source["path"]
                or ids(asset.get("owners")) != owners
                or products[source_product]["id"] not in ids(asset.get("dataProducts"))
                or domain["id"] not in ids(asset.get("domains"))
            ):
                gaps.append(f"source:{table}")
            if not any(
                e["fromEntity"] == source["id"]
                and e["toEntity"] == identity["id"]
                and (e.get("lineageDetails") or {}).get("pipeline", {}).get("id") == published["pipeline"]["id"]
                for e in edges
            ):
                gaps.append(f"lineage:{table}:source-pipeline-output")
    pipeline = fetch(f"pipelines/name/{PIPELINE}", {"fields": "owners,dataProducts,domains,tasks,pipelineStatus"})
    if (
        pipeline["id"] != published["pipeline"]["id"]
        or ids(pipeline.get("owners")) != owners
        or products[PRODUCT]["id"] not in ids(pipeline.get("dataProducts"))
        or domain["id"] not in ids(pipeline.get("domains"))
    ):
        gaps.append("pipeline:membership")
    tasks = {t["name"]: t for t in pipeline.get("tasks", [])}
    if set(tasks) != set(TASKS) or any(
        set(tasks[name].get("downstreamTasks") or []) != set(children) or not tasks[name].get("sourceUrl")
        for name, children in TASKS.items()
        if name in tasks
    ):
        gaps.append("pipeline:tasks")
    execution = pipeline.get("pipelineStatus") or {}
    expected_execution = published["status"]
    if (
        not contains(
            {
                **execution,
                "taskStatus": sorted(execution.get("taskStatus", []), key=lambda task: task.get("name", "")),
            },
            {
                **expected_execution,
                "taskStatus": sorted(expected_execution["taskStatus"], key=lambda task: task["name"]),
            },
        )
        or pipeline.get("sourceUrl") != published["url"]
    ):
        gaps.append("pipeline:execution")
    profile = published["profile"]
    if (
        not published["profile_required"]
        and profile.get("outcome") == "skipped"
        and profile.get("reason") == "recurring-disabled"
    ):
        return gaps
    if profile.get("outcome") != "passed" or set(profile.get("coverage", {})) != set(tables):
        gaps.append("profile:coverage")
    else:
        for table, coverage in profile["coverage"].items():
            found = fetch(f"tables/{urllib.parse.quote(entity(table), safe='')}/tableProfile/latest")
            values = {c["name"]: c.get("profile") or {} for c in found.get("columns", [])}
            if (
                coverage["scope"] != "latest-state-bounded-query"
                or coverage["max_rows"] < 1
                or (found.get("profile") or {}).get("timestamp", 0) < profile["started_at"]
                or any(
                    values.get(name, {}).get("nullCount") is None or values.get(name, {}).get("distinctCount") is None
                    for name in coverage["columns"]
                )
            ):
                gaps.append(f"profile:{table}:statistics")
    return gaps
