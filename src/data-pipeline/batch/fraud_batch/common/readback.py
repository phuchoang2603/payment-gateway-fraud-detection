PAGE = 1000


def get(path, query=None):
    from fraud_batch.common.catalog import client

    return client().client.get(f"/{path}", query)


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
