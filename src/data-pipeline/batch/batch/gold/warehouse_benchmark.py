"""Warehouse indexing evidence: each fixed query against raw Iceberg, an unsorted MergeTree copy, the indexed mart, and
the mart with its skip indexes disabled, all over the same pinned data and window."""

import json
import os
import statistics
from datetime import date, datetime, timedelta, timezone

from batch.gold import marts
from batch.store import Store

SCRATCH = "marts_benchmark.fact_payment_attempt_unsorted"
WARM_RUNS = 5
REFERENCE = "C_indexed_mart"
COLD_CACHES = (
    "SYSTEM DROP MARK CACHE",
    "SYSTEM DROP UNCOMPRESSED CACHE",
    "SYSTEM DROP INDEX MARK CACHE",
    "SYSTEM DROP INDEX UNCOMPRESSED CACHE",
    "SYSTEM DROP FILESYSTEM CACHE",
)
QUERIES = {
    "Q0_last_day_by_hour": """SELECT toStartOfHour(started_at) AS hour, outcome, count() AS attempts FROM {table}
WHERE started_at >= toDate('{end}') AND started_at < toDate('{after_end}') GROUP BY hour, outcome""",
    "Q1_hot_merchant_30_days": """SELECT toDate(started_at) AS day, outcome, count() AS attempts, sum(amount_minor) AS amount
FROM {table} WHERE merchant_id = '{hot_merchant}' AND started_at >= toDate('{month_ago}')
  AND started_at < toDate('{after_end}') GROUP BY day, outcome""",
    "Q1_median_merchant_30_days": """SELECT toDate(started_at) AS day, outcome, count() AS attempts, sum(amount_minor) AS amount
FROM {table} WHERE merchant_id = '{median_merchant}' AND started_at >= toDate('{month_ago}')
  AND started_at < toDate('{after_end}') GROUP BY day, outcome""",
    "Q2_customer_30_days": """SELECT payment_id, attempt_id, started_at, outcome, amount_minor FROM {table}
WHERE customer_id = '{customer}' AND started_at >= toDate('{month_ago}') AND started_at < toDate('{after_end}')""",
    "Q3_card_across_customers": """SELECT customer_id, count() AS attempts FROM {table}
WHERE payment_method_fingerprint = '{card}' GROUP BY customer_id""",
    "Q4_approved_high_risk": """SELECT outcome, count() AS attempts, sum(amount_minor) AS amount FROM {table}
WHERE risk_score > 0.9 AND outcome IN ('succeeded', 'declined') GROUP BY outcome""",
}


class VariantsDisagree(RuntimeError):
    pass


def parameters(client: marts.ClickHouse) -> dict:
    info = client.rows(
        "SELECT toString(window_start) AS start, toString(window_end) AS end, silver_attempts_snapshot_id AS snapshot "
        f"FROM {marts.name(marts.BUILD_INFO)}"
    )
    if len(info) != 1:
        raise RuntimeError("the marts have no build_info; run dp3-gold first")
    start, end = date.fromisoformat(info[0]["start"]), date.fromisoformat(info[0]["end"])
    fact = marts.name(marts.FACT)
    merchants = [
        row["merchant_id"]
        for row in client.rows(
            f"SELECT merchant_id FROM {fact} GROUP BY merchant_id ORDER BY count() DESC, merchant_id"
        )
    ]
    if not merchants:
        raise RuntimeError("the marts are empty")
    customer = client.value(
        f"SELECT customer_id FROM {fact} GROUP BY customer_id ORDER BY count() DESC, customer_id LIMIT 1"
    )
    card = client.value(
        f"SELECT payment_method_fingerprint FROM {fact} GROUP BY payment_method_fingerprint "
        "ORDER BY uniqExact(customer_id) DESC, payment_method_fingerprint LIMIT 1"
    )
    return {
        "window_start": str(start),
        "window_end": str(end),
        "snapshot": int(info[0]["snapshot"]),
        "end": str(end),
        "after_end": str(end + timedelta(days=1)),
        "month_ago": str(end - timedelta(days=29)),
        "hot_merchant": merchants[0],
        "median_merchant": merchants[len(merchants) // 2],
        "customer": customer,
        "card": card,
    }


def variants(values: dict) -> dict:
    raw = marts.pinned(
        marts.SILVER_ATTEMPTS,
        values["snapshot"],
        "*",
        f"lifecycle_valid AND started_at >= toDate('{values['window_start']}') "
        f"AND started_at < toDate('{values['after_end']}')",
    )
    return {
        "A_raw_iceberg": (raw, {}),
        "B_unsorted_mergetree": (SCRATCH, {}),
        "C_indexed_mart": (marts.name(marts.FACT), {}),
        "C_primary_key_only": (marts.name(marts.FACT), {"use_skip_indexes": "0"}),
    }


def fingerprint(client, sql, settings) -> tuple:
    row = client.rows(
        f"SELECT toString(count()) AS rows, toString(sum(cityHash64(toString(tuple(*))))) AS checksum FROM ({sql})",
        settings,
    )[0]
    return row["rows"], row["checksum"]


def guard(fingerprints: dict) -> None:
    """Every variant of a query must return the indexed mart's rows; name the first that does not."""
    for query, by_variant in fingerprints.items():
        reference = by_variant[REFERENCE]
        for variant, value in by_variant.items():
            if value != reference:
                raise VariantsDisagree(f"{query}: {variant} returned {value}, {REFERENCE} returned {reference}")


def measure(client: marts.ClickHouse, label: str, sql: str, settings: dict) -> dict:
    plan = [row["explain"] for row in client.rows(f"EXPLAIN indexes = 1 {sql}", settings)]
    for statement in COLD_CACHES:
        client.execute(statement)
    runs = {"cold": client.execute(sql, {**settings, "use_query_cache": "0"}, f"{label}-cold")}
    warm = [client.execute(sql, {**settings, "use_query_cache": "0"}, f"{label}-warm-{n}") for n in range(WARM_RUNS)]
    client.execute("SYSTEM FLUSH LOGS")
    ids = ", ".join(f"'{query_id}'" for query_id in [runs["cold"], *warm])
    logged = {
        row["query_id"]: row
        for row in client.rows(
            "SELECT query_id, query_duration_ms, read_rows, read_bytes, memory_usage, "
            "ProfileEvents['SelectedMarks'] AS selected_marks, ProfileEvents['SelectedParts'] AS selected_parts "
            f"FROM system.query_log WHERE type = 'QueryFinish' AND query_id IN ({ids})"
        )
    }
    warm_logged = [logged[query_id] for query_id in warm if query_id in logged]
    return {
        "plan": plan,
        "cold": logged.get(runs["cold"]),
        "warm_median_ms": statistics.median(int(row["query_duration_ms"]) for row in warm_logged)
        if warm_logged
        else None,
        "warm": warm_logged,
    }


def run(client: marts.ClickHouse, run_id: str) -> dict:
    values = parameters(client)
    client.execute(f"DROP TABLE IF EXISTS {SCRATCH}")
    client.execute(
        f"CREATE TABLE {SCRATCH} ENGINE = MergeTree ORDER BY tuple() AS SELECT * FROM {marts.name(marts.FACT)}"
    )
    try:
        fingerprints, results = {}, {}
        for query, template in QUERIES.items():
            for variant, (table, settings) in variants(values).items():
                sql = template.format(table=table, **values)
                fingerprints.setdefault(query, {})[variant] = fingerprint(client, sql, settings)
        guard(fingerprints)
        for query, template in QUERIES.items():
            for variant, (table, settings) in variants(values).items():
                sql = template.format(table=table, **values)
                results.setdefault(query, {})[variant] = {
                    "fingerprint": fingerprints[query][variant],
                    **measure(client, f"bench-{query}-{variant}", sql, settings),
                }
    finally:
        client.execute(f"DROP TABLE IF EXISTS {SCRATCH}")
    return {
        "run_id": run_id,
        "recorded_at": datetime.now(timezone.utc).isoformat(),
        "parameters": values,
        "results": results,
    }


def main():
    run_id = os.environ["RUN_ID"]
    report = run(marts.connect(), run_id)
    Store(os.environ["RUN_REPORT_ROOT"]).write_json(f"{run_id}/report.json", report)
    print(json.dumps(report, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
