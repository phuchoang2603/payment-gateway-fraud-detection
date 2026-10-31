"""DP3 marts: ClickHouse rebuilds the start days of a DP3 run from the lakehouse into staging tables, the checks audit
staging, and publish replaces those days' partitions in the live tables. A rebuilt day depends only on Silver, so a
re-run changes nothing and a backfill is a longer range.
"""

import base64
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from batch.store import Store

DATABASE = "marts"
FACT = "fact_payment_attempt"
MERCHANT_DAY = "fact_merchant_day"
BUILD_INFO = "build_info"
LABELS = "attempt_labels"
LIVE = (FACT, MERCHANT_DAY, BUILD_INFO)
STAGING = "__staging"
SILVER_ATTEMPTS = "lakehouse.`silver.attempts`"
SILVER_LABELS = "lakehouse.`silver.labels`"
TRUTH = "LABEL_SOURCE_SIMULATION_TRUTH"
CHARGEBACK = "LABEL_SOURCE_CHARGEBACK"
CHARGEBACK_DAYS = 21
DDL = Path(__file__).with_name("sql") / "marts"
# The settings that let full-history joins finish on the 4 GiB prod server: disk-backed sort-merge joins and
# aggregation instead of in-memory hash tables.
SPILL = {
    "join_algorithm": "full_sorting_merge",
    "max_bytes_before_external_sort": "400000000",
    "max_bytes_before_external_group_by": "800000000",
    "max_threads": "2",
    "max_insert_threads": "2",
}
FACT_COLUMNS = [
    "integration_id",
    "payment_id",
    "attempt_id",
    "merchant_id",
    "customer_id",
    "payment_method_fingerprint",
    "payment_method_type",
    "device_fingerprint",
    "amount_minor",
    "currency",
    "started_at",
    "attempted_at",
    "risk_outcome",
    "risk_score",
    "policy_version",
    "model_version",
    "outcome",
    "decline_source",
    "outcome_at",
    "payment_status",
]


class ClickHouse:
    def __init__(self, url, user, password):
        self.url = url.rstrip("/") + "/"
        self.auth = "Basic " + base64.b64encode(f"{user}:{password}".encode()).decode()
        self.queries = []

    def execute(self, sql: str, settings: dict | None = None, label: str | None = None) -> str:
        query_id = f"{label or 'marts'}-{time.time_ns()}"
        params = {"query_id": query_id, **(settings or {})}
        request = urllib.request.Request(
            f"{self.url}?{urllib.parse.urlencode(params)}",
            data=sql.encode(),
            method="POST",
            headers={"Authorization": self.auth},
        )
        try:
            with urllib.request.urlopen(
                request, timeout=int(os.environ.get("MARTS_QUERY_TIMEOUT_SECONDS", "7200"))
            ) as response:
                response.read()
        except urllib.error.HTTPError as error:
            message = error.read().decode(errors="replace").strip()
            raise RuntimeError(f"ClickHouse rejected {label or 'a query'}: {message}") from None
        if label:
            self.queries.append((label, query_id))
        return query_id

    def rows(self, sql: str, settings: dict | None = None) -> list[dict]:
        params = {"default_format": "JSONEachRow", **(settings or {})}
        request = urllib.request.Request(
            f"{self.url}?{urllib.parse.urlencode(params)}",
            data=sql.encode(),
            method="POST",
            headers={"Authorization": self.auth},
        )
        try:
            with urllib.request.urlopen(request, timeout=600) as response:
                body = response.read().decode()
        except urllib.error.HTTPError as error:
            message = error.read().decode(errors="replace").strip()
            raise RuntimeError(f"ClickHouse rejected a read: {message}") from None
        return [json.loads(line) for line in body.splitlines() if line]

    def value(self, sql: str, settings: dict | None = None):
        rows = self.rows(sql, settings)
        return next(iter(rows[0].values())) if rows else None


def connect() -> ClickHouse:
    return ClickHouse(
        os.environ["CLICKHOUSE_URL"], os.environ.get("CLICKHOUSE_USER", "marts"), os.environ["CLICKHOUSE_PASSWORD"]
    )


def name(table: str, staging: bool = False) -> str:
    return f"{DATABASE}.{table}{STAGING if staging else ''}"


def ddl(table: str, staging: bool, retention_days: int) -> str:
    text = (DDL / f"{table}.sql").read_text()
    return text.format(
        if_not_exists="" if staging else "IF NOT EXISTS ",
        table=name(table, staging),
        retention_days=retention_days,
    )


def pinned(table: str, snapshot: int, columns: str, where: str = "1") -> str:
    """A subquery reading one Iceberg table at a snapshot; each table carries its own pin."""
    return f"(SELECT {columns} FROM {table} WHERE {where} SETTINGS iceberg_snapshot_id = {int(snapshot)})"


def day_list(days) -> str:
    return ", ".join(f"toDate('{day}')" for day in days)


def months_of(days) -> list[str]:
    return sorted({day.strftime("%Y%m") for day in days})


def window(retention_days: int, today: date) -> date:
    return today - timedelta(days=retention_days - 1)


def drop_staging(client: ClickHouse) -> None:
    for table in (*LIVE, LABELS):
        client.execute(f"DROP TABLE IF EXISTS {name(table, staging=True)}")


def rebuilt_days(gold_report: dict, start: date, last: date) -> list[date]:
    """The run's days, plus the days whose chargeback window completes within it, inside [start, last]."""
    first, end = date.fromisoformat(gold_report["start"]), date.fromisoformat(gold_report["end"])
    maturing = [first + timedelta(days=n - CHARGEBACK_DAYS + 1) for n in range((end - first).days + 1)]
    days = {*map(date.fromisoformat, gold_report["days"]), *maturing}
    return sorted(day for day in days if start <= day <= last)


def build(client: ClickHouse, gold_report: dict, run_id: str, retention_days: int, today: date) -> dict:
    snapshots = gold_report["silver_snapshots"]
    attempts_snapshot = snapshots["silver.attempts"]
    labels_snapshot = snapshots["silver.labels"]
    complete_before = date.fromisoformat(gold_report["end"]) + timedelta(days=1)
    started = time.perf_counter()
    drop_staging(client)
    for table in LIVE:
        client.execute(ddl(table, staging=False, retention_days=retention_days))
    for table in (*LIVE, LABELS):
        client.execute(ddl(table, staging=True, retention_days=retention_days))

    start = window(retention_days, today)
    last = client.value(
        f"SELECT toString(max(toDate(started_at))) FROM "
        f"{pinned(SILVER_ATTEMPTS, attempts_snapshot, 'started_at', 'lifecycle_valid')} AS a"
    )
    days = rebuilt_days(gold_report, start, date.fromisoformat(last)) if last else []
    months = months_of(days)

    if days:
        client.execute(
            f"""INSERT INTO {name(LABELS, staging=True)}
SELECT integration_id, payment_id, attempt_id,
  argMinIf(is_fraud, (label_available_at, observation_id), source = '{TRUTH}'),
  argMinIf(scenario, (label_available_at, observation_id), source = '{TRUTH}'),
  minIf(label_available_at, source = '{CHARGEBACK}')
FROM {pinned(SILVER_LABELS, labels_snapshot, "*", f"linkage = 'linked' AND label_available_at >= toDate('{days[0]}')")}
  AS l
GROUP BY integration_id, payment_id, attempt_id""",
            SPILL,
            "labels",
        )
    by_month = {}
    for day in days:
        by_month.setdefault(day.strftime("%Y%m"), []).append(day)
    for month, month_days in sorted(by_month.items()):
        first, after = month_days[0], month_days[-1] + timedelta(days=1)
        where = (
            f"lifecycle_valid AND started_at >= toDate('{first}') AND started_at < toDate('{after}') "
            f"AND toDate(started_at) IN ({day_list(month_days)})"
        )
        client.execute(
            f"""INSERT INTO {name(FACT, staging=True)}
SELECT {", ".join(f"a.{column}" for column in FACT_COLUMNS)},
  l.truth_is_fraud, l.truth_scenario, l.chargeback_available_at
FROM {pinned(SILVER_ATTEMPTS, attempts_snapshot, ", ".join(FACT_COLUMNS), where)} AS a
LEFT JOIN {name(LABELS, staging=True)} AS l USING (integration_id, payment_id, attempt_id)""",
            {**SPILL, "join_use_nulls": "1"},
            f"fact-{month}",
        )

    if months:
        # A merchant-day partition is a month, so the live days of the touched months are regrouped with the staged ones.
        source = (
            f"(SELECT * FROM {name(FACT)} WHERE toYYYYMM(started_at) IN ({', '.join(months)}) "
            f"AND toDate(started_at) >= toDate('{start}') AND toDate(started_at) NOT IN ({day_list(days)}) "
            f"UNION ALL SELECT * FROM {name(FACT, staging=True)})"
        )
        client.execute(
            f"""INSERT INTO {name(MERCHANT_DAY, staging=True)}
SELECT integration_id, merchant_id, day, attempts, decided, succeeded, risk_declined, processor_declined,
  amount_attempted_minor, amount_succeeded_minor, distinct_customers, truth_labelled, truth_fraud, chargebacks,
  if(decided = 0, NULL, succeeded / decided),
  if(decided = 0, NULL, risk_declined / decided),
  if(truth_labelled = 0, NULL, truth_fraud / truth_labelled),
  if(succeeded = 0, NULL, chargebacks / succeeded),
  day + {CHARGEBACK_DAYS} <= toDate('{complete_before}')
FROM (
  SELECT integration_id, merchant_id, toDate(started_at) AS day,
    count() AS attempts,
    countIf(outcome IN ('succeeded', 'declined')) AS decided,
    countIf(outcome = 'succeeded') AS succeeded,
    countIf(outcome = 'declined' AND decline_source = 'DECLINE_SOURCE_RISK') AS risk_declined,
    countIf(outcome = 'declined' AND coalesce(decline_source, '') != 'DECLINE_SOURCE_RISK') AS processor_declined,
    sum(amount_minor) AS amount_attempted_minor,
    sumIf(amount_minor, outcome = 'succeeded') AS amount_succeeded_minor,
    uniqExact(customer_id) AS distinct_customers,
    countIf(truth_is_fraud IS NOT NULL) AS truth_labelled,
    countIf(truth_is_fraud) AS truth_fraud,
    countIf(chargeback_available_at IS NOT NULL) AS chargebacks
  FROM {source} AS f
  WHERE toYYYYMM(started_at) IN ({", ".join(months)})
  GROUP BY integration_id, merchant_id, day
)""",
            SPILL,
            "merchant-day",
        )
    client.execute(
        f"""INSERT INTO {name(BUILD_INFO, staging=True)} VALUES (
'{run_id}', toDate('{start}'), toDate('{last or start}'), [{day_list(days)}], toDate('{complete_before}'),
{int(attempts_snapshot)}, {int(labels_snapshot)}, {int(snapshots["silver.payment_events"])}, now64(6))"""
    )
    client.execute(f"DROP TABLE IF EXISTS {name(LABELS, staging=True)}")

    staged = int(client.value(f"SELECT count() FROM {name(FACT, staging=True)}"))
    expected = 0
    if days:
        expected = int(
            client.value(
                "SELECT count() FROM "
                + pinned(
                    SILVER_ATTEMPTS,
                    attempts_snapshot,
                    "started_at",
                    f"lifecycle_valid AND toDate(started_at) IN ({day_list(days)})",
                )
                + " AS a"
            )
        )
    if staged != expected:
        raise RuntimeError(f"staged {staged} attempts but Silver holds {expected} on the rebuilt days")
    return {
        "run_id": run_id,
        "window_start": str(start),
        "window_end": last,
        "rebuilt_days": len(days),
        "months": months,
        "staged_attempts": staged,
        "seconds": round(time.perf_counter() - started, 2),
        "queries": query_metrics(client),
    }


def query_metrics(client: ClickHouse) -> dict:
    if not client.queries:
        return {}
    client.execute("SYSTEM FLUSH LOGS")
    ids = ", ".join(f"'{query_id}'" for _, query_id in client.queries)
    rows = client.rows(
        "SELECT query_id, query_duration_ms, read_rows, read_bytes, written_rows, memory_usage "
        f"FROM system.query_log WHERE type = 'QueryFinish' AND query_id IN ({ids})"
    )
    by_id = {row["query_id"]: row for row in rows}
    return {label: by_id.get(query_id) for label, query_id in client.queries}


def publish(client: ClickHouse, retention_days: int, today: date) -> dict:
    info = client.rows(f"SELECT rebuilt_days FROM {name(BUILD_INFO, staging=True)}")
    if len(info) != 1:
        raise RuntimeError("no staged marts build to publish")
    start = window(retention_days, today)
    days = [date.fromisoformat(day) for day in info[0]["rebuilt_days"]]
    published = {FACT: replace_partitions(client, FACT, [str(day) for day in days], "toDate(started_at)")}
    staged_months = client.rows(
        f"SELECT DISTINCT toString(toYYYYMM(day)) AS month FROM {name(MERCHANT_DAY, staging=True)}"
    )
    months = sorted({*months_of(days), *(row["month"] for row in staged_months)})
    published[MERCHANT_DAY] = replace_partitions(client, MERCHANT_DAY, months, "toYYYYMM(day)")
    expired = [
        row["day"]
        for row in client.rows(
            f"SELECT DISTINCT toString(toDate(started_at)) AS day FROM {name(FACT)} WHERE started_at < toDate('{start}')"
        )
    ]
    for day in expired:
        client.execute(f"ALTER TABLE {name(FACT)} DROP PARTITION '{day}'")
    client.execute(f"DELETE FROM {name(MERCHANT_DAY)} WHERE day < toDate('{start}')")
    client.execute(f"EXCHANGE TABLES {name(BUILD_INFO)} AND {name(BUILD_INFO, staging=True)}")
    published[BUILD_INFO] = "exchanged"
    published["expired_days"] = len(expired)
    drop_staging(client)
    return {"published": published}


def replace_partitions(client: ClickHouse, table: str, partitions: list[str], expression: str) -> dict:
    """Replace each named partition of the live table with staging's, or drop it when staging has none."""
    present = {
        row["partition"]
        for row in client.rows(f"SELECT DISTINCT toString({expression}) AS partition FROM {name(table, staging=True)}")
    }
    replaced = dropped = 0
    for partition in sorted(set(partitions)):
        literal = f"'{partition}'" if "-" in partition else partition
        if partition in present:
            client.execute(f"ALTER TABLE {name(table)} REPLACE PARTITION {literal} FROM {name(table, staging=True)}")
            replaced += 1
        else:
            client.execute(f"ALTER TABLE {name(table)} DROP PARTITION {literal}")
            dropped += 1
    return {"replaced": replaced, "dropped": dropped}


def main():
    command = sys.argv[1] if len(sys.argv) > 1 else ""
    if command not in ("build", "publish"):
        raise SystemExit("usage: python -m batch.marts build|publish")
    run_id = os.environ["RUN_ID"]
    retention_days = int(os.environ.get("MARTS_RETENTION_DAYS", "180"))
    today = datetime.now(timezone.utc).date()
    client = connect()
    store = Store(os.environ["RUN_REPORT_ROOT"])
    if command == "build":
        report = build(client, store.read_json(f"{run_id}/gold.json"), run_id, retention_days, today)
    else:
        report = publish(client, retention_days, today)
    store.write_json(f"{run_id}/marts-{command}.json", report)
    print(json.dumps(report, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
