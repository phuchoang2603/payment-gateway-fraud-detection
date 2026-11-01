from datetime import date, datetime, timedelta, timezone

import pytest
from hypothesis import given
from hypothesis import strategies as st

from batch.gold import marts

TODAY = datetime.now(timezone.utc).date()
RETENTION = 180
ATTEMPTS_DDL = """CREATE TABLE lakehouse.`silver.attempts` (
  integration_id Nullable(String), payment_id Nullable(String), attempt_id Nullable(String),
  merchant_id Nullable(String), customer_id Nullable(String), payment_method_fingerprint Nullable(String),
  payment_method_type Nullable(String), device_fingerprint Nullable(String), amount_minor Nullable(Int64),
  currency Nullable(String), started_at Nullable(DateTime64(6, 'UTC')), attempted_at Nullable(DateTime64(6, 'UTC')),
  risk_outcome Nullable(String), risk_score Nullable(Float64), policy_version Nullable(String),
  model_version Nullable(String), outcome Nullable(String), decline_source Nullable(String),
  outcome_at Nullable(DateTime64(6, 'UTC')), payment_status Nullable(String), lifecycle_valid Nullable(Bool)
) ENGINE = MergeTree ORDER BY tuple()"""
LABELS_DDL = """CREATE TABLE lakehouse.`silver.labels` (
  observation_id Nullable(String), integration_id Nullable(String), payment_id Nullable(String),
  attempt_id Nullable(String), is_fraud Nullable(Bool), occurred_at Nullable(DateTime64(6, 'UTC')),
  label_available_at Nullable(DateTime64(6, 'UTC')), source Nullable(String), scenario Nullable(String),
  linkage Nullable(String)
) ENGINE = MergeTree ORDER BY tuple()"""


@pytest.fixture
def client(clickhouse):
    url = f"http://{clickhouse.get_container_host_ip()}:{clickhouse.get_exposed_port(8123)}/"
    client = marts.ClickHouse(url, "test", "test")
    for database in ("lakehouse", "marts"):
        client.execute(f"DROP DATABASE IF EXISTS {database} SYNC")
        client.execute(f"CREATE DATABASE {database}")
    client.execute(ATTEMPTS_DDL)
    client.execute(LABELS_DDL)
    return client


def at(day: date, hour=12) -> str:
    return f"{day} {hour:02d}:00:00"


def attempt(client, payment, day, merchant="m-1", customer="c-1", outcome="succeeded", source=None, hour=12):
    outcome_at = "NULL" if outcome == "pending" else f"'{at(day, hour + 1)}'"
    decline = "NULL" if source is None else f"'{source}'"
    client.execute(
        "INSERT INTO lakehouse.`silver.attempts` VALUES ("
        f"'i', '{payment}', 'a', '{merchant}', '{customer}', 'card', 'card', 'd', 100, 'USD', '{at(day, hour)}', NULL, "
        f"'RISK_OUTCOME_APPROVE', 0.5, 'policy-1', NULL, '{outcome}', {decline}, {outcome_at}, 'open', true)"
    )


def label(client, payment, day, source=marts.TRUTH, fraud=True, observation="o"):
    client.execute(
        "INSERT INTO lakehouse.`silver.labels` VALUES ("
        f"'{observation}-{payment}-{source}', 'i', '{payment}', 'a', {str(fraud).lower()}, '{at(day)}', "
        f"'{at(day, 14)}', '{source}', NULL, 'linked')"
    )


SNAPSHOTS = {"silver.attempts": 11, "silver.labels": 12, "silver.payment_events": 13}


def build(client, first=None, last=None, code="code-1"):
    """A build over [first, last], every day of the retention window by default."""
    first = first or TODAY - timedelta(days=RETENTION + 30)
    last = last or TODAY - timedelta(days=1)
    return marts.build(client, "run", first, last, SNAPSHOTS, code, RETENTION, TODAY)


def rebuild(client, first=None, last=None, code="code-1"):
    built = build(client, first, last, code)
    marts.publish(client, RETENTION, TODAY)
    return built


def content(client, table):
    return client.rows(f"SELECT * FROM marts.{table} ORDER BY ALL")


def test_rates_are_null_when_their_denominator_is_zero(client):
    day = TODAY - timedelta(days=3)
    attempt(client, "p-1", day, outcome="pending")
    attempt(client, "p-2", day, merchant="m-2", outcome="declined", source="DECLINE_SOURCE_RISK")

    rebuild(client)

    rows = {row["merchant_id"]: row for row in content(client, marts.MERCHANT_DAY)}
    assert rows["m-1"]["approval_rate"] is None and rows["m-1"]["attempts"] == 1
    assert rows["m-2"]["risk_decline_rate"] == 1 and rows["m-2"]["chargeback_rate"] is None


def test_the_build_keeps_the_rolling_window_and_the_ttl_drops_older_days(client):
    attempt(client, "old", TODAY - timedelta(days=RETENTION + 20))
    attempt(client, "new", TODAY - timedelta(days=10))
    label(client, "new", TODAY - timedelta(days=10))

    built = rebuild(client)

    assert [row["payment_id"] for row in content(client, marts.FACT)] == ["new"]
    assert content(client, marts.FACT)[0]["truth_is_fraud"] is True
    info = client.rows("SELECT toString(window_start) AS start FROM marts.build_info")
    assert info == [{"start": str(TODAY - timedelta(days=RETENTION - 1))}]
    assert built["staged_attempts"] == 1

    old = TODAY - timedelta(days=RETENTION + 5)
    client.execute(
        "INSERT INTO marts.fact_payment_attempt (integration_id, payment_id, attempt_id, merchant_id, customer_id, "
        "payment_method_fingerprint, payment_method_type, amount_minor, currency, started_at, outcome, payment_status) "
        f"VALUES ('i', 'expired', 'a', 'm', 'c', 'card', 'card', 1, 'USD', '{at(old)}', 'pending', 'open')"
    )
    client.execute("OPTIMIZE TABLE marts.fact_payment_attempt FINAL")
    assert client.value("SELECT count() FROM marts.fact_payment_attempt WHERE payment_id = 'expired'") == 0


def test_two_builds_on_the_same_silver_have_equal_content(client):
    attempt(client, "p-1", TODAY - timedelta(days=2))
    label(client, "p-1", TODAY - timedelta(days=2), source=marts.CHARGEBACK)

    rebuild(client)
    first = {table: content(client, table) for table in (marts.FACT, marts.MERCHANT_DAY)}
    rebuild(client)

    assert {table: content(client, table) for table in (marts.FACT, marts.MERCHANT_DAY)} == first


def test_a_failed_build_leaves_the_live_marts_unchanged(client):
    attempt(client, "p-1", TODAY - timedelta(days=2))
    rebuild(client)
    before = content(client, marts.FACT)
    client.execute("DROP TABLE lakehouse.`silver.labels`")
    attempt(client, "p-2", TODAY - timedelta(days=1))

    with pytest.raises(RuntimeError):
        build(client)

    assert content(client, marts.FACT) == before


def test_the_build_drops_leftover_staging(client):
    attempt(client, "p-1", TODAY - timedelta(days=2))
    rebuild(client)
    client.execute(marts.ddl(marts.FACT, staging=True, retention_days=RETENTION))
    client.execute(
        "INSERT INTO marts.fact_payment_attempt__staging SELECT * REPLACE ('stale' AS payment_id) "
        "FROM marts.fact_payment_attempt"
    )

    built = build(client)

    assert built["staged_attempts"] == 1
    assert client.value("SELECT count() FROM marts.fact_payment_attempt__staging WHERE payment_id = 'stale'") == 0


def test_a_range_rebuild_equals_a_full_rebuild(client):
    changed, untouched = TODAY - timedelta(days=20), TODAY - timedelta(days=40)
    attempt(client, "p-1", changed)
    attempt(client, "p-2", untouched)
    rebuild(client)
    attempt(client, "p-3", changed, merchant="m-2")
    label(client, "p-1", changed, source=marts.CHARGEBACK)

    rebuild(client, changed, changed)
    ranged = {table: content(client, table) for table in (marts.FACT, marts.MERCHANT_DAY)}
    rebuild(client)

    assert {table: content(client, table) for table in (marts.FACT, marts.MERCHANT_DAY)} == ranged


def test_a_range_rebuilds_its_days_and_the_label_lookback_before_them():
    days = marts.rebuilt_days(date(2026, 10, 30), date(2026, 10, 30), date(2026, 1, 1), date(2026, 10, 30))

    assert days[0] == date(2026, 9, 15) and days[-1] == date(2026, 10, 30)
    assert len(days) == marts.LABEL_LOOKBACK_DAYS + 1
    assert date(2026, 10, 10) in days


def test_the_window_bounds_the_rebuilt_days():
    assert marts.rebuilt_days(date(2026, 10, 30), date(2026, 10, 31), date(2026, 10, 1), date(2026, 10, 30)) == [
        date(2026, 10, 1) + timedelta(days=n) for n in range(30)
    ]


DATES = st.dates(date(2025, 1, 1), date(2027, 12, 31))


@given(run=st.tuples(DATES, DATES).map(sorted), window=st.tuples(DATES, DATES).map(sorted))
def test_the_rebuilt_days_are_the_run_and_its_lookback_clipped_to_the_window(run, window):
    (first, end), (start, last) = run, window
    days = marts.rebuilt_days(first, end, start, last)

    expected = {
        first - timedelta(days=marts.LABEL_LOOKBACK_DAYS) + timedelta(days=n)
        for n in range((end - first).days + marts.LABEL_LOOKBACK_DAYS + 1)
    }
    assert set(days) == {day for day in expected if start <= day <= last}
    assert days == sorted(set(days))


def test_the_marts_are_unchanged_only_for_the_same_range_snapshots_and_code(client):
    first, last = TODAY - timedelta(days=5), TODAY - timedelta(days=1)
    attempt(client, "p-1", TODAY - timedelta(days=2))
    rebuild(client, first, last, code="code-1")

    assert marts.unchanged(client, first, last, SNAPSHOTS, "code-1")
    assert not marts.unchanged(client, first, last, SNAPSHOTS, "code-2")
    assert not marts.unchanged(client, first, last, {**SNAPSHOTS, "silver.labels": 99}, "code-1")
    assert not marts.unchanged(client, first - timedelta(days=1), last, SNAPSHOTS, "code-1")
    assert not marts.unchanged(client, first, last, SNAPSHOTS, "")


def window_of(client):
    return client.rows("SELECT toString(window_start) AS start, toString(window_end) AS end FROM marts.build_info")[0]


def test_a_range_older_than_the_window_keeps_the_live_window_end(client):
    old = TODAY - timedelta(days=RETENTION + 40)
    attempt(client, "old", old)
    rebuild(client, old, old)
    empty = window_of(client)
    assert empty["end"] == str(date.fromisoformat(empty["start"]) - timedelta(days=1))

    attempt(client, "new", TODAY - timedelta(days=3))
    rebuild(client)
    rebuild(client, old, old)

    assert window_of(client)["end"] == str(TODAY - timedelta(days=3))
    assert [row["payment_id"] for row in content(client, marts.FACT)] == ["new"]
