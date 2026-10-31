import json
import math
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from pyspark.sql import functions as F

from batch.gold import features as gold
from batch.gold.windows import ANCHOR
from tests.events import at

EXAMPLES = json.loads(
    (Path(__file__).resolve().parents[5] / "contracts/features/fraud-history-batch-v1.examples.json").read_text()
)
VIEWS = {"customer": gold.CUSTOMER, "payment_method": gold.METHOD}
ATTEMPT_COLUMNS = (
    "integration_id string, payment_id string, attempt_id string, customer_id string, "
    "payment_method_fingerprint string, merchant_id string, device_fingerprint string, amount_minor bigint, "
    "currency string, started_at timestamp, attempted_at timestamp, outcome string, outcome_at timestamp, "
    "decline_source string, lifecycle_valid boolean"
)
PROFILE_COLUMNS = (
    "integration_id string, payment_id string, attempt_id string, customer_id string, device_fingerprint string, "
    "started_at timestamp, external_buyer_id string, customer_account_created_at timestamp, shipping_country string, "
    "shipping_region string, shipping_postal_code string, shipping_address_fingerprint string"
)
LABEL_COLUMNS = (
    "observation_id string, integration_id string, payment_id string, attempt_id string, is_fraud boolean, "
    "occurred_at timestamp, label_available_at timestamp, source string, linkage string"
)


def instant(value):
    return None if value is None else datetime.fromisoformat(value).astimezone(timezone.utc)


def micros(value):
    return (instant(value) - datetime(1970, 1, 1, tzinfo=timezone.utc)) // timedelta(microseconds=1)


def example_attempts(case):
    rows = []
    history = case.get("history", EXAMPLES["defaults"]["history"])
    for index, override in enumerate([*history, *case["attempts"]]):
        values = {**EXAMPLES["defaults"]["attempt"], **override}
        values.setdefault("attempted_at", values["started_at"])
        rows.append(
            (
                values["integration_id"],
                f"p-{index}",
                "a",
                values["customer_id"],
                values["payment_method_fingerprint"],
                values["merchant_id"],
                values["device_fingerprint"],
                values["amount_minor"],
                values["currency"],
                instant(values["started_at"]),
                instant(values["attempted_at"]),
                values["outcome"],
                instant(values["outcome_at"]),
                values["decline_source"],
                True,
            )
        )
    return rows


def valid(spark, rows):
    return gold.with_attempt_time(spark.createDataFrame(rows, ATTEMPT_COLUMNS)).where(F.col("lifecycle_valid"))


@pytest.mark.parametrize("case", EXAMPLES["cases"], ids=[case["name"] for case in EXAMPLES["cases"]])
def test_the_feature_definition_examples(spark, case):
    view = VIEWS[case.get("view", EXAMPLES["defaults"]["view"])]
    entity = case.get("entity", EXAMPLES["defaults"]["entity"])
    attempts = valid(spark, example_attempts(case))
    start = gold.history_start(attempts)
    cutoff = gold.midnight(instant(case.get("cutoff", EXAMPLES["defaults"]["cutoff"])).date())

    rows = gold.daily_features(view, attempts, start, start - gold.DAY, cutoff, cutoff)

    key = view.keys[1]
    selected = rows.where(
        (F.col("integration_id") == entity["integration_id"]) & (F.col(key) == entity[key])
    ).withColumn("_micros", F.unix_micros(ANCHOR))
    actual = sorted((row.asDict() for row in selected.collect()), key=lambda row: row["_micros"])
    expected = case["expected"]
    assert [row["_micros"] for row in actual] == [micros(row["event_timestamp"]) for row in expected]
    for got, want in zip(actual, expected):
        for column, value in want["values"].items():
            if value is None or got[column] is None:
                assert got[column] == value, f"{case['name']} {want['event_timestamp']} {column}"
            else:
                assert math.isclose(got[column], value), f"{case['name']} {want['event_timestamp']} {column}"


def attempt(payment, customer, card, device, started, attempted=None, merchant="m", outcome="succeeded", lag=1):
    return (
        "i",
        payment,
        "a",
        customer,
        card,
        merchant,
        device,
        100,
        "USD",
        at(started),
        None if attempted is None else at(attempted),
        outcome,
        at(started + lag),
        None,
        True,
    )


def history(days=50):
    """Two customers sharing a card, with gaps long enough for the 7- and 30-day windows to empty."""
    rows = []
    for day in range(days):
        if day % 9 < 4:
            rows.append(attempt(f"c-{day}", "c", "card-1", f"d-{day % 3}", day * 1440 + 60, merchant=f"m-{day % 2}"))
        if day % 13 == 0:
            rows.append(attempt(f"k-{day}", "k", "card-1", "d-k", day * 1440 + 600, outcome="declined"))
    return rows


def test_a_range_run_writes_exactly_the_full_run_rows_of_its_days(spark):
    attempts = valid(spark, history())
    start = gold.history_start(attempts)
    cutoff = start + 50 * gold.DAY
    for view in gold.VIEWS:
        full = gold.daily_features(view, attempts, start, start - gold.DAY, cutoff, cutoff)
        floor = start + 17 * gold.DAY
        upper = start + 41 * gold.DAY
        expected = full.where((F.col(ANCHOR) > F.lit(floor)) & (F.col(ANCHOR) <= F.lit(upper)))
        ranged = gold.daily_features(view, attempts, start, floor, upper, cutoff)
        assert ranged.count() > 0
        assert ranged.exceptAll(expected).count() == 0, view.table
        assert expected.exceptAll(ranged).count() == 0, view.table


def test_attempt_snapshots_count_earlier_starts_in_each_window_at_the_attempt_start(spark):
    rows = [
        attempt("p-0", "c", "card", "d-1", 2000),
        attempt("p-1", "c", "card", "d-2", 2002),
        attempt("p-2", "c", "card-2", "d-2", 2030, attempted=2001),
        attempt("p-3", "c", "card", None, 2040),
        attempt("p-4", "c", "card", "d-9", 2041, attempted=500),
        attempt("p-5", "c", None, "d-3", 3500),
    ]
    attempts = gold.with_attempt_time(spark.createDataFrame(rows, ATTEMPT_COLUMNS))
    start = gold.history_start(attempts)
    floor = gold.midnight(at(1440).date())
    computed = gold.attempt_snapshots(attempts, start, floor, floor + 2 * gold.DAY, floor)
    snapshots = {row.payment_id: row for row in computed.withColumn("_at", F.unix_micros(ANCHOR)).collect()}

    assert set(snapshots) == {"p-0", "p-1", "p-2", "p-3", "p-4", "p-5"}
    customer = {
        payment: (
            row.customer_snapshot_attempt_count_5m,
            row.customer_snapshot_attempt_count_1h,
            row.customer_snapshot_attempt_count_24h,
            row.customer_snapshot_distinct_devices_24h,
        )
        for payment, row in snapshots.items()
    }
    assert customer["p-0"] == (0, 0, None, None)
    assert customer["p-1"] == (1, 1, None, None)
    # p-2 was attempted at 2001 but started at 2030, so it counts from 2030 at its attempt time.
    assert customer["p-3"][:2] == (0, 3)
    # p-4 was attempted more than 24 hours before it started: too late to count anywhere.
    assert customer["p-5"][:2] == (0, 0)
    assert snapshots["p-3"].method_snapshot_attempt_count_1h == 2
    assert snapshots["p-5"].method_snapshot_attempt_count_1h is None
    assert snapshots["p-0"]._at == micros(at(2000).isoformat())


def test_attempt_snapshots_of_a_range_equal_the_full_run(spark):
    attempts = gold.with_attempt_time(spark.createDataFrame(history(20), ATTEMPT_COLUMNS))
    start = gold.history_start(attempts)
    full = gold.attempt_snapshots(attempts, start, start, start + 20 * gold.DAY, start)
    floor, upper = start + 6 * gold.DAY, start + 11 * gold.DAY
    expected = full.where((F.col(ANCHOR) >= F.lit(floor)) & (F.col(ANCHOR) < F.lit(upper)))
    ranged = gold.attempt_snapshots(attempts, start, floor, upper, start)
    assert ranged.count() > 0
    assert ranged.exceptAll(expected).count() == 0
    assert expected.exceptAll(ranged).count() == 0


def profile_row(attempt, day, device, address="A", customer="c"):
    return (
        "i",
        f"p-{attempt}",
        "a",
        customer,
        device,
        at(day * 24 * 60),
        "buyer",
        at(0),
        "DE",
        "BE",
        "10115",
        address,
    )


def test_customer_versions_follow_address_and_familiar_device_changes(spark):
    rows = [
        profile_row(0, 0, "X"),
        profile_row(1, 1, "X"),
        profile_row(2, 2, "X", address="B"),
        profile_row(3, 3, "X"),
        profile_row(4, 4, "Y"),
        profile_row(5, 5, "Z"),
        profile_row(6, 6, "Z"),
        profile_row(7, 95, "Z"),
        profile_row(8, 0, None, customer="same"),
        profile_row(9, 1, None, customer="same"),
    ]
    versions = gold.dim_customer(spark.createDataFrame(rows, PROFILE_COLUMNS))

    customer = sorted(
        (row.asDict() for row in versions.where(F.col("customer_id") == "c").collect()),
        key=lambda row: row["version_number"],
    )
    assert [(row["source_payment_id"], row["shipping_address_fingerprint"]) for row in customer] == [
        ("p-0", "A"),
        ("p-1", "A"),
        ("p-2", "B"),
        ("p-3", "A"),
        ("p-6", "A"),
        ("p-7", "A"),
    ]
    assert [row["familiar_device_fingerprints"] for row in customer] == [[], ["X"], ["X"], ["X"], ["X", "Z"], ["Z"]]
    for earlier, later in zip(customer, customer[1:]):
        assert earlier["valid_to_ts"] == later["valid_from_ts"]
        assert not earlier["is_current"]
    assert customer[-1]["valid_to_ts"] is None and customer[-1]["is_current"]
    assert len({row["customer_sk"] for row in customer}) == len(customer)

    unchanged = versions.where(F.col("customer_id") == "same").collect()
    assert [row["source_payment_id"] for row in unchanged] == ["p-8"]


def test_attempt_labels_keep_the_earliest_linked_observation_per_source(spark):
    attempts = valid(
        spark,
        [
            ("i", "p-1", "a", "c", "card", "m", "d", 100, "USD", at(0), None, "succeeded", at(1), None, True),
            ("i", "p-2", "a", "c", "card", "m", "d", 100, "USD", at(5), at(4), "pending", None, None, True),
        ],
    )
    truth, chargeback = "LABEL_SOURCE_SIMULATION_TRUTH", "LABEL_SOURCE_CHARGEBACK"
    labels = spark.createDataFrame(
        [
            ("o-2", "i", "p-1", "a", True, at(0), at(20), truth, "linked"),
            ("o-1", "i", "p-1", "a", False, at(0), at(10), truth, "linked"),
            ("o-3", "i", "p-1", "a", True, at(0), at(30), chargeback, "linked"),
            ("o-4", "i", "p-2", "a", True, at(5), at(6), truth, "pending"),
        ],
        LABEL_COLUMNS,
    )

    rows = {
        (row.payment_id, row.label_source): row
        for row in gold.label_attempt(labels, attempts).withColumn("_at", F.unix_micros(ANCHOR)).collect()
    }

    assert set(rows) == {("p-1", truth), ("p-1", chargeback)}
    assert rows[("p-1", truth)].label is False
    assert rows[("p-1", chargeback)].label is True
    assert rows[("p-1", truth)]._at == micros(at(0).isoformat())
