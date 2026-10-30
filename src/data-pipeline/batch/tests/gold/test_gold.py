import copy
import json
import math
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from pyspark.sql import functions as F

from batch.gold import features as gold
from batch.gold.windows import ANCHOR
from batch.silver import curate as silver
from tests.events import approved, at, created, label, started, succeeded
from tests.lake import load_bronze

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


@pytest.mark.parametrize("case", EXAMPLES["cases"], ids=[case["name"] for case in EXAMPLES["cases"]])
def test_the_feature_definition_examples(spark, case):
    view = VIEWS[case.get("view", EXAMPLES["defaults"]["view"])]
    entity = case.get("entity", EXAMPLES["defaults"]["entity"])
    attempts = gold.valid_attempts(spark.createDataFrame(example_attempts(case), ATTEMPT_COLUMNS))
    start = gold.history_start(attempts)
    cap = gold.midnight(instant(case.get("cutoff", EXAMPLES["defaults"]["cutoff"])))

    rows = gold.features(view, attempts, start, cap, cap)

    selected = rows.where(
        (F.col("integration_id") == entity["integration_id"]) & (F.col(view.key) == entity[view.key])
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
    attempts = gold.valid_attempts(
        spark.createDataFrame(
            [
                ("i", "p-1", "a", "c", "card", "m", "d", 100, "USD", at(0), None, "succeeded", at(1), None, True),
                ("i", "p-2", "a", "c", "card", "m", "d", 100, "USD", at(5), at(4), "pending", None, None, True),
            ],
            ATTEMPT_COLUMNS,
        )
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


def shipped_to(event, address):
    changed = copy.deepcopy(event)
    changed["attempt_started"]["context"]["snapshot"]["shipping"]["address_fingerprint"] = address
    return changed


def test_a_rerun_on_the_same_silver_writes_identical_gold(spark, report_root):
    for table in gold.TABLES:
        spark.sql(f"DROP TABLE IF EXISTS {table}")
    load_bronze(
        spark,
        [
            created("pay-1", "e-1"),
            started("pay-1", "e-2", 2, 1, "att-1"),
            approved("pay-1", "e-3", 3, 2, "att-1"),
            succeeded("pay-1", "e-4", 4, 3, "att-1"),
            created("pay-2", "f-1", minutes=3000),
            shipped_to(started("pay-2", "f-2", 2, 3001, "att-2"), "g"),
        ],
        [label("pay-1", "att-1", available=10)],
    )
    silver.run(spark, "silver")

    first = gold.run(spark, "gold-1")
    snapshots = {table: info["snapshot_id"] for table, info in first["tables"].items()}
    gold.run(spark, "gold-2")

    for table, snapshot in snapshots.items():
        before = spark.read.option("versionAsOf", snapshot).table(table)
        after = spark.table(table)
        assert before.exceptAll(after).count() == 0, table
        assert after.exceptAll(before).count() == 0, table
    assert spark.table(gold.DIM_CUSTOMER).count() == 2
    assert spark.table(gold.LABEL_ATTEMPT).count() == 1
    assert spark.table(gold.FEAT_CUSTOMER).count() > 0
