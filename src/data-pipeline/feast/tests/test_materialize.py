from datetime import UTC, date, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from fraud_feast import check_online, materialize
from fraud_feast.lake import read_json, write_json
from fraud_feast.push_job import write_online
from fraud_feast.repo import DAILY_FIELDS

DAY = datetime(2025, 9, 1, tzinfo=UTC)
FIELDS = DAILY_FIELDS["customer"]


def data(spark, rows):
    columns = (
        "integration_id string, customer_id string, event_timestamp timestamp, created timestamp, "
        + ", ".join(f"{f} {'double' if f.startswith('decline_rate') else 'long'}" for f in FIELDS)
    )
    return spark.createDataFrame(
        [
            ("i", key, at, DAY, *[0.25 if f.startswith("decline_rate") else n for f in FIELDS])
            for key, at, n in rows
        ],
        columns,
    )


def online(store, key):
    return store.get_online_features(
        features=[f"customer_daily:{f}" for f in FIELDS],
        entity_rows=[{"integration_id": "i", "customer_id": key}],
    ).to_dict()


def test_rewritten_lookback_deleted_entity_and_repeat_refresh(spark, feature_store):
    prior = data(spark, [("updated", DAY, 1), ("deleted", DAY, 1), ("newer", DAY, 1)])
    current = data(spark, [("updated", DAY, 2), ("newer", DAY + timedelta(days=1), 3)])
    write_online(feature_store, "customer_daily", prior.toPandas().to_dict("records"), FIELDS)
    feature_store.config.online_store.skip_dedup = True
    for _ in range(2):
        refreshed = materialize.refresh_frame(
            current,
            prior,
            "customer",
            [(date(2025, 9, 1), date(2025, 9, 2))],
            DAY + timedelta(days=2),
        )
        write_online(
            feature_store, "customer_daily", refreshed.toPandas().to_dict("records"), FIELDS
        )
        assert online(feature_store, "updated")["attempt_count_7d"] == [2]
        assert online(feature_store, "deleted")["attempt_count_7d"] == [None]
        assert online(feature_store, "newer")["attempt_count_7d"] == [3]
        assert online(feature_store, "updated")["decline_rate_7d"] == [0.25]


def test_range_chain_missing_state_and_expired_snapshot_fallback():
    snapshots = {
        1: SimpleNamespace(snapshot_id=1, parent_snapshot_id=None, summary={}),
        2: SimpleNamespace(
            snapshot_id=2, parent_snapshot_id=1, summary={"dp3.range": "2025-09-01/2025-09-03"}
        ),
    }
    table = SimpleNamespace(snapshot_by_id=snapshots.get)
    assert materialize.changed_ranges(table, None, 2) is None
    assert materialize.changed_ranges(table, 42, 2) is None
    assert materialize.changed_ranges(table, 1, 2) == [(date(2025, 9, 1), date(2025, 9, 3))]
    assert materialize.changed_ranges(table, 2, 2) == []


def test_missing_online_entity_fails_and_does_not_publish_state(
    feature_store, tmp_path, monkeypatch
):
    uri = str(tmp_path / "state.json")
    write_json(uri, {"pins": {"customer": 1}, "end": "before"})
    expected = {
        "integration_id": "i",
        "customer_id": "absent",
        "event_timestamp": DAY,
        **dict.fromkeys(FIELDS, 7),
    }
    with pytest.raises(ValueError, match="differs"):
        check_online.validate(feature_store, {"samples": {"customer": [expected]}})
    assert read_json(uri)["end"] == "before"
    report_uri = str(tmp_path / "report.json")
    write_json(
        report_uri, {"pins": {}, "default": True, "previous_state": read_json(uri), "now": "after"}
    )
    monkeypatch.setenv("RUN_REPORT", report_uri)
    monkeypatch.setenv("FEAST_MATERIALIZE_STATE", uri)
    monkeypatch.setattr(check_online, "catalog", Mock())
    monkeypatch.setattr("sys.argv", ["check_online", "--publish"])
    with pytest.raises(ValueError, match="passing check"):
        check_online.main()
    assert read_json(uri)["end"] == "before"


def test_explicit_checked_range_leaves_consumption_state_unchanged(tmp_path, monkeypatch):
    uri = str(tmp_path / "state.json")
    write_json(uri, {"pins": {}, "end": "before"})
    report_uri = str(tmp_path / "report.json")
    write_json(report_uri, {"pins": {}, "checked": True, "default": False, "now": "after"})
    monkeypatch.setenv("RUN_REPORT", report_uri)
    monkeypatch.setenv("FEAST_MATERIALIZE_STATE", uri)
    monkeypatch.setattr(check_online, "catalog", Mock())
    monkeypatch.setattr("sys.argv", ["check_online", "--publish"])
    check_online.main()
    assert read_json(uri)["end"] == "before"


def test_missing_state_full_refresh_then_explicit_range_preserves_registry(
    spark, feature_store, monkeypatch
):
    from fraud_feast.repo import DAILY_TABLES, KEYS

    for scope, key in KEYS.items():
        fields = DAILY_FIELDS[scope]
        columns = (
            f"integration_id string, {key} string, event_timestamp timestamp, created timestamp, "
            + ", ".join(
                f"{f} {'double' if f.startswith('decline_rate') else 'long'}" for f in fields
            )
        )
        rows = [
            (
                "full-refresh",
                scope,
                DAY,
                DAY + timedelta(days=2),
                *[0.25 if f.startswith("decline_rate") else 7 for f in fields],
            )
        ]
        spark.createDataFrame(rows, columns).writeTo(f"lake.{DAILY_TABLES[scope]}").using(
            "iceberg"
        ).createOrReplace()

    class Catalog:
        def load_table(self, name):
            identifier = f"lake.{name}"
            snapshots = {
                row.snapshot_id: SimpleNamespace(
                    snapshot_id=row.snapshot_id,
                    parent_snapshot_id=row.parent_id,
                    summary=dict(row.summary),
                )
                for row in spark.sql(f"SELECT * FROM {identifier}.snapshots").collect()
            }
            head = spark.sql(
                f"SELECT snapshot_id FROM {identifier}.refs WHERE name='main'"
            ).first()[0]
            return SimpleNamespace(
                current_snapshot=lambda: snapshots[head], snapshot_by_id=snapshots.get
            )

    pinned = materialize.PinnedStore({}, config=feature_store.config)

    def ingest(frame, view, fields):
        rows = frame.toPandas().to_dict("records")
        write_online(pinned, view, rows, fields)
        return len(rows)

    monkeypatch.setattr(materialize, "ingest", ingest)
    now = DAY + timedelta(days=3)
    report = materialize.run(spark, pinned, Catalog(), now, None)
    assert report["full_refresh"] is True
    assert report["refreshed"] == 2
    assert check_online.validate(pinned, report) == 2
    before = {
        scope: pinned.get_feature_view(f"{scope}_daily").materialization_intervals for scope in KEYS
    }
    state = {"pins": report["pins"], "end": report["now"]}
    explicit = materialize.run(
        spark, pinned, Catalog(), now, state, DAY.date(), (DAY + timedelta(days=1)).date()
    )
    assert explicit["default"] is False
    assert explicit["previous_state"] == state
    assert before == {
        scope: pinned.get_feature_view(f"{scope}_daily").materialization_intervals for scope in KEYS
    }
    assert check_online.validate(pinned, explicit) == 2
