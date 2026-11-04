"""Tests for reading and validating a training-set version, with an in-memory Iceberg table."""

import json
from datetime import UTC, datetime
from types import SimpleNamespace

import numpy as np
import pyarrow as pa
import pytest

from risk_model.data import KEY, DatasetReader, Split
from risk_model.split import Window

FEATURES = ["customer_daily__count", "request_features__amount"]
PROPERTIES = {
    "ml.features": json.dumps(FEATURES),
    "ml.sample_rate": "0.1",
    "ml.feature_service": "payment_risk_v1",
    "ml.sources": json.dumps({"lake.gold.label_attempt": 5}),
    "ml.window": "2026-10-01..2026-10-03",
    "ml.cutoff": "2026-10-04T00:00:00Z",
}


class FakeTable:
    """An Iceberg table that serves one Arrow table for any scan and records the scan arguments."""

    def __init__(self, data, snapshots=(7,)):
        self.data, self.snapshots, self.scans = data, snapshots, []

    def snapshot_by_id(self, snapshot_id):
        """Returns a snapshot carrying the properties, or None for an unknown ID."""
        if snapshot_id not in self.snapshots:
            return None
        return SimpleNamespace(summary=SimpleNamespace(additional_properties=PROPERTIES))

    def scan(self, **options):
        """Records the options and returns an object whose `to_arrow` gives the data."""
        self.scans.append(options)
        return SimpleNamespace(to_arrow=lambda: self.data)


def frame(rows, label=None, **columns):
    """Returns an Arrow table of attempts: one row per `(day, label, bucket)`, with two features."""
    count = len(rows)
    table = {
        "integration_id": ["i"] * count,
        "payment_id": [f"p{n}" for n in range(count)],
        "attempt_id": [f"a{n}" for n in range(count)],
        "started_at": pa.array(
            [datetime(2026, 10, day, 12, tzinfo=UTC) for day, _, _ in rows],
            pa.timestamp("us", "UTC"),
        ),
        "label": pa.array(label if label is not None else [r[1] for r in rows], pa.bool_()),
        "bucket": pa.array([r[2] for r in rows], pa.int32()),
        "row_hash": pa.array(list(range(count)), pa.int64()),
        FEATURES[0]: pa.array([float(n) for n in range(count)]),
        FEATURES[1]: pa.array([None if n == 0 else 10.0 * n for n in range(count)]),
    }
    table.update(columns)
    return pa.table(table)


def reader(table, snapshots=(7,)):
    """Returns a DatasetReader on a fake catalog serving the table."""
    fake = FakeTable(table, snapshots)
    return DatasetReader(SimpleNamespace(load_table=lambda name: fake), 7), fake


ROWS = [(1, True, 5), (1, False, 5), (1, False, 500), (2, True, 90), (2, False, 800)]


def test_an_unknown_snapshot_is_rejected():
    """A snapshot ID the table does not have raises ValueError."""
    with pytest.raises(ValueError, match="no snapshot 8"):
        DatasetReader(SimpleNamespace(load_table=lambda name: FakeTable(frame(ROWS))), 8)


def test_provenance_and_properties_come_from_the_snapshot_summary():
    """The feature list, sample rate and provenance are read from the snapshot's properties."""
    version, _ = reader(frame(ROWS))
    assert version.features == FEATURES and version.sample_rate == 0.1
    assert version.provenance() == {
        "training_set_snapshot": 7,
        "feature_service": "payment_risk_v1",
        "features": FEATURES,
        "sources": {"lake.gold.label_attempt": 5},
        "window": "2026-10-01..2026-10-03",
        "cutoff": "2026-10-04T00:00:00Z",
        "sample_rate": 0.1,
    }


def test_a_scan_pins_the_snapshot_and_narrows_by_window_and_columns():
    """scan passes the snapshot, a time filter for a window and the selected columns to Iceberg."""
    version, fake = reader(frame(ROWS))
    version.scan()
    version.scan(Window.parse("2026-10-01..2026-10-01"), ["label"])
    assert fake.scans[0] == {"snapshot_id": 7}
    assert fake.scans[1]["snapshot_id"] == 7 and fake.scans[1]["selected_fields"] == ("label",)
    assert "row_filter" in fake.scans[1]


def test_a_read_weights_negatives_by_the_inverse_sample_rate_and_keeps_nulls_as_nan():
    """Positives weigh 1, kept negatives weigh 1/rate, and a null feature becomes NaN."""
    split = reader(frame(ROWS))[0].read(Window.parse("2026-10-01..2026-10-02"))
    assert split.y.tolist() == [1.0, 0.0, 0.0, 1.0, 0.0]
    assert split.w.tolist() == [1.0, 10.0, 10.0, 1.0, 10.0]
    assert split.x.shape == (5, 2) and split.x.dtype == np.float32
    assert np.isnan(split.x[0, 1]) and split.x[1, 1] == 10.0
    assert split.features == FEATURES
    assert split.keys[0] == "i\x1fp0\x1fa0"


def test_a_lower_rate_drops_negatives_by_bucket_and_keeps_every_positive():
    """Sampling down keeps negatives whose bucket is below rate*1000, and all positives."""
    split = reader(frame(ROWS))[0].read(Window.parse("2026-10-01..2026-10-02"), rate=0.05)
    assert split.y.tolist() == [1.0, 0.0, 1.0]
    assert split.w.tolist() == [1.0, 20.0, 1.0]


@pytest.mark.parametrize("rate", [0, -0.1, 0.11])
def test_a_rate_outside_the_stored_sample_is_rejected(rate):
    """A rate of zero, below zero or above the stored sample rate raises ValueError."""
    with pytest.raises(ValueError, match="sampling rate"):
        reader(frame(ROWS))[0].read(Window.parse("2026-10-01..2026-10-02"), rate=rate)


def test_a_clean_version_validates_and_reports_rates_and_null_shares():
    """A valid version reports ok, its row count, a weighted fraud rate per day and null shares."""
    report = reader(frame(ROWS))[0].validate()
    assert report["ok"] and report["failures"] == [] and report["rows"] == 5
    assert report["fraud_rate_by_day"]["2026-10-01"] == pytest.approx(1 / (1 + 2 / 0.1))
    assert report["fraud_rate_by_day"]["2026-10-02"] == pytest.approx(1 / (1 + 1 / 0.1))
    assert report["null_share_by_day"]["2026-10-01"][FEATURES[1]] == pytest.approx(1 / 3)


def test_missing_columns_fail_validation_at_once():
    """A table without a required column reports it and skips the other checks."""
    report = reader(frame(ROWS).drop_columns(["row_hash"]))[0].validate()
    assert report == {"ok": False, "failures": ["missing columns: row_hash"]}


def test_a_text_feature_column_fails_validation():
    """A feature column that is not numeric or boolean is reported."""
    table = frame(ROWS, **{FEATURES[0]: ["x"] * len(ROWS)})
    report = reader(table)[0].validate()
    assert not report["ok"] and "non-numeric feature columns" in report["failures"][0]


def test_duplicate_keys_and_null_labels_fail_validation():
    """Repeated attempt keys and null labels are each counted."""
    table = frame(ROWS, label=[True, None, False, False, False])
    table = table.set_column(
        table.column_names.index("attempt_id"), "attempt_id", pa.array(["a"] * 5)
    )
    table = table.set_column(
        table.column_names.index("payment_id"), "payment_id", pa.array(["p"] * 5)
    )
    report = reader(table)[0].validate()
    assert "4 duplicated attempt keys" in report["failures"]
    assert "1 null labels" in report["failures"]


def test_a_day_with_an_implausible_fraud_rate_fails_validation():
    """A daily weighted fraud rate outside the bounds is reported with its day."""
    report = reader(frame(ROWS))[0].validate(rate_bounds=(0.5, 1.0))
    assert not report["ok"]
    assert any("2026-10-01 fraud rate" in failure for failure in report["failures"])


def test_the_attempt_key_is_three_columns():
    """The attempt key is the integration, payment and attempt IDs."""
    assert KEY == ("integration_id", "payment_id", "attempt_id")


def test_joined_splits_keep_every_row_in_order_with_the_shared_feature_list():
    """join stacks the rows of several splits and keeps the feature list of the first."""
    one = Split(
        np.ones((2, 2), np.float32),
        np.array([0, 1], np.float32),
        np.ones(2, np.float32),
        np.array(["a", "b"], object),
        ["x", "y"],
    )
    two = Split(
        np.zeros((1, 2), np.float32),
        np.array([1], np.float32),
        np.full(1, 5, np.float32),
        np.array(["c"], object),
        ["x", "y"],
    )
    joined = Split.join([one, two])
    assert joined.x.shape == (3, 2) and joined.features == ["x", "y"]
    assert joined.y.tolist() == [0, 1, 1] and joined.w.tolist() == [1, 1, 5]
    assert joined.keys.tolist() == ["a", "b", "c"]
