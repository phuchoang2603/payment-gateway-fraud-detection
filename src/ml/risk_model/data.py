"""Reads a versioned training set from Iceberg into arrays, and validates it."""

import json
from dataclasses import dataclass

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
from pyiceberg.expressions import And, GreaterThanOrEqual, LessThan

TABLE = "ml.payment_risk_v1_examples"
KEY = ("integration_id", "payment_id", "attempt_id")
BUCKETS = 1000
CHUNK_DAYS = 16
META = (*KEY, "started_at", "label", "bucket", "row_hash")


@dataclass
class Split:
    """The rows of one window as arrays: features `x`, labels `y`, sample weights `w` and attempt keys."""

    x: np.ndarray
    y: np.ndarray
    w: np.ndarray
    keys: np.ndarray
    features: list

    def select(self, names):
        """Returns a Split with only the named feature columns, in that order."""
        columns = [self.features.index(name) for name in names]
        return Split(self.x[:, columns], self.y, self.w, self.keys, list(names))

    @classmethod
    def join(cls, parts):
        """Returns one Split with the rows of the given splits, which share their feature list, in order."""
        return cls(
            np.vstack([part.x for part in parts]),
            np.concatenate([part.y for part in parts]),
            np.concatenate([part.w for part in parts]),
            np.concatenate([part.keys for part in parts]),
            parts[0].features,
        )

    def ordered(self):
        """Returns a Split sorted by attempt key, so training does not depend on read order."""
        order = np.argsort(self.keys, kind="stable")
        return Split(self.x[order], self.y[order], self.w[order], self.keys[order], self.features)


def matrix(table, features):
    """Returns the named columns of an Arrow table as a float32 matrix with nulls as NaN."""
    x = np.empty((table.num_rows, len(features)), dtype=np.float32)
    for index, name in enumerate(features):
        x[:, index] = table[name].cast(pa.float32()).to_numpy(zero_copy_only=False)
    return x


class DatasetReader:
    """Reads one snapshot of the training-set table.

    `catalog` is the Iceberg catalog and `snapshot_id` the version to read, which must exist. The
    snapshot's summary records its provenance: feature list, source table snapshots, window, cutoff and
    sample rate. `read` returns a window as a Split, `scan` returns raw Arrow rows, and `validate`
    checks the whole version.
    """

    def __init__(self, catalog, snapshot_id):
        self.table = catalog.load_table(TABLE)
        self.snapshot_id = int(snapshot_id)
        snapshot = self.table.snapshot_by_id(self.snapshot_id)
        if snapshot is None:
            raise ValueError(f"{TABLE} has no snapshot {snapshot_id}")
        self.properties = dict(snapshot.summary.additional_properties)

    @classmethod
    def at(cls, snapshot_id):
        """Returns a reader on the default catalog for one snapshot."""
        from fraud_feast.lake import catalog

        return cls(catalog(), snapshot_id)

    @property
    def features(self):
        """Returns the ordered `<view>__<feature>` column names the model trains on."""
        return json.loads(self.properties["ml.features"])

    @property
    def sample_rate(self):
        """Returns the share of negatives kept when the version was built."""
        return float(self.properties["ml.sample_rate"])

    def provenance(self):
        """Returns what the version was built from, as recorded in its snapshot summary."""
        return {
            "training_set_snapshot": self.snapshot_id,
            "feature_service": self.properties["ml.feature_service"],
            "features": self.features,
            "sources": json.loads(self.properties["ml.sources"]),
            "window": self.properties["ml.window"],
            "cutoff": self.properties["ml.cutoff"],
            "sample_rate": self.sample_rate,
        }

    def scan(self, window=None, columns=None):
        """Returns the rows of the snapshot as an Arrow table, optionally one window and some columns."""
        scan = {"snapshot_id": self.snapshot_id}
        if window is not None:
            scan["row_filter"] = And(
                GreaterThanOrEqual("started_at", window.start.isoformat()),
                LessThan("started_at", window.end.isoformat()),
            )
        if columns is not None:
            scan["selected_fields"] = tuple(columns)
        return self.table.scan(**scan).to_arrow()

    def read(self, window, rate=None):
        """Returns a window as a Split, optionally sampling negatives down to `rate`.

        Kept negatives weigh `1 / rate` and positives weigh 1, so weighted metrics match the real rate.
        """
        stored = self.sample_rate
        rate = stored if rate is None else rate
        if not 0 < rate <= stored:
            raise ValueError(f"sampling rate {rate} must be within (0, {stored}]")
        table = self.scan(window)
        if rate < stored:
            table = table.filter(
                pc.or_(pc.less(table["bucket"], round(rate * BUCKETS)), table["label"])
            )
        y = table["label"].to_numpy(zero_copy_only=False).astype(np.float32)
        keys = pc.binary_join_element_wise(
            *[table[name].cast(pa.string()) for name in KEY], "\x1f"
        ).to_numpy(zero_copy_only=False)
        return Split(
            matrix(table, self.features),
            y,
            np.where(y > 0, 1.0, 1.0 / rate).astype(np.float32),
            keys,
            self.features,
        )

    def validate(self, rate_bounds=(0.0005, 0.25)):
        """Returns a report: missing or non-numeric columns, duplicate keys, null labels and daily fraud rates."""
        table = self.scan()
        failures = []
        missing = [name for name in (*META, *self.features) if name not in table.column_names]
        if missing:
            return {"ok": False, "failures": [f"missing columns: {', '.join(missing)}"]}
        wrong = [
            name
            for name in self.features
            if not any(
                check(table.schema.field(name).type)
                for check in (pa.types.is_integer, pa.types.is_floating, pa.types.is_boolean)
            )
        ]
        if wrong:
            failures.append(f"non-numeric feature columns: {', '.join(wrong)}")
        keys = table.select(KEY).group_by(list(KEY)).aggregate([]).num_rows
        if keys != table.num_rows:
            failures.append(f"{table.num_rows - keys} duplicated attempt keys")
        if table["label"].null_count:
            failures.append(f"{table['label'].null_count} null labels")
        table = table.append_column("day", pc.floor_temporal(table["started_at"], unit="day"))
        positives = table.group_by("day").aggregate([("label", "sum"), ("label", "count")])
        rate = self.sample_rate
        days = {}
        for row in positives.to_pylist():
            positive = row["label_sum"] or 0
            negative = row["label_count"] - positive
            weighted = positive / (positive + negative / rate) if positive + negative else 0.0
            days[str(row["day"].date())] = weighted
            if not rate_bounds[0] <= weighted <= rate_bounds[1]:
                failures.append(
                    f"{row['day'].date()} fraud rate {weighted:.5f} outside {rate_bounds}"
                )
        nulls = table.group_by("day").aggregate(
            [(name, "count", pc.CountOptions(mode="only_null")) for name in self.features]
            + [("label", "count", pc.CountOptions(mode="all"))]
        )
        null_share = {
            str(row["day"].date()): {
                name: row[f"{name}_count"] / row["label_count"] for name in self.features
            }
            for row in nulls.to_pylist()
        }
        return {
            "ok": not failures,
            "failures": failures,
            "rows": table.num_rows,
            "fraud_rate_by_day": days,
            "null_share_by_day": null_share,
        }
