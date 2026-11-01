import argparse
import math
import os
from datetime import UTC, datetime

from fraud_feast.lake import catalog, read_json, write_json
from fraud_feast.materialize import unchanged
from fraud_feast.repo import DAILY_FIELDS, DAILY_TABLES, KEYS
from fraud_feast.runtime import store


def verify_samples(lake, report):
    from functools import reduce

    from pyiceberg.expressions import And, EqualTo, LessThanOrEqual, Or

    now = datetime.fromisoformat(report["now"])
    for scope, samples in report["samples"].items():
        if not samples:
            continue
        key = KEYS[scope]
        predicate = reduce(
            Or,
            [
                And(EqualTo("integration_id", row["integration_id"]), EqualTo(key, row[key]))
                for row in samples
            ],
        )
        rows = (
            lake.load_table(DAILY_TABLES[scope])
            .scan(
                snapshot_id=report["pins"][scope],
                row_filter=And(predicate, LessThanOrEqual("event_timestamp", now)),
                selected_fields=(
                    "integration_id",
                    key,
                    "event_timestamp",
                    "created",
                    *DAILY_FIELDS[scope],
                ),
            )
            .to_arrow_batch_reader()
        )
        latest = {}
        for batch in rows:
            for row in batch.to_pylist():
                identity = row["integration_id"], row[key]
                previous = latest.get(identity)
                if previous is None or (row["event_timestamp"], row["created"]) > (
                    previous["event_timestamp"],
                    previous["created"],
                ):
                    latest[identity] = row
        for sample in samples:
            expected = latest.get((sample["integration_id"], sample[key]))
            if expected is None:
                if any(sample[field] is not None for field in DAILY_FIELDS[scope]):
                    raise ValueError("sample is absent from the pinned snapshot")
            elif (
                any(sample[field] != expected[field] for field in DAILY_FIELDS[scope])
                or datetime.fromisoformat(str(sample["event_timestamp"])).replace(tzinfo=UTC)
                != expected["event_timestamp"]
            ):
                raise ValueError("sample differs from pinned offline latest row")


def validate(feature_store, report):
    checked = 0
    service = feature_store.get_feature_service("fraud_model_v1")
    for scope, samples in report["samples"].items():
        for row in samples:
            entities = {
                "integration_id": row["integration_id"],
                "customer_id": row.get("customer_id") or "",
                "payment_method_fingerprint": row.get("payment_method_fingerprint") or "",
            }
            result = feature_store.get_online_features(
                features=service, entity_rows=[entities], full_feature_names=True
            )
            proto = result.proto
            names = {name: index for index, name in enumerate(proto.metadata.feature_names.val)}
            for field in DAILY_FIELDS[scope]:
                column = f"{scope}_daily__{field}"
                if column not in names:
                    raise ValueError("online response omitted a daily feature")
                value = proto.results[names[column]]
                if len(value.values) != 1 or len(value.event_timestamps) != 1:
                    raise ValueError("invalid online response shape")
                expected = row[field]
                if hasattr(expected, "item"):
                    expected = expected.item()
                from feast.type_map import feast_value_type_to_python_type

                actual = feast_value_type_to_python_type(value.values[0])
                if isinstance(actual, float) and math.isnan(actual):
                    actual = None
                if actual != expected:
                    raise ValueError(f"online value differs from pinned offline row: {column}")
                expected_time = row["event_timestamp"]
                if isinstance(expected_time, str):
                    expected_time = datetime.fromisoformat(expected_time)
                if value.event_timestamps[0].ToDatetime(tzinfo=UTC) != expected_time.replace(
                    tzinfo=UTC
                ):
                    raise ValueError("online event time differs from pinned offline row")
            checked += 1
    return checked


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--publish", action="store_true")
    args = parser.parse_args()
    uri = os.environ["RUN_REPORT"]
    report = read_json(uri)
    if report is None:
        raise ValueError("materialization report is missing")
    lake = catalog()
    unchanged(lake, report["pins"])
    if args.publish:
        if not report.get("checked"):
            raise ValueError("cannot publish materialization state without a passing check")
        if report["default"]:
            state_uri = os.environ["FEAST_MATERIALIZE_STATE"]
            if read_json(state_uri) != report["previous_state"]:
                raise ValueError("materialization state changed concurrently")
            write_json(state_uri, {"pins": report["pins"], "end": report["now"]})
    else:
        verify_samples(lake, report)
        report["sampled"] = validate(store(online=True), report)
        report["checked"] = True
        write_json(uri, report)


if __name__ == "__main__":
    main()
