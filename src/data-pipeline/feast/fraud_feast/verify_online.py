import math
import os
from datetime import UTC, datetime

from fraud_feast.lake import read_json, write_json
from fraud_feast.repo import DAILY_FIELDS
from fraud_feast.runtime import store


def validate(feature_store, report):
    from feast.type_map import feast_value_type_to_python_type

    checked = 0
    for scope, samples in report["samples"].items():
        for row in samples:
            entities = {
                "integration_id": row["integration_id"],
                "customer_id": row.get("customer_id") or "",
                "payment_method_fingerprint": row.get("payment_method_fingerprint") or "",
            }
            result = feature_store.get_online_features(
                features=[f"{scope}_daily:{field}" for field in DAILY_FIELDS[scope]],
                entity_rows=[entities],
                full_feature_names=True,
            ).proto
            names = {name: index for index, name in enumerate(result.metadata.feature_names.val)}
            for field in DAILY_FIELDS[scope]:
                column = f"{scope}_daily__{field}"
                if column not in names:
                    raise ValueError("online response omitted a daily feature")
                value = result.results[names[column]]
                if len(value.values) != 1 or len(value.event_timestamps) != 1:
                    raise ValueError("invalid online response shape")
                actual = feast_value_type_to_python_type(value.values[0])
                if isinstance(actual, float) and math.isnan(actual):
                    actual = None
                if actual != row[field]:
                    raise ValueError(f"online value differs from offline row: {column}")
                expected_time = row["event_timestamp"]
                if isinstance(expected_time, str):
                    expected_time = datetime.fromisoformat(expected_time)
                if value.event_timestamps[0].ToDatetime(tzinfo=UTC) != expected_time.replace(
                    tzinfo=UTC
                ):
                    raise ValueError("online event time differs from offline row")
            checked += 1
    return checked


def main():
    uri = os.environ["RUN_REPORT"]
    report = read_json(uri)
    if report is None:
        raise ValueError("materialization report is missing")
    report["sampled"] = validate(store(online=True), report)
    report["checked"] = True
    write_json(uri, report)


if __name__ == "__main__":
    main()
