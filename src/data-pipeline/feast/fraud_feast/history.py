import json
import os
import time
import urllib.request
from collections import Counter
from datetime import UTC, date, datetime
from urllib.parse import urlencode

from confluent_kafka import ConsumerGroupTopicPartitions, TopicPartition
from confluent_kafka.admin import AdminClient

from fraud_feast.lake import read_json, write_json


def rest(path):
    with urllib.request.urlopen(os.environ["HISTORY_FLINK_URL"] + path, timeout=30) as response:
        return json.load(response)


def consumed(admin, seed):
    epoch = os.environ["FEATURE_EPOCH"]
    group = f"fraud-features-{epoch}"
    offsets = (
        admin.list_consumer_group_offsets(
            [
                ConsumerGroupTopicPartitions(
                    group,
                    [
                        TopicPartition(f"payments.events.{epoch}", 0),
                        TopicPartition(f"fraud.clock.{epoch}", 0),
                    ],
                )
            ],
            request_timeout=30,
        )[group]
        .result()
        .topic_partitions
    )
    by_topic = {item.topic: item.offset for item in offsets}
    result = {
        "input": by_topic[f"payments.events.{epoch}"],
        "clock": by_topic[f"fraud.clock.{epoch}"],
    }
    return (
        result
        if result["input"] >= seed["input_end"] and result["clock"] >= seed["clock_end"]
        else None
    )


def checkpoints():
    running = [job["jid"] for job in rest("/jobs/overview")["jobs"] if job["state"] == "RUNNING"]
    if len(running) != 1:
        raise ValueError("history must have exactly one running Flink job")
    job = running[0]
    latest = rest(f"/jobs/{job}/checkpoints").get("latest", {}).get("completed")
    return job, latest["id"] if latest else -1


def counters(job):
    result = Counter(duplicates=0, too_late=0, rejects=0, decode_rejects=0)
    for vertex in rest(f"/jobs/{job}")["vertices"]:
        prefix = f"/jobs/{job}/vertices/{vertex['id']}/subtasks/metrics"
        names = [
            item["id"]
            for item in rest(prefix)
            if item["id"].endswith(("duplicates_dropped", "too_late", "rejected"))
        ]
        if names:
            for metric in rest(prefix + "?" + urlencode({"get": ",".join(names), "agg": "sum"})):
                name = metric["id"]
                count = int(float(metric.get("sum", 0)))
                key = (
                    "duplicates"
                    if name.endswith("duplicates_dropped")
                    else "too_late"
                    if name.endswith("too_late")
                    else "rejects"
                )
                result[key] += count
                if name.endswith("rejected") and any(
                    f"reject_class.{kind}." in name
                    for kind in ("undecodable", "invalid_event", "invalid_context")
                ):
                    result["decode_rejects"] += count
    return dict(result)


def bounds(start, end):
    first = datetime.combine(date.fromisoformat(start), datetime.min.time(), UTC)
    last = datetime.combine(date.fromisoformat(end), datetime.min.time(), UTC)
    if not os.environ["FEATURE_EPOCH"].startswith("history-") or first >= last:
        raise ValueError("history requires a history epoch and a nonempty range")
    return first, last


class HistoryReplay:
    def __init__(self, start, end):
        self.first, self.last = bounds(start, end)
        self.admin = AdminClient({"bootstrap.servers": os.environ["FEATURE_KAFKA_BROKERS"]})
        self.start, self.end = start, end
        self.seed = self.progress = self.checkpoint = None
        self.last_check = 0
        self.deadline = time.monotonic() + int(os.environ.get("HISTORY_TIMEOUT_SECONDS", "259200"))

    def ends(self):
        if time.monotonic() > self.deadline:
            raise TimeoutError("history unit did not finish before its deadline")
        if time.monotonic() - self.last_check < 5:
            return False
        self.last_check = time.monotonic()
        self.seed = read_json(os.environ["HISTORY_SEED_REPORT"])
        if self.seed and self.seed.get("failed"):
            raise RuntimeError("history seed failed")
        if self.seed and self.seed.get("done"):
            self.progress = consumed(self.admin, self.seed)
            if self.progress:
                _, latest = checkpoints()
                if self.checkpoint is None:
                    self.checkpoint = latest
                elif latest >= self.checkpoint + 2:
                    return True
        return False

    def report(self, rows):
        job, _ = checkpoints()
        write_json(
            os.environ["RUN_REPORT"],
            {
                "start": self.start,
                "end": self.end,
                "epoch": os.environ["FEATURE_EPOCH"],
                "seed": self.seed,
                "counts": counters(job),
                "rows": rows,
                "consumed": self.progress,
                "gate": False,
            },
        )


def validate_gate(report):
    seed, progress = report["seed"], report["consumed"]
    if not seed or not seed.get("done") or report["counts"]["decode_rejects"]:
        raise ValueError("history seed is incomplete or replay has decode rejects")
    if (
        not progress
        or progress["input"] < seed["input_end"]
        or progress["clock"] < seed["clock_end"]
    ):
        raise ValueError("history has not consumed every seeded record")


def main():
    uri = os.environ["RUN_REPORT"]
    report = read_json(uri)
    validate_gate(report)
    report["gate"] = True
    write_json(uri, report)


if __name__ == "__main__":
    main()
