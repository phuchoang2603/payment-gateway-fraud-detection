import argparse
import json
import os
import urllib.request
from collections import Counter
from datetime import UTC, date, datetime
from urllib.parse import urlencode

from fraud_feast.lake import read_json, write_json


def rest(path):
    with urllib.request.urlopen(os.environ["HISTORY_FLINK_URL"] + path, timeout=30) as response:
        return json.load(response)


def finished_job():
    jobs = rest("/jobs/overview")["jobs"]
    if len(jobs) != 1 or jobs[0]["state"] != "FINISHED":
        raise ValueError("history requires exactly one finished Flink job")
    return jobs[0]["jid"]


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
        self.start, self.end = start, end

    def check(self):
        seed = read_json(os.environ["HISTORY_SEED_REPORT"])
        if not seed or not seed.get("done"):
            raise ValueError("history seed is incomplete or replay has decode rejects")
        identity = {
            "start": self.start,
            "end": self.end,
            "epoch": os.environ["FEATURE_EPOCH"],
            "topic": os.environ["FEATURE_UPDATE_TOPIC"],
            "seed": seed,
        }
        receipt_uri = os.environ["RUN_REPORT"] + ".verified.json"
        receipt = read_json(receipt_uri)
        if receipt is None:
            job = finished_job()
            counts = counters(job)
            if counts["decode_rejects"]:
                raise ValueError("history seed is incomplete or replay has decode rejects")
            receipt = {**identity, "job_id": job, "counts": counts}
            write_json(receipt_uri, receipt)
        if any(receipt.get(key) != value for key, value in identity.items()):
            raise ValueError("history verification does not match the replay unit")
        counts = receipt["counts"]
        if not receipt.get("job_id") or counts["decode_rejects"]:
            raise ValueError("history seed is incomplete or replay has decode rejects")
        self.seed, self.counts = seed, counts

    def report(self, rows):
        write_json(
            os.environ["RUN_REPORT"],
            {
                "start": self.start,
                "end": self.end,
                "epoch": os.environ["FEATURE_EPOCH"],
                "seed": self.seed,
                "counts": self.counts,
                "rows": rows,
            },
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    args = parser.parse_args()
    HistoryReplay(args.start, args.end).check()
