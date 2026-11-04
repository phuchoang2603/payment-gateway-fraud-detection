"""The hourly drift check: reads the drift API and starts one bounded retraining run when the rule says so."""

import json
import os
import urllib.request
from datetime import datetime, timezone

from risk_model.drift import PREFIX, decide, load_policy
from risk_model.runs import Pipelines


def fetch(base, window):
    """Returns the drift API's answer for one window."""
    with urllib.request.urlopen(
        f"{base.rstrip('/')}/v1/drift?window={window}", timeout=30
    ) as response:
        return json.load(response)


def parameters(trigger):
    """Returns the pipeline parameters of a started run: the window sizes from the environment and what started it."""
    return {
        "train_days": int(os.environ.get("TRAIN_DAYS", "56")),
        "validation_days": int(os.environ.get("VALIDATION_DAYS", "14")),
        "test_days": int(os.environ.get("TEST_DAYS", "14")),
        "lag_days": int(os.environ.get("LAG_DAYS", "1")),
        "trigger": trigger,
    }


def check(base, pipelines, policy, now):
    """Decides once and starts a run if needed; returns what was decided and why."""
    short, long = fetch(base, "6h"), fetch(base, "24h")
    trained = datetime.fromisoformat(long["created"])
    start, reason = decide(
        short["assessment"],
        long["assessment"],
        runs=pipelines.runs(PREFIX),
        trained=trained,
        now=now,
        policy=policy,
    )
    outcome = {
        "version": long["version"],
        "start": start,
        "reason": reason,
        "states": [short["assessment"]["state"], long["assessment"]["state"]],
        "over": long["assessment"].get("over", []),
    }
    if start:
        name = f"{PREFIX}{'age-' if reason == 'age' else ''}{long['version']}-{now:%Y%m%d%H}"
        description = f"{reason}: {json.dumps(outcome['over'])} over {long['assessment']['samples']:.0f} requests"
        outcome["run"] = pipelines.start(name, description, parameters(reason))
    return outcome


def main():
    """Runs one check against `DRIFT_URL` and prints the outcome as JSON."""
    outcome = check(
        os.environ["DRIFT_URL"],
        Pipelines(),
        load_policy(os.environ.get("DRIFT_POLICY", "")),
        datetime.now(timezone.utc),
    )
    print(json.dumps(outcome), flush=True)


if __name__ == "__main__":
    main()
