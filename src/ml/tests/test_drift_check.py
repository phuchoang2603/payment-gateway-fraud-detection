"""Tests for the hourly drift check: what it reads, what it decides and the run it starts."""

from datetime import datetime, timedelta, timezone

from risk_model import drift_check
from risk_model.drift import Policy

NOW = datetime(2026, 11, 10, 12, tzinfo=timezone.utc)


class Runs:
    """A Kubeflow Pipelines client that lists prepared runs and records what it starts."""

    def __init__(self, runs=()):
        self.listed, self.started = list(runs), []

    def runs(self, prefix):
        """Returns the prepared runs."""
        assert prefix == "drift-"
        return self.listed

    def start(self, name, description, parameters):
        """Records the start and returns a run ID."""
        self.started.append((name, description, parameters))
        return "run-1"


def answers(monkeypatch, short, long, created=NOW - timedelta(days=5)):
    """Makes the drift API answer with the given states for the two windows."""

    def fetch(base, window):
        state = short if window == "6h" else long
        assessment = {"state": state, "samples": 5000.0, "over": ["a", "b", "c"]}
        return {"version": "v3", "created": created.isoformat(), "assessment": assessment}

    monkeypatch.setattr(drift_check, "fetch", fetch)


def test_persistent_drift_starts_a_run_named_for_the_version_and_the_hour(monkeypatch):
    """Both windows drifting starts one run with the window sizes and what started it."""
    answers(monkeypatch, "drifting", "drifting")
    monkeypatch.setenv("TRAIN_DAYS", "36")
    pipelines = Runs()
    outcome = drift_check.check("http://drift", pipelines, Policy(), NOW)
    assert outcome["start"] is True and outcome["reason"] == "drift" and outcome["run"] == "run-1"
    ((name, description, parameters),) = pipelines.started
    assert name == "drift-v3-2026111012" and "5000 requests" in description
    assert parameters["trigger"] == "drift" and parameters["train_days"] == 36
    assert parameters["validation_days"] == 14 and parameters["lag_days"] == 1


def test_one_drifting_window_starts_nothing(monkeypatch):
    """Drift in only one window starts no run."""
    answers(monkeypatch, "drifting", "ok")
    pipelines = Runs()
    outcome = drift_check.check("http://drift", pipelines, Policy(), NOW)
    assert outcome["start"] is False and outcome["reason"] == "none" and pipelines.started == []


def test_an_old_model_starts_an_age_run_named_for_the_rule(monkeypatch):
    """A model older than the ceiling starts a run named for the age rule, without drift."""
    answers(monkeypatch, "insufficient", "insufficient", created=NOW - timedelta(days=20))
    pipelines = Runs()
    outcome = drift_check.check("http://drift", pipelines, Policy(), NOW)
    assert outcome["reason"] == "age"
    assert pipelines.started[0][0] == "drift-age-v3-2026111012"


def test_an_active_run_stops_the_check(monkeypatch):
    """A drift run still running means no second one."""
    answers(monkeypatch, "drifting", "drifting")
    busy = Runs([{"name": "drift-v3-x", "state": "RUNNING", "created": NOW - timedelta(hours=30)}])
    outcome = drift_check.check("http://drift", busy, Policy(), NOW)
    assert outcome["reason"] == "active" and busy.started == []


def test_the_api_is_read_for_the_requested_window(monkeypatch):
    """fetch asks the API for the window and decodes its answer."""
    import io
    import json

    seen = []

    def urlopen(url, timeout):
        seen.append(url)
        return io.BytesIO(json.dumps({"version": "v3"}).encode())

    monkeypatch.setattr(drift_check.urllib.request, "urlopen", urlopen)
    assert drift_check.fetch("http://drift/", "6h") == {"version": "v3"}
    assert seen == ["http://drift/v1/drift?window=6h"]


def test_main_prints_the_outcome(monkeypatch, capsys):
    """main reads the environment, checks once and prints the outcome as JSON."""
    answers(monkeypatch, "ok", "ok")
    monkeypatch.setenv("DRIFT_URL", "http://drift")
    monkeypatch.setattr(drift_check, "Pipelines", lambda: Runs())
    drift_check.main()
    assert '"reason": "none"' in capsys.readouterr().out
