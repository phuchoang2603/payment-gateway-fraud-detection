import json
from datetime import UTC, date, datetime
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from botocore.exceptions import ClientError

from fraud_feast import history, lake, materialize, verify_online

LAKE_ENV = {
    "ICEBERG_REST_URI": "http://catalog:8181/api/catalog",
    "ICEBERG_WAREHOUSE": "lakehouse",
    "ICEBERG_CLIENT_ID": "id",
    "ICEBERG_CLIENT_SECRET": "secret",
    "ICEBERG_OAUTH2_URI": "http://catalog:8181/api/catalog/v1/oauth/tokens",
    "S3_ENDPOINT": "http://minio:9000",
    "AWS_ACCESS_KEY_ID": "key",
    "AWS_SECRET_ACCESS_KEY": "secret",
}


@pytest.fixture
def lake_env(monkeypatch):
    for name, value in LAKE_ENV.items():
        monkeypatch.setenv(name, value)
    return monkeypatch


# Partitions of the storage addressing: path-style for MinIO, virtual-hosted for S3.
@pytest.mark.parametrize(
    ("path_style", "virtual", "addressing"),
    [("true", "false", "path"), ("false", "true", "virtual")],
)
def test_catalog_and_client_follow_the_addressing_style(lake_env, path_style, virtual, addressing):
    lake_env.setenv("S3_PATH_STYLE", path_style)
    loaded = {}
    session = SimpleNamespace(
        headers={"X-Iceberg-Access-Delegation": "vended-credentials", "Other": "kept"}
    )

    def load_catalog(name, **properties):
        loaded.update(properties, name=name)
        return SimpleNamespace(_session=session)

    lake_env.setattr(lake, "load_catalog", load_catalog)
    lake.catalog()
    assert loaded["name"] == "lake" and loaded["s3.force-virtual-addressing"] == virtual
    assert session.headers == {"Other": "kept"}
    assert lake.s3().meta.config.s3["addressing_style"] == addressing


def test_json_round_trips_through_files_and_reports_missing(tmp_path):
    target = tmp_path / "runs" / "report.json"
    assert lake.read_json(str(target)) is None
    lake.write_json(str(target), {"rows": 3, "at": datetime(2026, 8, 1, tzinfo=UTC)})
    assert lake.read_json(str(target)) == {"rows": 3, "at": "2026-08-01 00:00:00+00:00"}
    assert not target.with_suffix(".pending").exists()


def test_json_round_trips_through_object_storage(monkeypatch):
    stored = {}
    client = MagicMock()
    client.exceptions.NoSuchKey = KeyError
    client.put_object.side_effect = lambda **kwargs: stored.update({kwargs["Key"]: kwargs["Body"]})

    def get_object(Bucket, Key):
        if Key not in stored:
            raise KeyError(Key)
        return {"Body": SimpleNamespace(read=lambda: stored[Key])}

    client.get_object.side_effect = get_object
    monkeypatch.setattr(lake, "s3", lambda: client)
    assert lake.read_json("s3://bucket/runs/missing.json") is None
    lake.write_json("s3://bucket/runs/report.json", {"gate": True})
    assert stored == {"runs/report.json": b'{"gate": true}'}
    assert lake.read_json("s3://bucket/runs/report.json") == {"gate": True}


def flink(responses):
    def rest(path):
        for prefix, value in responses.items():
            if path.startswith(prefix):
                return value
        raise AssertionError(path)

    return rest


# Partitions of the Flink overview: one finished job, one running job, none, and two.
@pytest.mark.parametrize(
    ("jobs", "finished"),
    [
        ([{"jid": "a", "state": "FINISHED"}], True),
        ([{"jid": "a", "state": "RUNNING"}], False),
        ([], False),
        ([{"jid": "a", "state": "FINISHED"}, {"jid": "b", "state": "FINISHED"}], False),
    ],
)
def test_finished_job_requires_exactly_one_finished_job(monkeypatch, jobs, finished):
    monkeypatch.setattr(history, "rest", flink({"/jobs/overview": {"jobs": jobs}}))
    if finished:
        assert history.finished_job() == "a"
    else:
        with pytest.raises(ValueError):
            history.finished_job()


def test_counters_sum_every_vertex_and_classify_decode_rejects(monkeypatch):
    metrics = [
        {"id": "0.duplicates_dropped", "sum": "4"},
        {"id": "0.too_late", "sum": "2.0"},
        {"id": "0.reject_class.undecodable.rejected", "sum": "1"},
        {"id": "0.reject_class.conflicting_duplicate.rejected", "sum": "3"},
    ]
    names = [{"id": metric["id"]} for metric in metrics] + [{"id": "0.numRecordsIn"}]
    monkeypatch.setattr(
        history,
        "rest",
        flink(
            {
                "/jobs/j/vertices/v1/subtasks/metrics?": metrics,
                "/jobs/j/vertices/v1/subtasks/metrics": names,
                "/jobs/j/vertices/v2/subtasks/metrics": [{"id": "0.numRecordsIn"}],
                "/jobs/j": {"vertices": [{"id": "v1"}, {"id": "v2"}]},
            }
        ),
    )
    assert history.counters("j") == {
        "duplicates": 4,
        "too_late": 2,
        "rejects": 4,
        "decode_rejects": 1,
    }


# Boundary values of a history range: one day, empty, reversed, and a live epoch.
@pytest.mark.parametrize(
    ("epoch", "start", "end", "valid"),
    [
        ("history-ci", "2026-08-01", "2026-08-02", True),
        ("history-ci", "2026-08-01", "2026-08-01", False),
        ("history-ci", "2026-08-02", "2026-08-01", False),
        ("live-ci", "2026-08-01", "2026-08-02", False),
    ],
)
def test_history_bounds(monkeypatch, epoch, start, end, valid):
    monkeypatch.setenv("FEATURE_EPOCH", epoch)
    if valid:
        assert history.bounds(start, end) == (
            datetime(2026, 8, 1, tzinfo=UTC),
            datetime(2026, 8, 2, tzinfo=UTC),
        )
    else:
        with pytest.raises(ValueError):
            history.bounds(start, end)


def replay(monkeypatch, tmp_path, *, seed, rejects=0, state="FINISHED"):
    seed_path, report = tmp_path / "seed.json", tmp_path / "report.json"
    seed_path.write_text(json.dumps(seed))
    monkeypatch.setenv("FEATURE_EPOCH", "history-ci")
    monkeypatch.setenv("FEATURE_UPDATE_TOPIC", "fraud.feature-updates.v2.history-ci")
    monkeypatch.setenv("HISTORY_SEED_REPORT", str(seed_path))
    monkeypatch.setenv("RUN_REPORT", str(report))
    monkeypatch.setattr(
        history, "rest", flink({"/jobs/overview": {"jobs": [{"jid": "j", "state": state}]}})
    )
    monkeypatch.setattr(history, "counters", lambda job: {"decode_rejects": rejects, "job": job})
    return history.HistoryReplay("2026-08-01", "2026-08-08"), report


def test_check_then_report_records_the_unit(monkeypatch, tmp_path):
    unit, report = replay(monkeypatch, tmp_path, seed={"done": True})
    unit.check()
    unit.report(12)
    result = json.loads(report.read_text())
    assert (
        result["rows"] == 12 and result["counts"]["job"] == "j" and result["seed"] == {"done": True}
    )


def test_verified_unit_survives_flink_endpoint_loss(monkeypatch, tmp_path):
    unit, report = replay(monkeypatch, tmp_path, seed={"done": True})
    unit.check()
    monkeypatch.setattr(history, "rest", lambda _: pytest.fail("verified replay queried Flink"))
    unit.check()
    unit.report(12)
    assert json.loads(report.read_text())["rows"] == 12


@pytest.mark.parametrize("changed", ["seed", "topic", "range", "epoch"])
def test_verification_cannot_be_reused_for_different_inputs(monkeypatch, tmp_path, changed):
    unit, _ = replay(monkeypatch, tmp_path, seed={"done": True})
    unit.check()
    if changed == "seed":
        (tmp_path / "seed.json").write_text(json.dumps({"done": True, "snapshot": "new"}))
    elif changed == "topic":
        monkeypatch.setenv("FEATURE_UPDATE_TOPIC", "fraud.feature-updates.v2.other")
    elif changed == "epoch":
        monkeypatch.setenv("FEATURE_EPOCH", "history-other")
    else:
        unit = history.HistoryReplay("2026-08-02", "2026-08-08")
    with pytest.raises(ValueError, match="does not match"):
        unit.check()


@pytest.mark.parametrize(
    "broken",
    [
        {"seed": {"done": False}},
        {"seed": {"done": True}, "rejects": 1},
        {"seed": {"done": True}, "state": "RUNNING"},
    ],
)
def test_check_refuses_an_incomplete_seed_decode_rejects_or_an_unfinished_job(
    monkeypatch, tmp_path, broken
):
    unit, report = replay(monkeypatch, tmp_path, **broken)
    with pytest.raises(ValueError):
        unit.check()
    assert not report.exists()


def online_response(names, values, timestamps):
    from feast.protos.feast.serving.ServingService_pb2 import GetOnlineFeaturesResponse
    from feast.protos.feast.types.Value_pb2 import Value

    proto = GetOnlineFeaturesResponse()
    proto.metadata.feature_names.val.extend(names)
    for value, timestamp in zip(values, timestamps):
        result = proto.results.add()
        result.values.append(Value(int64_val=value) if value is not None else Value())
        result.event_timestamps.add().FromDatetime(timestamp)
    return SimpleNamespace(proto=proto)


AT = datetime(2026, 8, 1, tzinfo=UTC)


def sampled(monkeypatch, value, timestamp, *, names=None):
    monkeypatch.setattr(verify_online, "DAILY_FIELDS", {"customer": ["attempt_count_7d"]})
    store = MagicMock()
    names = names or ["customer_daily__attempt_count_7d"]
    store.get_online_features.return_value = online_response(names, [value], [timestamp])
    row = {
        "integration_id": "i",
        "customer_id": "c",
        "attempt_count_7d": 3,
        "event_timestamp": AT.isoformat(),
    }
    return store, {"samples": {"customer": [row]}}


# Partitions of an online read: equal, a different value, a different time, and a missing column.
@pytest.mark.parametrize(
    ("value", "timestamp", "names", "error"),
    [
        (3, AT, None, None),
        (4, AT, None, "value differs"),
        (3, datetime(2026, 8, 2, tzinfo=UTC), None, "event time"),
        (3, AT, ["customer_daily__other"], "omitted"),
    ],
)
def test_verify_online_compares_every_sampled_value(monkeypatch, value, timestamp, names, error):
    store, report = sampled(monkeypatch, value, timestamp, names=names)
    if error:
        with pytest.raises(ValueError, match=error):
            verify_online.validate(store, report)
    else:
        assert verify_online.validate(store, report) == 1
        _, kwargs = store.get_online_features.call_args
        assert kwargs["entity_rows"] == [
            {"integration_id": "i", "customer_id": "c", "payment_method_fingerprint": ""}
        ]


def test_verify_online_main_records_the_check(monkeypatch, tmp_path):
    report = tmp_path / "report.json"
    monkeypatch.setenv("RUN_REPORT", str(report))
    with pytest.raises(ValueError, match="missing"):
        verify_online.main()
    report.write_text(json.dumps({"samples": {}}))
    monkeypatch.setattr(verify_online, "store", lambda **_: MagicMock())
    verify_online.main()
    assert json.loads(report.read_text()) == {"samples": {}, "sampled": 0, "checked": True}


# Partitions of the materialization flags: daily with and without a since day, stream recovery, and both.
@pytest.mark.parametrize(
    ("argv", "expected"),
    [
        ([], {"since": None, "stream_recovery": False}),
        (["--since", "2026-07-31"], {"since": "2026-07-31", "stream_recovery": False}),
        (["--stream-recovery"], {"since": None, "stream_recovery": True}),
        (["--stream-recovery", "--since", "2026-07-31"], None),
    ],
)
def test_materialize_main_parses_its_flags(monkeypatch, tmp_path, argv, expected):
    import sys

    report = tmp_path / "report.json"
    monkeypatch.setenv("RUN_REPORT", str(report))
    monkeypatch.setattr(sys, "argv", ["materialize", *argv])
    seen = {}

    def run(spark, feature_store, now, lookback, since, *, stream_recovery):
        seen.update(since=str(since) if since else None, stream_recovery=stream_recovery)
        return {"windows": {}}

    session = MagicMock()
    builder = MagicMock()
    builder.config.return_value.getOrCreate.return_value = session
    monkeypatch.setattr("pyspark.sql.SparkSession.builder", builder)
    monkeypatch.setattr(materialize, "store", lambda: MagicMock())
    monkeypatch.setattr(materialize, "run", run)
    if expected is None:
        with pytest.raises(SystemExit):
            materialize.main()
        return
    materialize.main()
    assert seen == expected
    assert json.loads(report.read_text()) == {"windows": {}}
    session.stop.assert_called_once()


def test_materialize_rejects_a_negative_lookback():
    with pytest.raises(ValueError, match="lookback"):
        materialize.run(None, MagicMock(), AT, -1)


def test_registry_store_raises_other_client_errors():
    from fraud_feast.registry import PrefixS3RegistryStore

    store = PrefixS3RegistryStore.__new__(PrefixS3RegistryStore)
    store._bucket, store._key, store._uri = "bucket", "key", MagicMock()
    store.s3_client = MagicMock()
    store.s3_client.Object.return_value.get.side_effect = ClientError(
        {"Error": {"Code": "AccessDenied"}}, "GetObject"
    )
    with pytest.raises(ClientError):
        store.get_registry_proto()


# The two windows the Spark test does not reach: an explicit since day, and stream recovery (STREAM_TTL back).
@pytest.mark.parametrize(
    ("since", "recovery", "start", "views"),
    [
        (date(2026, 7, 20), False, datetime(2026, 7, 20, tzinfo=UTC), ["customer_daily"]),
        (None, True, AT - materialize.STREAM_TTL, ["customer_stream", "payment_method_stream"]),
    ],
)
def test_materialize_windows(monkeypatch, since, recovery, start, views):
    feature_store = MagicMock()
    # Stop after the first daily materialization: the sampling that follows needs a live Spark session.
    feature_store.materialize.side_effect = None if recovery else StopIteration
    if recovery:
        report = materialize.run(None, feature_store, AT, 2, since, stream_recovery=True)
        assert {window["start"] for window in report["windows"].values()} == {start.isoformat()}
    else:
        with pytest.raises(StopIteration):
            materialize.run(None, feature_store, AT, 2, since)
    assert feature_store.config.online_store.skip_dedup is not recovery
    feature_store.materialize.assert_called_once_with(start, AT, feature_views=views)
