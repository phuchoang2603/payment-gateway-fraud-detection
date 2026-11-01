import json
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from botocore.exceptions import ClientError

from fraud_feast import apply, history, lake, materialize, verify_online

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


def test_report_then_gate_marks_the_unit_passed(monkeypatch, tmp_path):
    seed, report = tmp_path / "seed.json", tmp_path / "report.json"
    seed.write_text(json.dumps({"done": True}))
    monkeypatch.setenv("FEATURE_EPOCH", "history-ci")
    monkeypatch.setenv("HISTORY_SEED_REPORT", str(seed))
    monkeypatch.setenv("RUN_REPORT", str(report))
    monkeypatch.setattr(
        history, "rest", flink({"/jobs/overview": {"jobs": [{"jid": "j", "state": "FINISHED"}]}})
    )
    monkeypatch.setattr(history, "counters", lambda job: {"decode_rejects": 0, "job": job})
    history.HistoryReplay("2026-08-01", "2026-08-08").report(12)
    history.main()
    result = json.loads(report.read_text())
    assert result["gate"] is True and result["rows"] == 12 and result["counts"]["job"] == "j"


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


def test_apply_creates_tables_and_registers_definitions(monkeypatch):
    calls = []
    monkeypatch.setattr(apply, "catalog", lambda: "lake")
    monkeypatch.setattr(apply, "ensure_tables", lambda catalog: calls.append(("tables", catalog)))
    monkeypatch.setattr(apply, "definitions", lambda: ["view"])
    monkeypatch.setattr(
        apply,
        "store",
        lambda: SimpleNamespace(apply=lambda objects: calls.append(("apply", objects))),
    )
    apply.main()
    assert calls == [("tables", "lake"), ("apply", ["view"])]


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
