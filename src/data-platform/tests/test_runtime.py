from data_platform import ingest, spark
from data_platform.runs import open_run
from data_platform.storage import Store


def test_preprovisioned_namespace_does_not_request_namespace_creation(tmp_path, monkeypatch):
    class Catalog:
        def databaseExists(self, name):
            assert name == "lake.bronze"
            return True

        def tableExists(self, name):
            return True

    class Session:
        catalog = Catalog()

        def sql(self, query):
            raise AssertionError("preprovisioned namespace must not be mutated")

    monkeypatch.setattr(ingest, "verify_files", lambda *args: None)
    monkeypatch.setattr(ingest, "source_frames", lambda *args: {"payment_events": None, "fraud_labels": None})
    monkeypatch.setattr(ingest, "committed_snapshot", lambda *args: 1)
    store = Store(str(tmp_path))
    run = open_run(store, "candidate", "digest", {}, "image", "workflow")
    ingest.ingest(Session(), "source", {}, store, run)
    assert run["snapshots"] == {"payment_events": 1, "fraud_labels": 1}


def test_rest_signing_and_source_prefix_access_do_not_require_root_bucket_listing(monkeypatch):
    class Context:
        def setLogLevel(self, level):
            pass

    class Builder:
        sparkContext = Context()

        def __init__(self):
            self.options = {}

        def appName(self, name):
            return self

        def config(self, key, value):
            self.options[key] = value
            return self

        def getOrCreate(self):
            return self

    builder = Builder()
    monkeypatch.setattr(spark, "SparkSession", type("SparkSession", (), {"builder": builder}))
    for name, value in {
        "ICEBERG_REST_URI": "https://minio.home.phuchoang.sbs/iceberg",
        "SOURCE_ACCESS_KEY_ID": "runtime-key",
        "SOURCE_SECRET_ACCESS_KEY": "runtime-secret",
        "LAKE_ACCESS_KEY_ID": "runtime-key",
        "LAKE_SECRET_ACCESS_KEY": "runtime-secret",
    }.items():
        monkeypatch.setenv(name, value)
    monkeypatch.delenv("ICEBERG_WAREHOUSE", raising=False)
    spark.session()
    assert builder.options["spark.sql.catalog.lake.warehouse"] == "fraud-lakehouse-prod"
    assert builder.options["spark.sql.catalog.lake.rest.access-key-id"] == "runtime-key"
    assert builder.options["spark.sql.catalog.lake.s3.access-key-id"] == "runtime-key"
    assert builder.options["spark.hadoop.fs.s3a.bucket.synthetic-source.bucket.probe"] == "0"
    assert not any(k.startswith("spark.hadoop.fs.s3a.bucket.probe") for k in builder.options)
