from batch import spark


class Recorder:
    def __init__(self):
        self.puts = []

    def put_object(self, **kwargs):
        self.puts.append(kwargs)


def configure(monkeypatch, directory):
    recorder = Recorder()
    clients = []

    def client(service, **kwargs):
        clients.append((service, kwargs))
        return recorder

    monkeypatch.setattr(spark.boto3, "client", client)
    monkeypatch.setenv("S3_ENDPOINT", "https://object-store.test")
    monkeypatch.setenv("S3_PATH_STYLE", "true")
    if directory:
        monkeypatch.setenv("SPARK_EVENT_LOG_DIR", directory)
    else:
        monkeypatch.delenv("SPARK_EVENT_LOG_DIR", raising=False)
    return recorder, clients


def test_event_log_prefix_gets_a_marker_object(monkeypatch):
    recorder, clients = configure(monkeypatch, "s3a://platform-dev/runs/payment-gateway/spark-events/")

    spark.ensure_event_log_prefix()

    assert recorder.puts == [{"Bucket": "platform-dev", "Key": "runs/payment-gateway/spark-events/.keep", "Body": b""}]
    assert clients[0][1]["endpoint_url"] == "https://object-store.test"
    assert clients[0][1]["config"].s3["addressing_style"] == "path"
    assert clients[0][1]["config"].request_checksum_calculation == "when_required"


def test_no_event_log_dir_touches_no_object_store(monkeypatch):
    recorder, clients = configure(monkeypatch, None)

    spark.ensure_event_log_prefix()

    assert recorder.puts == []
    assert clients == []
