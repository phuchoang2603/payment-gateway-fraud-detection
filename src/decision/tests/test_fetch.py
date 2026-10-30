import pytest

from fraud_decision import fetch

PREFIX = "artifacts/models/convaiinnovations/laya/rev"
OBJECTS = {
    f"{PREFIX}/rl_agent_config.json": b"{}",
    f"{PREFIX}/tokenizer/tokenizer.json": b"tok",
    f"{PREFIX}/model.safetensors": b"weights",
}


class FakeS3:
    def __init__(self, objects):
        self.objects = objects
        self.downloads = []

    def get_paginator(self, name):
        assert name == "list_objects_v2"
        return self

    def paginate(self, Bucket, Prefix):
        yield {"Contents": [{"Key": k, "Size": len(v)} for k, v in self.objects.items() if k.startswith(Prefix)]}
        yield {}

    def download_file(self, bucket, key, path):
        self.downloads.append(key)
        with open(path, "wb") as file:
            file.write(self.objects[key])


def test_fetch_copies_the_checkpoint_and_skips_what_is_present(tmp_path):
    client = FakeS3(OBJECTS)
    assert fetch.fetch(client, "bucket", PREFIX, tmp_path) == 3
    assert (tmp_path / "tokenizer" / "tokenizer.json").read_bytes() == b"tok"
    assert fetch.fetch(client, "bucket", PREFIX + "/", tmp_path) == 0
    assert len(client.downloads) == 3


def test_fetch_refuses_a_prefix_without_a_checkpoint(tmp_path):
    with pytest.raises(SystemExit):
        fetch.fetch(FakeS3({f"{PREFIX}/README.md": b"x"}), "bucket", PREFIX, tmp_path)


def test_main_reads_the_environment(tmp_path, monkeypatch, capsys):
    client = FakeS3(OBJECTS)
    monkeypatch.setattr(fetch.boto3, "client", lambda name: client)
    monkeypatch.setenv("MODEL_DIR", str(tmp_path))
    monkeypatch.setenv("MODEL_BUCKET", "bucket")
    monkeypatch.setenv("MODEL_PREFIX", PREFIX)
    fetch.main()
    assert "3 files copied" in capsys.readouterr().out
