"""Tests for the object storage helpers, with the S3 client replaced by a recorder."""

from types import SimpleNamespace

from risk_model import objects


def client(**calls):
    """Returns a fake S3 client whose methods are the given callables."""
    return SimpleNamespace(**calls)


def test_a_uri_splits_into_a_bucket_and_a_key():
    """split drops the scheme and the leading slash."""
    assert objects.split("s3://bucket/runs/r1/model.json") == ("bucket", "runs/r1/model.json")


def test_downloading_writes_to_the_given_path_from_the_uris_bucket_and_key(monkeypatch, tmp_path):
    """download fetches a bucket and key and returns the local path as text."""
    seen = []
    monkeypatch.setattr(objects, "s3", lambda: client(download_file=lambda *a: seen.append(a)))
    target = tmp_path / "model.json"
    assert objects.download("s3://bucket/runs/r1/model/model.json", target) == str(target)
    assert seen == [("bucket", "runs/r1/model/model.json", str(target))]


def test_an_upload_goes_to_the_bucket_and_key_of_the_uri(monkeypatch):
    """upload splits an `s3://` URI into a bucket and key."""
    sent = []
    monkeypatch.setattr(objects, "s3", lambda: client(upload_file=lambda *a: sent.append(a)))
    objects.upload("/tmp/model.json", "s3://bucket/runs/r1/model/model.json")
    assert sent == [("/tmp/model.json", "bucket", "runs/r1/model/model.json")]


def test_text_is_written_with_its_content_type_and_read_back(monkeypatch):
    """write_text encodes the text with a content type, and read_text decodes an object's body."""
    puts = []

    class Body:
        """A response body."""

        def read(self):
            """Returns the stored bytes."""
            return "ünï".encode()

    fake = client(put_object=lambda **kw: puts.append(kw), get_object=lambda **kw: {"Body": Body()})
    monkeypatch.setattr(objects, "s3", lambda: fake)
    objects.write_text("s3://bucket/runs/r1/pr_curve.html", "<svg/>", "text/html")
    assert puts == [
        {
            "Bucket": "bucket",
            "Key": "runs/r1/pr_curve.html",
            "Body": b"<svg/>",
            "ContentType": "text/html",
        }
    ]
    assert objects.read_text("s3://bucket/runs/r1/pr_curve.html") == "ünï"
