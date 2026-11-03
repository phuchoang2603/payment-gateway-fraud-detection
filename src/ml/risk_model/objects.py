"""Object storage helpers shared by the steps and the GPU jobs: download, upload and text files."""

from urllib.parse import urlsplit

from fraud_feast.lake import s3


def split(uri):
    """Returns the bucket and key of an `s3://` URI."""
    parts = urlsplit(uri)
    return parts.netloc, parts.path.lstrip("/")


def download(uri, target):
    """Downloads an `s3://` URI to the local path `target` and returns the path as text."""
    bucket, key = split(uri)
    s3().download_file(bucket, key, str(target))
    return str(target)


def upload(path, uri):
    """Uploads a local file to an `s3://` URI."""
    bucket, key = split(uri)
    s3().upload_file(str(path), bucket, key)


def read_text(uri):
    """Returns the text of an `s3://` object."""
    bucket, key = split(uri)
    return s3().get_object(Bucket=bucket, Key=key)["Body"].read().decode()


def write_text(uri, text, content_type="text/plain"):
    """Writes text to an `s3://` URI."""
    bucket, key = split(uri)
    s3().put_object(Bucket=bucket, Key=key, Body=text.encode(), ContentType=content_type)
