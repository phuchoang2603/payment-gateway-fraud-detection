import hashlib
import io
from importlib.metadata import version
from pathlib import Path

import pytest

from fraud_feast import spark_jars


def test_runtime_versions_match_installed_spark_and_image():
    pins = spark_jars.manifest()
    assert version("pyspark") == pins["versions"]["spark"]
    dockerfile = Path(__file__).resolve().parents[4] / "infra/docker/data-platform-spark.Dockerfile"
    assert f"apache/spark:{pins['versions']['spark']}-" in dockerfile.read_text()


@pytest.mark.parametrize("valid", [True, False])
def test_download_checks_integrity_before_replacing_a_jar(tmp_path, monkeypatch, valid):
    data = b"jar contents"
    pins = {
        "versions": {"iceberg": "1.12.0"},
        "jars": [
            {
                "group": "org.apache.iceberg",
                "artifact": "iceberg-aws-bundle",
                "version": "iceberg",
                "sha256": hashlib.sha256(data if valid else b"other").hexdigest(),
            }
        ],
    }
    monkeypatch.setattr(spark_jars, "manifest", lambda: pins)
    monkeypatch.setattr(spark_jars, "urlopen", lambda *args, **kwargs: io.BytesIO(data))
    target = tmp_path / "iceberg-aws-bundle-1.12.0.jar"
    target.write_bytes(b"previous jar")
    if valid:
        spark_jars.install(tmp_path)
        assert target.read_bytes() == data
        assert target.stat().st_mode & 0o777 == 0o644
    else:
        with pytest.raises(ValueError, match="checksum mismatch"):
            spark_jars.install(tmp_path)
        assert target.read_bytes() == b"previous jar"
    assert list(tmp_path.iterdir()) == [target]
