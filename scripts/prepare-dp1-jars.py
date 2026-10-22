import argparse
import hashlib
import json
import urllib.request
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("destination", type=Path)
parser.add_argument("--local", action="store_true")
args = parser.parse_args()
lock = json.loads((Path(__file__).parent / "dp1-jars.lock.json").read_text())
args.destination.mkdir(parents=True, exist_ok=True)
for artifact in lock:
    if args.local and not artifact["path"].startswith(
        "org/apache/iceberg/iceberg-spark-runtime"
    ):
        continue
    output = args.destination / artifact["path"].split("/")[-1]
    if not output.exists():
        urllib.request.urlretrieve(
            "https://repo.maven.apache.org/maven2/" + artifact["path"], output
        )
    with output.open("rb") as stream:
        digest = hashlib.file_digest(stream, "sha256").hexdigest()
    if digest != artifact["sha256"]:
        raise ValueError("JAR integrity mismatch: " + output.name)
