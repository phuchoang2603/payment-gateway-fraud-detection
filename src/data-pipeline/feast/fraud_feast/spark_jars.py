import argparse
import hashlib
import json
import tempfile
from pathlib import Path
from urllib.request import urlopen


def manifest():
    return json.loads(Path(__file__).with_suffix(".json").read_text())


def install(directory):
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    pins = manifest()
    for jar in pins["jars"]:
        version = pins["versions"][jar["version"]]
        artifact = jar["artifact"]
        filename = f"{artifact}-{version}.jar"
        url = (
            "https://repo.maven.apache.org/maven2/"
            f"{jar['group'].replace('.', '/')}/{artifact}/{version}/{filename}"
        )
        temporary = None
        try:
            with (
                urlopen(url, timeout=120) as response,
                tempfile.NamedTemporaryFile(dir=directory, delete=False) as output,
            ):
                temporary = Path(output.name)
                digest = hashlib.sha256()
                while chunk := response.read(1024 * 1024):
                    digest.update(chunk)
                    output.write(chunk)
            if digest.hexdigest() != jar["sha256"]:
                raise ValueError(f"checksum mismatch for {filename}")
            temporary.chmod(0o644)
            temporary.replace(directory / filename)
        finally:
            if temporary is not None:
                temporary.unlink(missing_ok=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("directory", type=Path)
    install(parser.parse_args().directory)


if __name__ == "__main__":
    main()
