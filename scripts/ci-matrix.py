#!/usr/bin/env python3
import argparse
import json
import re
import subprocess
import tomllib
from pathlib import Path, PurePosixPath

MANIFESTS = {"go": "go.mod", "python": "pyproject.toml"}
JVM_PACKAGES = {"pyspark", "apache-flink"}
IVY_PACKAGES = {"pyspark"}


def tracked(manifest):
    output = subprocess.check_output(
        ["git", "ls-files", "-z", "--", f"src/**/{manifest}"]
    ).decode()
    return [path for path in output.split("\0") if path]


def declared(pyproject):
    data = tomllib.loads(Path(pyproject).read_text())
    requirements = list(data.get("project", {}).get("dependencies", []))
    for group in data.get("dependency-groups", {}).values():
        requirements += [item for item in group if isinstance(item, str)]
    return {
        re.split(r"[\s<>=!~\[;]", requirement, maxsplit=1)[0].lower()
        for requirement in requirements
    }


def entry(manifest, kind):
    project = str(PurePosixPath(manifest).parent)
    item = {"project": project, "id": project.replace("/", "-")}
    if kind == "python":
        packages = declared(manifest)
        item["java"] = bool(packages & JVM_PACKAGES)
        item["ivy"] = bool(packages & IVY_PACKAGES)
    return item


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--kind", choices=sorted(MANIFESTS), required=True)
    kind = parser.parse_args().kind
    projects = [
        entry(manifest, kind) for manifest in tracked(MANIFESTS[kind]) if manifest
    ]
    if not projects:
        raise SystemExit(f"No tracked {kind} projects found")
    print(
        json.dumps(
            {"include": sorted(projects, key=lambda item: item["project"])},
            separators=(",", ":"),
        )
    )


if __name__ == "__main__":
    main()
