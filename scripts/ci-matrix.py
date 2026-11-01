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
# A change here runs every project.
SHARED = ["contracts/", "scripts/", ".github/workflows/"]


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


def resolve(project, path):
    return PurePosixPath(Path(project, path).resolve().relative_to(Path.cwd()))


def workspace(project):
    """The uv workspace root that contains the project, and its members by package name."""
    for root in PurePosixPath(project).parents:
        manifest = Path(root, "pyproject.toml")
        members = (
            tomllib.loads(manifest.read_text())
            .get("tool", {})
            .get("uv", {})
            .get("workspace", {})
            .get("members", [])
            if manifest.exists()
            else []
        )
        if members:
            names = {
                tomllib.loads(Path(root, member, "pyproject.toml").read_text())[
                    "project"
                ]["name"]: root / member
                for member in members
            }
            return root, names
    return None, {}


def local_dependencies(project, kind):
    """Paths outside the project that its manifest declares: Go replace directories, uv path and workspace sources
    with the workspace's lock, and pytest import paths."""
    if kind == "go":
        return {
            resolve(project, path)
            for path in re.findall(
                r"=>\s*(\.\.?/\S+)", Path(project, "go.mod").read_text()
            )
        }
    tool = tomllib.loads(Path(project, "pyproject.toml").read_text()).get("tool", {})
    paths = {
        resolve(project, path)
        for path in tool.get("pytest", {}).get("ini_options", {}).get("pythonpath", [])
    }
    root, members = workspace(project)
    if root:
        paths |= {root / "pyproject.toml", root / "uv.lock"}
    for name, source in tool.get("uv", {}).get("sources", {}).items():
        if "path" in source:
            paths.add(resolve(project, source["path"]))
        elif source.get("workspace") and name in members:
            paths.add(members[name])
    # The project itself, or an ancestor on its import path, is not an outside dependency.
    own = PurePosixPath(project)
    return {path for path in paths if path != own and path not in own.parents}


def affected(project, kind, changed):
    roots = [PurePosixPath(project), *local_dependencies(project, kind)]
    return any(
        file.startswith(tuple(SHARED))
        or any(PurePosixPath(file).is_relative_to(root) for root in roots)
        for file in changed
    )


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
    parser.add_argument(
        "--changed", metavar="BASE", help="keep only the projects changed since BASE"
    )
    args = parser.parse_args()
    manifests = tracked(MANIFESTS[args.kind])
    if not manifests:
        raise SystemExit(f"No tracked {args.kind} projects found")
    changed = None
    if args.changed:
        changed = (
            subprocess.check_output(
                ["git", "diff", "--name-only", f"{args.changed}...HEAD"]
            )
            .decode()
            .split()
        )
    projects = [
        entry(manifest, args.kind)
        for manifest in manifests
        if changed is None
        or affected(str(PurePosixPath(manifest).parent), args.kind, changed)
    ]
    print(
        json.dumps(
            {"include": sorted(projects, key=lambda item: item["project"])},
            separators=(",", ":"),
        )
    )


if __name__ == "__main__":
    main()
