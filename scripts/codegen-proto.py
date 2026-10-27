import re
import subprocess
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CONTRACTS = ROOT / "contracts"


def generate(project, config):
    sources = list(
        dict.fromkeys(
            source
            for pattern in config["sources"]
            for source in sorted(CONTRACTS.glob(pattern))
        )
    )
    if not sources:
        raise ValueError(f"No protobuf sources selected by {project}")
    command = ["protoc", f"-I{CONTRACTS}"]
    if "descriptor" in config:
        output = project / config["descriptor"]
        output.parent.mkdir(parents=True, exist_ok=True)
        subprocess.run(
            [
                *command,
                "--include_imports",
                f"--descriptor_set_out={output}",
                *map(str, sources),
            ],
            check=True,
        )
    if "python-package" in config:
        package = config["python-package"]
        output = project.joinpath(*package.split("."))
        output.mkdir(parents=True, exist_ok=True)
        subprocess.run(
            [
                *command,
                f"--python_out={output}",
                f"--pyi_out={output}",
                *map(str, sources),
            ],
            check=True,
        )
        roots = {source.relative_to(CONTRACTS).parts[0] for source in sources}
        for path in [*output.rglob("*_pb2.py"), *output.rglob("*_pb2.pyi")]:
            directory = path.parent
            while directory.is_relative_to(output):
                (directory / "__init__.py").touch()
                directory = directory.parent
            text = path.read_text()
            for namespace in sorted(roots):
                text = re.sub(
                    rf"\bfrom {re.escape(namespace)}\.",
                    f"from {package}.{namespace}.",
                    text,
                )
                text = text.replace(f"'{namespace}.", f"'{package}.{namespace}.")
            path.write_text(text)


if __name__ == "__main__":
    manifests = (
        subprocess.check_output(
            ["git", "ls-files", "-z", "src/**/pyproject.toml"], cwd=ROOT
        )
        .decode()
        .split("\0")
    )
    for name in filter(None, manifests):
        manifest = ROOT / name
        config = tomllib.loads(manifest.read_text()).get("tool", {}).get("protobuf")
        if config:
            generate(manifest.parent, config)
