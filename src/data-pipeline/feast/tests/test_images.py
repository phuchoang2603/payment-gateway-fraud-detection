import io
import os
import subprocess
import tarfile
from pathlib import Path
from uuid import uuid4

import pytest
from testcontainers.core.container import DockerContainer

ROOT = Path(__file__).resolve().parents[4]


@pytest.fixture(
    params=[
        ("feast", "/app/.venv/bin/python"),
        ("data-platform-spark", "/opt/data-pipeline/feast/.venv/bin/python"),
    ],
)
def runtime_image(request):
    name, python = request.param
    context = io.BytesIO()
    paths = (
        subprocess.check_output(
            [
                "git",
                "ls-files",
                "-z",
                "infra/docker",
                "src/data-pipeline/feast",
                "src/data-pipeline/batch",
                "contracts",
            ],
            cwd=ROOT,
        )
        .decode()
        .split("\0")
    )
    with tarfile.open(fileobj=context, mode="w") as archive:
        for path in filter(None, paths):
            archive.add(ROOT / path, arcname=path)
    tag = f"feast-ci-{name}:{uuid4().hex}"
    with context:
        subprocess.run(
            [
                "docker",
                "build",
                "--file",
                f"infra/docker/{name}.Dockerfile",
                "--tag",
                tag,
                "-",
            ],
            input=context.getvalue(),
            env={**os.environ, "DOCKER_BUILDKIT": "1"},
            check=True,
        )
    try:
        yield name, python, tag
    finally:
        subprocess.run(["docker", "image", "rm", tag], check=True)


def test_runtime_image_build_and_packaged_repository(runtime_image):
    name, python, image = runtime_image
    container = DockerContainer(image).with_kwargs(
        entrypoint=[python, "-c", "import time; time.sleep(120)"]
    )
    with container:
        code, output = container.exec(
            [
                python,
                "-c",
                "import importlib.resources, sys; import fraud_feast.push_job, fraud_feast.check_online; assert sys.version_info[:2] == (3,12); assert importlib.resources.files('fraud_feast').joinpath('feature_store.yaml').is_file()",
            ]
        )
        assert code == 0, output.decode()
        code, output = container.exec(
            [
                python,
                "-c",
                f"import importlib.util; assert (importlib.util.find_spec('pyspark') is not None) == {name == 'data-platform-spark'}",
            ]
        )
        assert code == 0, output.decode()
