import io
import subprocess
import tarfile
from pathlib import Path

import pytest
from testcontainers.core.container import DockerContainer
from testcontainers.core.image import DockerImage

ROOT = Path(__file__).resolve().parents[4]


@pytest.mark.parametrize(
    "name,python",
    [
        ("feast", "/app/.venv/bin/python"),
        ("data-platform-spark", "/opt/data-pipeline/feast/.venv/bin/python"),
    ],
)
def test_runtime_image_build_and_packaged_repository(name, python):
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
    context.seek(0)
    with (
        context,
        DockerImage(
            path=ROOT,
            fileobj=context,
            custom_context=True,
            dockerfile_path=f"infra/docker/{name}.Dockerfile",
        ) as image,
    ):
        container = DockerContainer(str(image)).with_kwargs(
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
