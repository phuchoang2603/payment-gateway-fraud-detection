#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
export UV_PROJECT_ENVIRONMENT="$PWD/src/data-platform/.venv"
export UV_PYTHON_PREFERENCE=managed
export UV_PYTHON_DOWNLOADS=automatic
uv sync --project src/data-platform --python 3.12 --locked
python3 scripts/prepare-dp1-jars.py "${RUNNER_TEMP:-/tmp}/dp1-jars" --local
export DP1_JARS
DP1_JARS="$(find "${RUNNER_TEMP:-/tmp}/dp1-jars" -name 'iceberg-spark-runtime*.jar' -print -quit)"
uv run --project src/data-platform --python 3.12 --locked ruff check src/data-platform/data_platform src/data-platform/tests
uv run --project src/data-platform --python 3.12 --locked ruff format --check src/data-platform/data_platform src/data-platform/tests
uv run --project src/data-platform --python 3.12 --locked pytest -q src/data-platform/tests
