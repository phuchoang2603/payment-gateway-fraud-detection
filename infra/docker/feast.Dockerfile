FROM ghcr.io/astral-sh/uv:0.13.1 AS uv
FROM python:3.11.16-slim-bookworm AS build
COPY --from=uv /uv /usr/local/bin/uv
WORKDIR /app
COPY src/data-pipeline/feast/pyproject.toml src/data-pipeline/feast/uv.lock ./
RUN uv sync --locked --no-dev --no-install-project

FROM python:3.11.16-slim-bookworm
WORKDIR /app
COPY --from=build /app/.venv /app/.venv
COPY src/data-pipeline/feast/fraud_feast ./fraud_feast
ENV PATH="/app/.venv/bin:$PATH" PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 HOME=/tmp
USER 65532:65532
ENTRYPOINT ["python", "-m", "fraud_feast.runtime"]
