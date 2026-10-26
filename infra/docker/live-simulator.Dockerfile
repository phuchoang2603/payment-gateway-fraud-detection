FROM ghcr.io/astral-sh/uv:0.13.1 AS uv
FROM python:3.14-slim-bookworm
COPY --from=uv /uv /usr/local/bin/uv
WORKDIR /app
COPY src/data-pipeline/simulator/pyproject.toml src/data-pipeline/simulator/uv.lock ./
RUN uv sync --locked --no-dev --no-install-project
COPY src/data-pipeline/simulator/simulator ./simulator
RUN uv sync --locked --no-dev
ENV PATH="/app/.venv/bin:$PATH" PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
USER 65532:65532
ENTRYPOINT ["simulator"]
