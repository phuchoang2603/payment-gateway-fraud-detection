FROM ghcr.io/astral-sh/uv:0.13.1 AS uv
FROM python:3.14-slim-bookworm
COPY --from=uv /uv /usr/local/bin/uv
WORKDIR /app
COPY src/data-pipeline/simulator/pyproject.toml src/data-pipeline/simulator/uv.lock ./
RUN uv sync --locked --no-dev --no-install-project
COPY src/data-pipeline/simulator/simulator ./simulator
COPY src/data-pipeline/simulator/configs ./configs
COPY contracts/data-products/historical-payments /app/contracts/historical-payments
RUN uv sync --locked --no-dev && .venv/bin/python -c "import simulator.runtime"
ENV PATH="/app/.venv/bin:$PATH" PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
USER 65532:65532
ENTRYPOINT ["simulator"]
