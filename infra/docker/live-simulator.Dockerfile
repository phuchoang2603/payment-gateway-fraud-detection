FROM ghcr.io/astral-sh/uv:0.13.1 AS uv
FROM python:3.14-slim-bookworm
COPY --from=uv /uv /usr/local/bin/uv
WORKDIR /app
COPY src/data-pipeline/simulator/pyproject.toml src/data-pipeline/simulator/uv.lock ./
RUN uv sync --locked --no-dev --no-install-project
COPY src/data-pipeline/simulator/simulator ./simulator
COPY src/data-pipeline/simulator/configs/historical-smoke.yaml ./configs/historical-smoke.yaml
COPY contracts/data-products/historical-payments /contracts/data-products/historical-payments
COPY contracts/data-products/bronze-payments /contracts/data-products/bronze-payments
COPY contracts/features /contracts/features
ENV PATH="/app/.venv/bin:$PATH" PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 \
    DATA_PRODUCTS_ROOT="/contracts/data-products" \
    PAYMENT_PHYSICAL_SCHEMAS_PATH="/contracts/data-products/historical-payments/physical-schemas-v1.yaml"
RUN uv sync --locked --no-dev && .venv/bin/python -c "import simulator.runtime"
USER 65532:65532
ENTRYPOINT ["simulator"]
