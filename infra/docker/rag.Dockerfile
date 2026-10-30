FROM ghcr.io/astral-sh/uv:0.13.1 AS uv
FROM python:3.12.16-slim-bookworm AS build
COPY --from=uv /uv /usr/local/bin/uv
WORKDIR /app
COPY src/rag/pyproject.toml src/rag/uv.lock ./
RUN uv sync --locked --no-dev --no-install-project

FROM python:3.12.16-slim-bookworm
WORKDIR /app
COPY --from=build /app/.venv /app/.venv
COPY src/rag/rag ./rag
COPY openspec/specs ./sources/openspec/specs
COPY contracts/payments/v1 ./sources/contracts/payments/v1
COPY contracts/features/fraud-snapshot-v2.yaml ./sources/contracts/features/fraud-snapshot-v2.yaml
COPY docs/payment-gateway/flows.md ./sources/docs/payment-gateway/flows.md
COPY docs/data/products/silver-payments ./sources/docs/data/products/silver-payments
ENV PATH="/app/.venv/bin:$PATH" PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 HOME=/tmp SOURCES_ROOT=/app/sources
USER 65532:65532
ENTRYPOINT ["python", "-m"]
CMD ["rag.service"]
