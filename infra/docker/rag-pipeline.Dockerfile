FROM ghcr.io/astral-sh/uv:0.13.1 AS uv
FROM python:3.12.16-slim-bookworm AS build
COPY --from=uv /uv /usr/local/bin/uv
WORKDIR /workspace
COPY src/llm/pyproject.toml src/llm/uv.lock ./
COPY src/llm/common ./common
COPY src/llm/rag_pipeline/pyproject.toml ./rag_pipeline/pyproject.toml
COPY src/llm/rag_serving/pyproject.toml ./rag_serving/pyproject.toml
COPY src/llm/decision/pyproject.toml ./decision/pyproject.toml
COPY src/llm/agent_runtime/pyproject.toml ./agent_runtime/pyproject.toml
RUN uv sync --locked --no-dev --no-editable --package fraud-rag-pipeline

FROM python:3.12.16-slim-bookworm
WORKDIR /app
COPY --from=build /workspace/.venv /app/.venv
COPY src/llm/rag_pipeline/fraud_rag_pipeline ./fraud_rag_pipeline
COPY openspec/specs ./sources/openspec/specs
COPY contracts/payments/v1 ./sources/contracts/payments/v1
COPY contracts/features/fraud-snapshot-v2.yaml ./sources/contracts/features/fraud-snapshot-v2.yaml
COPY docs/payment-gateway/flows.md ./sources/docs/payment-gateway/flows.md
COPY docs/data/products/silver-payments ./sources/docs/data/products/silver-payments
ENV PATH="/app/.venv/bin:$PATH" PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 HOME=/tmp SOURCES_ROOT=/app/sources
USER 65532:65532
ENTRYPOINT ["python", "-m", "fraud_rag_pipeline"]
