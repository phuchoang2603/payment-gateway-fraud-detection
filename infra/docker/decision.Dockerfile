FROM ghcr.io/astral-sh/uv:0.13.1 AS uv
FROM python:3.12.16-slim-bookworm AS build
COPY --from=uv /uv /usr/local/bin/uv
WORKDIR /workspace
COPY src/llm/pyproject.toml src/llm/uv.lock ./
COPY src/llm/common/pyproject.toml ./common/pyproject.toml
COPY src/llm/rag_pipeline/pyproject.toml ./rag_pipeline/pyproject.toml
COPY src/llm/rag_serving/pyproject.toml ./rag_serving/pyproject.toml
COPY src/llm/decision/pyproject.toml ./decision/pyproject.toml
COPY src/llm/agent_runtime/pyproject.toml ./agent_runtime/pyproject.toml
RUN uv sync --locked --no-dev --no-editable --package fraud-decision

FROM python:3.12.16-slim-bookworm
WORKDIR /app
COPY --from=build /workspace/.venv /app/.venv
COPY src/llm/decision/fraud_decision ./fraud_decision
ENV PATH="/app/.venv/bin:$PATH" PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 HOME=/tmp HF_HUB_OFFLINE=1
USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["python", "-m", "fraud_decision"]
