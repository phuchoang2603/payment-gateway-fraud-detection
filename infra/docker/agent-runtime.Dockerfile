FROM ghcr.io/astral-sh/uv:0.13.1 AS uv
# Bookworm, like kagent's own runtime images: Substrate restores checkpoints across workers with the same glibc.
FROM python:3.12.16-slim-bookworm AS build
COPY --from=uv /uv /usr/local/bin/uv
RUN apt-get update && apt-get install -y --no-install-recommends git && rm -rf /var/lib/apt/lists/*
WORKDIR /workspace
COPY src/llm/pyproject.toml src/llm/uv.lock ./
COPY src/llm/common/pyproject.toml ./common/pyproject.toml
COPY src/llm/rag_pipeline/pyproject.toml ./rag_pipeline/pyproject.toml
COPY src/llm/rag_serving/pyproject.toml ./rag_serving/pyproject.toml
COPY src/llm/decision/pyproject.toml ./decision/pyproject.toml
COPY src/llm/agent_runtime ./agent_runtime
RUN uv sync --locked --no-dev --no-editable --package fraud-agent-runtime

FROM python:3.12.16-slim-bookworm
# kagent materializes the compiled agent configuration into /config at start.
RUN mkdir /config && chown 65532:65532 /config
WORKDIR /app
COPY --from=build /workspace/.venv /app/.venv
ENV PATH="/app/.venv/bin:$PATH" PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 HOME=/tmp LANG=C.UTF-8
USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["/app/.venv/bin/python", "-m", "fraud_agent_runtime", "--host", "0.0.0.0", "--port", "8080"]
