FROM ghcr.io/astral-sh/uv:0.13.1 AS uv
FROM python:3.12.16-slim-bookworm AS build
COPY --from=uv /uv /usr/local/bin/uv
WORKDIR /workspace
COPY src/decision/pyproject.toml src/decision/uv.lock ./
RUN uv sync --locked --no-dev --no-editable

FROM python:3.12.16-slim-bookworm
WORKDIR /app
COPY --from=build /workspace/.venv /app/.venv
COPY src/decision/fraud_decision ./fraud_decision
ENV PATH="/app/.venv/bin:$PATH" PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 HOME=/tmp HF_HUB_OFFLINE=1
USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["python", "-m", "fraud_decision"]
