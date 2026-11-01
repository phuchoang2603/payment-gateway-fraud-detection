FROM ghcr.io/astral-sh/uv:0.13.1 AS uv
FROM python:3.12.15-slim-bookworm AS build
COPY --from=uv /uv /usr/local/bin/uv
WORKDIR /app
ENV UV_NO_CACHE=1 UV_LINK_MODE=copy UV_PYTHON_DOWNLOADS=0
COPY src/ml/predictor/pyproject.toml src/ml/predictor/uv.lock ./
RUN uv sync --locked --no-dev --no-install-project
COPY src/ml/predictor/payment_risk_predictor ./payment_risk_predictor

FROM python:3.12.15-slim-bookworm
RUN apt-get update && apt-get install -y --no-install-recommends libgomp1 \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=build /app /app
ENV PATH="/app/.venv/bin:$PATH" PYTHONPATH=/app PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 \
    MODEL_DIR=/mnt/models HOME=/tmp
USER 65532:65532
ENTRYPOINT ["python", "-m", "payment_risk_predictor.model"]
CMD ["--model_name=payment-risk"]
