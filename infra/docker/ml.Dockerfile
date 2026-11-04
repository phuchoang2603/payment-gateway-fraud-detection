FROM ghcr.io/astral-sh/uv:0.13.1 AS uv

FROM python:3.12.15-slim-bookworm AS jars
COPY src/data-pipeline/feast/fraud_feast/spark_jars.py src/data-pipeline/feast/fraud_feast/spark_jars.json /runtime/
RUN python /runtime/spark_jars.py /jars

FROM nvidia/cuda:12.6.3-runtime-ubuntu24.04 AS base
RUN apt-get update && apt-get install -y --no-install-recommends \
        python3.12 python3.12-venv openjdk-17-jre-headless procps tini \
    && rm -rf /var/lib/apt/lists/*

FROM base AS build
COPY --from=uv /uv /usr/local/bin/uv
ENV UV_NO_CACHE=1 UV_LINK_MODE=copy UV_PYTHON_DOWNLOADS=0 UV_PYTHON=/usr/bin/python3.12 UV_PROJECT_ENVIRONMENT=/app/.venv
WORKDIR /src/data-pipeline/feast
COPY src/data-pipeline/feast/pyproject.toml src/data-pipeline/feast/uv.lock ./
COPY src/data-pipeline/feast/fraud_feast ./fraud_feast
WORKDIR /src/ml
COPY src/ml/pyproject.toml src/ml/uv.lock ./
COPY src/ml/risk_model ./risk_model
RUN uv sync --locked --no-dev --no-editable --extra notebook
COPY --from=jars /jars/ /app/.venv/lib/python3.12/site-packages/pyspark/jars/

FROM base
RUN mkdir -p /home/ml /tmp/spark /opt/notebooks && chown 1000:1000 /home/ml /tmp/spark
COPY --from=build /app/.venv /app/.venv
COPY src/ml/notebooks/payment_risk_exploration.ipynb /opt/notebooks/
ENV PATH="/app/.venv/bin:$PATH" \
    VIRTUAL_ENV=/app/.venv \
    PYSPARK_PYTHON=/app/.venv/bin/python \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    HOME=/home/ml
WORKDIR /home/ml
USER 1000:1000
ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["python", "-m", "risk_model.steps"]
