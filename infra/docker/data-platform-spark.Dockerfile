FROM apache/spark:3.5.8-scala2.12-java17-python3-ubuntu AS spark-runtime
FROM ghcr.io/astral-sh/uv:0.12.11 AS uv

FROM python:3.12.15-slim-bookworm AS build
COPY --from=uv /uv /usr/local/bin/uv
WORKDIR /opt/data-pipeline/bronze-ingestion
ENV UV_NO_CACHE=1 UV_LINK_MODE=copy UV_PYTHON_DOWNLOADS=0
COPY src/data-pipeline/bronze-ingestion/pyproject.toml src/data-pipeline/bronze-ingestion/uv.lock ./
RUN uv sync --locked --no-dev --no-install-project --extra spark --no-install-package pyspark
COPY src/data-pipeline/bronze-ingestion/data_platform ./data_platform
RUN uv sync --locked --no-dev --no-editable --extra spark --no-install-package pyspark
COPY scripts/prepare-dp1-jars.py scripts/dp1-jars.lock.json /opt/jars/
RUN .venv/bin/python /opt/jars/prepare-dp1-jars.py /opt/jars/bundled && \
    find .venv -type d \( -path "*/site-packages/numpy" -o -path "*/site-packages/dbt" \) -prune -o \
        -type d \( -name tests -o -name test \) -prune -exec rm -rf {} + && \
    find .venv -type d -name __pycache__ -prune -exec rm -rf {} + && \
    rm -rf .venv/lib/python3.12/site-packages/pip* .venv/bin/pip*

FROM python:3.12.15-slim-bookworm
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps tini && \
    rm -rf /var/lib/apt/lists/* && \
    python -m pip uninstall -y pip && \
    rm -rf /usr/local/lib/python3.12/ensurepip && \
    find /usr/local/lib/python3.12 -type d -name __pycache__ -prune -exec rm -rf {} +
COPY --from=spark-runtime /opt/entrypoint.sh /opt/entrypoint.sh
COPY --from=spark-runtime /opt/spark/bin /opt/spark/bin
COPY --from=spark-runtime /opt/spark/jars /opt/spark/jars
COPY --from=spark-runtime /opt/spark/python/lib /opt/spark/python/lib
ENV PYTHONDONTWRITEBYTECODE=1 \
    PATH="/opt/spark/bin:$PATH" \
    SPARK_HOME="/opt/spark" \
    PYSPARK_PYTHON="/usr/local/bin/python" \
    PYTHONPATH="/opt/spark/python/lib/pyspark.zip:/opt/spark/python/lib/py4j-0.10.9.7-src.zip"
RUN python -c 'from pathlib import Path; \
        jars = Path("/opt/spark/jars"); \
        original = jars / "jta-1.1.jar"; \
        duplicate = jars / "transaction-api-1.1.jar"; \
        assert original.read_bytes() == duplicate.read_bytes(); \
        duplicate.unlink(); duplicate.symlink_to(original.name)' && \
    groupadd --gid 185 spark && useradd --uid 185 --gid 185 --home-dir /opt/spark spark
COPY --from=build /opt/data-pipeline/bronze-ingestion/.venv /opt/data-pipeline/bronze-ingestion/.venv
COPY --from=build /opt/jars/bundled/ /opt/spark/jars/
COPY contracts/data-products /contracts/data-products
ENV PATH="/opt/data-pipeline/bronze-ingestion/.venv/bin:$PATH" \
    PYSPARK_PYTHON="/opt/data-pipeline/bronze-ingestion/.venv/bin/python" \
    DATA_PRODUCTS_ROOT="/contracts/data-products" \
    DP1_BUNDLED_JARS="true"
RUN chown 185:185 /opt/data-pipeline/bronze-ingestion
USER 185
ENTRYPOINT ["/usr/bin/tini", "--", "/opt/entrypoint.sh"]
