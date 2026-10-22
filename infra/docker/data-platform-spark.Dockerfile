FROM apache/spark:3.5.8-scala2.12-java17-python3-ubuntu AS spark-runtime
FROM ghcr.io/astral-sh/uv:0.12.11 AS uv
FROM python:3.12.15-slim-bookworm
COPY --from=uv /uv /usr/local/bin/uv
COPY --from=spark-runtime /opt/entrypoint.sh /opt/entrypoint.sh
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps tini && rm -rf /var/lib/apt/lists/*
WORKDIR /opt/dp1
COPY src/data-platform/pyproject.toml src/data-platform/uv.lock ./
COPY src/data-platform/data_platform ./data_platform
RUN uv sync --locked --no-dev --no-editable --extra spark
COPY scripts/prepare-dp1-jars.py scripts/dp1-jars.lock.json /opt/jars/
RUN .venv/bin/python /opt/jars/prepare-dp1-jars.py /opt/dp1/.venv/lib/python3.12/site-packages/pyspark/jars
COPY contracts/data-products /contracts/data-products
ENV PATH="/opt/dp1/.venv/bin:$PATH" \
    SPARK_HOME="/opt/dp1/.venv/lib/python3.12/site-packages/pyspark" \
    PYSPARK_PYTHON="/opt/dp1/.venv/bin/python" \
    DATA_PRODUCTS_ROOT="/contracts/data-products" \
    DP1_BUNDLED_JARS="true" \
    PYTHONPATH="/opt/dp1"
RUN ln -s /opt/dp1/.venv/lib/python3.12/site-packages/pyspark /opt/spark && \
    groupadd --gid 185 spark && useradd --uid 185 --gid 185 --home-dir /opt/dp1 spark && chown 185:185 /opt/dp1
USER 185
ENTRYPOINT ["/usr/bin/tini", "--", "/opt/entrypoint.sh"]
