FROM apache/spark:4.1.3-scala2.13-java17-python3-ubuntu AS spark-runtime
FROM ghcr.io/astral-sh/uv:0.12.11 AS uv

FROM scratch AS jars
ADD --checksum=sha256:3123c3798af642dd537230923548e013ef41cc28b6626e5225808cf5670a0f82 --chmod=644 https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-spark-runtime-4.1_2.13/1.12.0/iceberg-spark-runtime-4.1_2.13-1.12.0.jar /jars/iceberg-spark-runtime-4.1_2.13-1.12.0.jar
ADD --checksum=sha256:dd7e49ffa4ac9b215bed846581991259e3aec9bb2fa08d6cec029c46cd66d957 --chmod=644 https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.12.0/iceberg-aws-bundle-1.12.0.jar /jars/iceberg-aws-bundle-1.12.0.jar
ADD --checksum=sha256:d2d24e6b715b5656b9d238bf13b6682a07e2004dd36aad74a73aeb854319f4d9 --chmod=644 https://repo.maven.apache.org/maven2/org/apache/hadoop/hadoop-aws/3.4.2/hadoop-aws-3.4.2.jar /jars/hadoop-aws-3.4.2.jar
ADD --checksum=sha256:62638f33045690c6bc5e496b54b0b2e89a9ad38cbedacdf6e017fea6bc85f55c --chmod=644 https://repo.maven.apache.org/maven2/software/amazon/awssdk/bundle/2.29.52/bundle-2.29.52.jar /jars/bundle-2.29.52.jar

FROM python:3.12.15-slim-bookworm AS build
COPY --from=uv /uv /usr/local/bin/uv
WORKDIR /opt/data-pipeline/bronze-ingestion
ENV UV_NO_CACHE=1 UV_LINK_MODE=copy UV_PYTHON_DOWNLOADS=0
COPY src/data-pipeline/bronze-ingestion/pyproject.toml src/data-pipeline/bronze-ingestion/uv.lock ./
RUN uv sync --locked --no-dev --no-install-project --extra spark --no-install-package pyspark
COPY src/data-pipeline/bronze-ingestion/data_platform ./data_platform
RUN uv sync --locked --no-dev --no-editable --extra spark --no-install-package pyspark
RUN find .venv -type d \( -path "*/site-packages/numpy" -o -path "*/site-packages/dbt" \) -prune -o \
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
    PYTHONPATH="/opt/spark/python/lib/pyspark.zip:/opt/spark/python/lib/py4j-0.10.9.9-src.zip"
RUN groupadd --gid 185 spark && useradd --uid 185 --gid 185 --home-dir /opt/spark spark
COPY --from=build /opt/data-pipeline/bronze-ingestion/.venv /opt/data-pipeline/bronze-ingestion/.venv
COPY --from=jars /jars/ /opt/spark/jars/
COPY infra/docker/data-platform-spark/log4j2.properties /opt/dp1/log4j2.properties
COPY contracts/data-products /contracts/data-products
ENV PATH="/opt/data-pipeline/bronze-ingestion/.venv/bin:$PATH" \
    PYSPARK_PYTHON="/opt/data-pipeline/bronze-ingestion/.venv/bin/python" \
    DATA_PRODUCTS_ROOT="/contracts/data-products" \
    DP1_BUNDLED_JARS="true"
WORKDIR /opt/spark/work-dir
RUN chown 185:185 /opt/data-pipeline/bronze-ingestion /opt/spark/work-dir
USER 185
ENTRYPOINT ["/usr/bin/tini", "--", "/opt/entrypoint.sh"]
