FROM ghcr.io/astral-sh/uv:0.13.1 AS uv
FROM flink:2.2.1-java17 AS build
USER root
COPY --from=uv /uv /usr/local/bin/uv
ENV UV_PYTHON_INSTALL_DIR=/opt/python UV_PYTHON_BIN_DIR=/usr/local/bin UV_LINK_MODE=copy
RUN apt-get update \
    && apt-get install -y --no-install-recommends gcc libc6-dev ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && uv python install 3.11.16
WORKDIR /app
COPY src/data-pipeline/streaming/pyproject.toml src/data-pipeline/streaming/uv.lock ./
RUN uv sync --locked --no-dev --no-install-project \
    && mkdir -p /opt/flink/plugins/s3-fs-presto \
    && cp /opt/flink/opt/flink-s3-fs-presto-2.2.1.jar /opt/flink/plugins/s3-fs-presto/ \
    && wget -q -O /opt/flink/lib/flink-sql-connector-kafka-5.0.0-2.2.jar \
       https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/5.0.0-2.2/flink-sql-connector-kafka-5.0.0-2.2.jar \
    && echo '5605c691d11a501382c383fecba37a7a552467da5ab7ba904ef5d6f3d62c5616  /opt/flink/lib/flink-sql-connector-kafka-5.0.0-2.2.jar' | sha256sum -c -

FROM flink:2.2.1-java17
COPY --from=build /opt/python /opt/python
COPY --from=build /app/.venv /app/.venv
COPY --from=build /opt/flink/plugins/s3-fs-presto /opt/flink/plugins/s3-fs-presto
COPY --from=build /opt/flink/lib/flink-sql-connector-kafka-5.0.0-2.2.jar /opt/flink/lib/
WORKDIR /app
COPY src/data-pipeline/streaming/fraud_streaming ./fraud_streaming
ENV PATH="/app/.venv/bin:$PATH" PYTHONPATH=/app PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
USER flink
WORKDIR /opt/flink
