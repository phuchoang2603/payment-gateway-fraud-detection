FROM maven:3.9.11-eclipse-temurin-17 AS jvm-build
WORKDIR /workspace
COPY contracts ./contracts
COPY src/data-pipeline/streaming/jvm/pom.xml ./src/data-pipeline/streaming/jvm/pom.xml
COPY src/data-pipeline/streaming/jvm/src ./src/data-pipeline/streaming/jvm/src
RUN mvn -B -f src/data-pipeline/streaming/jvm/pom.xml -Dmaven.test.skip=true package

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
    && cp /opt/flink/opt/flink-s3-fs-presto-2.2.1.jar /opt/flink/plugins/s3-fs-presto/


FROM flink:2.2.1-java17
COPY --from=jvm-build /workspace/src/data-pipeline/streaming/jvm/target/fraud-streaming-1.0.0.jar /opt/flink/opt/fraud-streaming.jar
COPY --from=build /opt/python /opt/python
COPY --from=build /app/.venv /app/.venv
COPY --from=build /opt/flink/plugins/s3-fs-presto /opt/flink/plugins/s3-fs-presto
WORKDIR /app
COPY src/data-pipeline/streaming/fraud_streaming/__init__.py src/data-pipeline/streaming/fraud_streaming/history.py src/data-pipeline/streaming/fraud_streaming/contracts.py src/data-pipeline/streaming/fraud_streaming/contracts.pb ./fraud_streaming/
COPY contracts/features/fraud-snapshot-v2.yaml /contracts/features/
ENV FEATURE_DEFINITION=/contracts/features/fraud-snapshot-v2.yaml
ENV PATH="/app/.venv/bin:$PATH" PYTHONPATH=/app PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
USER flink
WORKDIR /opt/flink
