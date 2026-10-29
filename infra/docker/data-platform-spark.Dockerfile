FROM apache/spark:4.1.3-scala2.13-java17-python3-ubuntu AS spark-runtime
FROM ghcr.io/astral-sh/uv:0.13.1 AS uv

FROM scratch AS jars
ADD --checksum=sha256:3123c3798af642dd537230923548e013ef41cc28b6626e5225808cf5670a0f82 --chmod=644 https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-spark-runtime-4.1_2.13/1.12.0/iceberg-spark-runtime-4.1_2.13-1.12.0.jar /jars/iceberg-spark-runtime-4.1_2.13-1.12.0.jar
ADD --checksum=sha256:dd7e49ffa4ac9b215bed846581991259e3aec9bb2fa08d6cec029c46cd66d957 --chmod=644 https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.12.0/iceberg-aws-bundle-1.12.0.jar /jars/iceberg-aws-bundle-1.12.0.jar
ADD --checksum=sha256:d2d24e6b715b5656b9d238bf13b6682a07e2004dd36aad74a73aeb854319f4d9 --chmod=644 https://repo.maven.apache.org/maven2/org/apache/hadoop/hadoop-aws/3.4.2/hadoop-aws-3.4.2.jar /jars/hadoop-aws-3.4.2.jar
ADD --checksum=sha256:62638f33045690c6bc5e496b54b0b2e89a9ad38cbedacdf6e017fea6bc85f55c --chmod=644 https://repo.maven.apache.org/maven2/software/amazon/awssdk/bundle/2.29.52/bundle-2.29.52.jar /jars/bundle-2.29.52.jar
ADD --checksum=sha256:cb31ffd9c8f5527dba061db8af671d295562092e4f3f5ff42fc249fab8a8fde6 --chmod=644 https://repo.maven.apache.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.13/4.1.3/spark-sql-kafka-0-10_2.13-4.1.3.jar /jars/spark-sql-kafka-0-10_2.13-4.1.3.jar
ADD --checksum=sha256:0535a111d7d11b2859c0a0548d5c8d172c6925f9b8d041d0f0dd38ddc5a3497c --chmod=644 https://repo.maven.apache.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.13/4.1.3/spark-token-provider-kafka-0-10_2.13-4.1.3.jar /jars/spark-token-provider-kafka-0-10_2.13-4.1.3.jar
ADD --checksum=sha256:7568b998572d256f0b7bc0afdc1b7a2588b8b08415c62ce314c864a6851ae9d9 --chmod=644 https://repo.maven.apache.org/maven2/org/apache/kafka/kafka-clients/3.9.1/kafka-clients-3.9.1.jar /jars/kafka-clients-3.9.1.jar
ADD --checksum=sha256:5273c8bc8c0dca2211175c0d27efbd7228efae99689aac001a8e1ef8e872e9ef --chmod=644 https://repo.maven.apache.org/maven2/org/apache/commons/commons-pool2/2.12.1/commons-pool2-2.12.1.jar /jars/commons-pool2-2.12.1.jar
ADD --checksum=sha256:4eae6e68cf44e9f709970355590ae981883edf6484608d747376a56cbb285432 --chmod=644 https://repo.maven.apache.org/maven2/org/scala-lang/modules/scala-parallel-collections_2.13/1.2.0/scala-parallel-collections_2.13-1.2.0.jar /jars/scala-parallel-collections_2.13-1.2.0.jar
ADD --checksum=sha256:4fed241863ac3405715e14a59b1ccf394a7ca7248437fa5bbaa1e1276addeb05 --chmod=644 https://repo.maven.apache.org/maven2/org/apache/spark/spark-protobuf_2.13/4.1.3/spark-protobuf_2.13-4.1.3.jar /jars/spark-protobuf_2.13-4.1.3.jar

FROM python:3.12.15-slim-bookworm AS build
COPY --from=uv /uv /usr/local/bin/uv
WORKDIR /opt/data-pipeline/batch
ENV UV_NO_CACHE=1 UV_LINK_MODE=copy UV_PYTHON_DOWNLOADS=0
COPY src/data-pipeline/batch/pyproject.toml src/data-pipeline/batch/uv.lock ./
RUN uv sync --locked --no-dev --no-install-project
COPY src/data-pipeline/batch/batch ./batch
RUN uv sync --locked --no-dev --no-editable \
    && find .venv -type d -name __pycache__ -prune -exec rm -rf {} + && \
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
COPY --from=build /opt/data-pipeline/batch/.venv /opt/data-pipeline/batch/.venv
COPY --from=jars /jars/ /opt/spark/jars/
COPY contracts/data-products /contracts/data-products
ENV PATH="/opt/data-pipeline/batch/.venv/bin:$PATH" \
    PYSPARK_PYTHON="/opt/data-pipeline/batch/.venv/bin/python" \
    DATA_PRODUCTS_ROOT="/contracts/data-products"
WORKDIR /opt/spark/work-dir
RUN chown 185:185 /opt/data-pipeline/batch /opt/spark/work-dir
USER 185
ENTRYPOINT ["/usr/bin/tini", "--", "/opt/entrypoint.sh"]
