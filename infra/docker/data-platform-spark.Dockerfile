FROM apache/spark:4.1.3-scala2.13-java17-python3-ubuntu AS spark-runtime
FROM ghcr.io/astral-sh/uv:0.13.1 AS uv

FROM python:3.12.15-slim-bookworm AS jars
COPY src/data-pipeline/feast/fraud_feast/spark_jars.py src/data-pipeline/feast/fraud_feast/spark_jars.json /runtime/
RUN python /runtime/spark_jars.py /jars

FROM python:3.12.15-slim-bookworm AS build
COPY --from=uv /uv /usr/local/bin/uv
WORKDIR /opt/data-pipeline/batch
ENV UV_NO_CACHE=1 UV_LINK_MODE=copy UV_PYTHON_DOWNLOADS=0
COPY src/data-pipeline/batch/pyproject.toml src/data-pipeline/batch/uv.lock ./
RUN uv sync --locked --no-dev --no-install-project
COPY src/data-pipeline/batch/batch ./batch
RUN uv sync --locked --no-dev --no-editable \
    && find .venv -type d -name __pycache__ -prune -exec rm -rf {} + \
    && rm -rf .venv/lib/python3.12/site-packages/pip* .venv/bin/pip*
COPY contracts/payments /opt/contracts/payments
RUN uv run --no-project --with grpcio-tools==1.84.0 python -c "import pathlib, sys; import grpc_tools; from grpc_tools import protoc; include = pathlib.Path(grpc_tools.__file__).parent / '_proto'; sys.exit(protoc.main(['protoc', '-I/opt/contracts', f'-I{include}', '--include_imports', '--descriptor_set_out=/opt/data-pipeline/batch/payments.pb', 'payments/v1/events.proto', 'payments/v1/labels.proto']))"

WORKDIR /opt/data-pipeline/feast
COPY src/data-pipeline/feast/pyproject.toml src/data-pipeline/feast/uv.lock ./
COPY src/data-pipeline/feast/fraud_feast ./fraud_feast
RUN uv sync --locked --no-dev --extra spark --no-editable

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
COPY --from=build /opt/data-pipeline/feast /opt/data-pipeline/feast
COPY --from=build /opt/data-pipeline/batch/.venv /opt/data-pipeline/batch/.venv
COPY --from=build /opt/data-pipeline/batch/payments.pb /opt/data-pipeline/batch/payments.pb
COPY --from=jars /jars/ /opt/spark/jars/
COPY contracts/data-products /contracts/data-products
COPY contracts/features/fraud-snapshot-v2.yaml /contracts/features/
ENV FEATURE_DEFINITION=/contracts/features/fraud-snapshot-v2.yaml
ENV PATH="/opt/data-pipeline/batch/.venv/bin:$PATH" \
    PYSPARK_PYTHON="/opt/data-pipeline/batch/.venv/bin/python" \
    DATA_PRODUCTS_ROOT="/contracts/data-products" \
    PAYMENTS_DESCRIPTOR="/opt/data-pipeline/batch/payments.pb"
WORKDIR /opt/spark/work-dir
RUN chown 185:185 /opt/data-pipeline/batch /opt/spark/work-dir
USER 185
ENTRYPOINT ["/usr/bin/tini", "--", "/opt/entrypoint.sh"]
