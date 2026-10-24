FROM ghcr.io/astral-sh/uv:0.12.11 AS uv

FROM python:3.12.15-slim-bookworm AS build
COPY --from=uv /uv /usr/local/bin/uv
WORKDIR /opt/data-pipeline/bronze-ingestion
ENV UV_NO_CACHE=1 UV_LINK_MODE=copy UV_PYTHON_DOWNLOADS=0
COPY src/data-pipeline/bronze-ingestion/pyproject.toml src/data-pipeline/bronze-ingestion/uv.lock ./
RUN uv sync --locked --no-dev --no-install-project
COPY src/data-pipeline/bronze-ingestion/data_platform ./data_platform
RUN uv sync --locked --no-dev --no-editable && \
    find .venv -type d \( -path "*/site-packages/numpy" -o -path "*/site-packages/dbt" \) -prune -o \
        -type d \( -name tests -o -name test \) -prune -exec rm -rf {} + && \
    find .venv -type d -name __pycache__ -prune -exec rm -rf {} + && \
    rm -rf .venv/lib/python3.12/site-packages/pip*

FROM python:3.12.15-slim-bookworm
RUN python -m pip uninstall -y pip && \
    rm -rf /usr/local/lib/python3.12/ensurepip && \
    find /usr/local/lib/python3.12 -type d -name __pycache__ -prune -exec rm -rf {} +
COPY --from=build /opt/data-pipeline/bronze-ingestion/.venv /opt/data-pipeline/bronze-ingestion/.venv
COPY contracts/data-products /contracts/data-products
ENV PYTHONDONTWRITEBYTECODE=1 DATA_PRODUCTS_ROOT="/contracts/data-products" PATH="/opt/data-pipeline/bronze-ingestion/.venv/bin:$PATH"
RUN rm -f /opt/data-pipeline/bronze-ingestion/.venv/bin/pip* && \
    groupadd --gid 185 metadata && useradd --uid 185 --gid 185 --home-dir /opt/data-pipeline/bronze-ingestion metadata && chown 185:185 /opt/data-pipeline/bronze-ingestion
USER 185
ENTRYPOINT ["dp1"]
