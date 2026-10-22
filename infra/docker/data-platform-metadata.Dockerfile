FROM ghcr.io/astral-sh/uv:0.12.11 AS uv
FROM python:3.12.15-slim-bookworm
COPY --from=uv /uv /usr/local/bin/uv
WORKDIR /opt/dp1
COPY src/data-platform/pyproject.toml src/data-platform/uv.lock ./
COPY src/data-platform/data_platform ./data_platform
RUN uv sync --locked --no-dev --no-editable
COPY contracts/data-products /contracts/data-products
ENV DATA_PRODUCTS_ROOT="/contracts/data-products" PATH="/opt/dp1/.venv/bin:$PATH" PYTHONPATH="/opt/dp1"
RUN groupadd --gid 185 metadata && useradd --uid 185 --gid 185 --home-dir /opt/dp1 metadata && chown 185:185 /opt/dp1
USER 185
ENTRYPOINT ["dp1"]
