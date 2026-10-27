FROM debian:bookworm-slim AS protobuf
RUN apt-get update && apt-get install -y --no-install-recommends libprotobuf-dev && \
    rm -rf /var/lib/apt/lists/*

FROM python:3.14-slim-bookworm
WORKDIR /app
COPY --from=protobuf /usr/include/google/protobuf/timestamp.proto /app/contracts/google/protobuf/timestamp.proto
COPY contracts/payments/v1 /app/contracts/payments/v1
COPY contracts/features/v1 /app/contracts/features/v1
COPY scripts/schema_registry.py /app/schema_registry.py
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
USER 65532:65532
ENTRYPOINT ["python", "/app/schema_registry.py"]
