# Docker images

Every deployable unit has its own image built in CI from [`infra/docker`](../../infra/docker); build paths per image are declared in [`.github/service-images.json`](../../.github/service-images.json), so an image rebuilds only when its inputs change. All images run as a non-root user.

| Image | Dockerfile | Build | Runtime base |
|---|---|---|---|
| edge, payments, fraud, notifier, vault-simulator, processor-simulator | `go-service.Dockerfile` (shared, parameterized by service path) | `golang:1.26-bookworm` builder; static binary with `CGO_ENABLED=0`, `-trimpath`, `-ldflags='-s -w'` | `gcr.io/distroless/static-debian12:nonroot` |
| live-simulator (also the historical generator) | `live-simulator.Dockerfile` | `uv sync --locked --no-dev`, dependencies before source for layer caching | `python:3.14-slim-bookworm` |
| feast | `feast.Dockerfile` | Separate build stage with `uv sync --locked --no-dev` | `python:3.11.16-slim-bookworm` |
| streaming | `streaming.Dockerfile` | Build stage on the Flink image; reuses the base image's Flink JARs instead of installing `apache-flink-libraries` | `flink:2.2.1-java17` |
| schema-register | `schema-register.Dockerfile` | `protoc` stage, then a slim Python runtime | `python:3.14-slim-bookworm` |
| connect-debezium | `connect-debezium.Dockerfile` | Debezium plugin added to the Strimzi Kafka image | `quay.io/strimzi/kafka:1.2.0-kafka-4.3.1` |
| data-platform-metadata | `data-platform-metadata.Dockerfile` | Integration modules added to the pinned OpenMetadata ingestion image | `openmetadata/ingestion:2.0.6` |

The Go services share one Dockerfile so every service gets the same multi-stage build and distroless runtime; there is no shell or package manager in the final image.

**Pending evidence:** image sizes before and after these optimizations on comparable builds, with the original single-stage baselines, are added here.
