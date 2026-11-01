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
| data-platform-spark | `data-platform-spark.Dockerfile` | Checksummed connector JARs in a `scratch` stage, `uv sync` in a build stage; the runtime copies only Spark's `bin`, `jars` and Python libraries from the official image | `python:3.12.15-slim-bookworm` with a headless JRE |
| data-platform-metadata | `data-platform-metadata.Dockerfile` | Integration modules added to the pinned OpenMetadata ingestion image | `openmetadata/ingestion:2.0.6` |

The Go services share one Dockerfile so every service gets the same multi-stage build and distroless runtime; there is no shell or package manager in the final image.

## Image sizes

Local is `docker images` on the build host; compressed is the sum of the layer blobs a node pulls from the GitHub container registry, and unpacked is what those layers expand to on disk (`linux/amd64`).

| Image | Before | After | Change |
|---|---|---|---|
| data-platform-spark | `1e090ee` (2026-10-22): `uv sync` kept uv's download cache in the image, so its layer held 2.25 GB for about 1.5 GB of installed packages. 5.07 GB local, 2.02 GB compressed, 2.99 GB unpacked | `opt` (same day, same Dockerfile otherwise): `UV_NO_CACHE=1` and `UV_LINK_MODE=copy`. 3.06 GB local, 1.06 GB compressed, 1.94 GB unpacked | −40% local, −48% compressed |
| data-platform-spark, current | | `main-eb98307` (2026-11-01): Spark 4.1.3 with Iceberg, Kafka and protobuf connectors, built in separate stages (checksummed JARs, `uv sync`), with only Spark's `bin`, `jars` and Python libraries copied from the official image and pip and `__pycache__` removed. 1.35 GB compressed, 1.89 GB unpacked | −33% compressed against `1e090ee`, with a newer Spark and more connectors |
| streaming | `6cec4a9` (2026-10-27): `uv sync` also installed `apache-flink-libraries`, a second copy of the Flink JARs the base image already has. 1.22 GB compressed, 1.82 GB unpacked | `52219c0` (same day): `--no-install-package apache-flink-libraries`. 0.91 GB compressed, 1.49 GB unpacked | −25% compressed, −18% unpacked |

The Spark image runs on the driver node and every executor node, so each saved gigabyte is pulled once per node per image version.

The Go service images are a single static binary on distroless and the Python service images start from slim bases, so they had no larger first build to compare: live-simulator is 159 MB and feast 180 MB compressed.
