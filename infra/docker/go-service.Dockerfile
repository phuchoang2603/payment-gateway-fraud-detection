FROM golang:1.26-bookworm AS builder

ARG SERVICE_PATH=src/payment-gateway/edge
ARG BUILD_PKG=./cmd/edge
ARG BUILD_BIN=edge
ENV GOWORK=off
WORKDIR /repo/${SERVICE_PATH}
COPY src/payment-gateway/shared/go.mod src/payment-gateway/shared/go.sum /repo/src/payment-gateway/shared/
COPY ${SERVICE_PATH}/go.mod ${SERVICE_PATH}/go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY src/payment-gateway/shared /repo/src/payment-gateway/shared
COPY ${SERVICE_PATH} ./
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -trimpath -ldflags='-s -w' -o /out/${BUILD_BIN} ${BUILD_PKG}

FROM gcr.io/distroless/static-debian12:nonroot
ARG BUILD_BIN=edge
ARG EXPOSE_PORT=8080
COPY --from=builder /out/${BUILD_BIN} /app/service
EXPOSE ${EXPOSE_PORT}
USER nonroot:nonroot
ENTRYPOINT ["/app/service"]
