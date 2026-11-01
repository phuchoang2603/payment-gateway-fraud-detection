# kagent's Go agent runtime with the Langfuse prompt plugin. The build context is the repository root.
# The runtime image mirrors kagent's: bash for its built-in shell tool and writable config, skills and plugin dirs.
FROM golang:1.27.3-bookworm AS builder

ENV GOWORK=off
WORKDIR /src
COPY src/llm/agent_runtime/go.mod src/llm/agent_runtime/go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY src/llm/agent_runtime ./
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -trimpath -ldflags='-s -w' -o /out/agent-runtime ./cmd/agent_runtime

FROM alpine:3.24
RUN apk add --no-cache bash ca-certificates && \
    addgroup -S -g 65532 agent && \
    adduser -S -D -u 65532 -G agent -h /home/agent agent && \
    mkdir -p /config /skills /plugins /data/plugins && \
    chown -R 65532:65532 /config /skills /plugins /data
COPY --from=builder /out/agent-runtime /app/agent-runtime
USER 65532:65532
ENV HOME=/home/agent
ENTRYPOINT ["/app/agent-runtime"]
