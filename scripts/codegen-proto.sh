#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

GO_MODULE_PREFIX=github.com/phuchoang2603/payment-gateway-fraud-detection

mapfile -d '' protos < <(find contracts -type f -name '*.proto' -print0 | sort -z)

mkdir -p src/payment-gateway/shared/proto
protoc \
  -I contracts \
  --go_out=src/payment-gateway/shared --go_opt="module=$GO_MODULE_PREFIX/shared" \
  --go-grpc_out=src/payment-gateway/shared --go-grpc_opt="module=$GO_MODULE_PREFIX/shared" \
  "${protos[@]}"
