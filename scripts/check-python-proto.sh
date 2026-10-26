#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
proto_dir=$(mktemp -d)
trap 'rm -rf "$proto_dir"' EXIT
curl --fail --location --silent --show-error \
  https://github.com/protocolbuffers/protobuf/releases/download/v36.1/protoc-36.1-linux-x86_64.zip \
  -o "$proto_dir/protoc.zip"
unzip -q "$proto_dir/protoc.zip" -d "$proto_dir"
PATH="$proto_dir/bin:$PATH" bash scripts/codegen-proto.sh --python-only
git diff --exit-code -- src/data-pipeline/simulator/simulator/contracts
