#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

GO_MODULE_PREFIX=github.com/phuchoang2603/payment-gateway-fraud-detection

mapfile -d '' protos < <(find contracts -type f -name '*.proto' -print0 | sort -z)

if [[ "${1:-}" != "--python-only" ]]; then
mkdir -p src/payment-gateway/shared/proto
protoc \
  -I contracts \
  --go_out=src/payment-gateway/shared --go_opt="module=$GO_MODULE_PREFIX/shared" \
  --go-grpc_out=src/payment-gateway/shared --go-grpc_opt="module=$GO_MODULE_PREFIX/shared" \
  "${protos[@]}"
fi

python_out=src/data-pipeline/simulator/simulator/contracts
mkdir -p "$python_out"
protoc -I contracts --python_out="$python_out" --pyi_out="$python_out" \
  contracts/payments/v1/types.proto contracts/payments/v1/events.proto contracts/payments/v1/labels.proto
python3 - "$python_out" <<'PYTHON'
from pathlib import Path
import sys
root = Path(sys.argv[1])
for directory in (root, root / "payments", root / "payments/v1"):
    (directory / "__init__.py").touch()
for path in list(root.rglob("*_pb2.py")) + list(root.rglob("*_pb2.pyi")):
    text = path.read_text().replace("from payments.v1 import", "from simulator.contracts.payments.v1 import")
    text = text.replace("'payments.v1.", "'simulator.contracts.payments.v1.")
    path.write_text(text)
PYTHON
