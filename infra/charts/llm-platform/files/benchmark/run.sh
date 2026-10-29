#!/bin/sh
set -eu

out=/work/reports
mkdir -p "$out"

python3 /etc/llm-benchmark/server_metrics.py "$METRICS_URL" > /work/metrics-before.txt

set -- --config_file /etc/llm-benchmark/config.yml --server.base_url "$TARGET_URL"
if [ -n "${LLM_API_KEY:-}" ]; then
  set -- "$@" --server.api_key "$LLM_API_KEY"
fi
inference-perf "$@"

python3 /etc/llm-benchmark/server_metrics.py "$METRICS_URL" > /work/metrics-after.txt
mv /work/metrics-before.txt /work/metrics-after.txt "$out/"

# inference-perf saves its full configuration, including the API key, as config.yaml next to the reports.
find "$out" -name 'config.yaml' -delete
if [ -n "${LLM_API_KEY:-}" ] && grep -rqF "$LLM_API_KEY" "$out"; then
  echo "refusing to upload: API key found in reports" >&2
  exit 1
fi

python3 - "$out" <<'EOF'
import os, sys, pathlib, boto3
root = pathlib.Path(sys.argv[1])
s3 = boto3.client("s3", endpoint_url=os.environ["S3_ENDPOINT"], region_name="us-east-1")
prefix = f'{os.environ["S3_PREFIX"]}/{os.environ["RUN_NAME"]}'
for path in sorted(p for p in root.rglob("*") if p.is_file()):
    key = f"{prefix}/{path.relative_to(root)}"
    s3.upload_file(str(path), os.environ["S3_BUCKET"], key)
    print(f"uploaded s3://{os.environ['S3_BUCKET']}/{key}", flush=True)
EOF
