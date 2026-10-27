#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

if [[ "${GITHUB_EVENT_NAME:-}" != "push" || "${GITHUB_REF:-}" != "refs/heads/main" ]]; then
  echo "production image pins require a main push" >&2
  exit 1
fi

uv run --with PyYAML==6.0.3 python scripts/pin-service-images.py "$1"

if git diff --quiet -- 'infra/**/values-prod.yaml'; then
  echo "production images already match published digests"
  exit 0
fi
git config user.name "github-actions[bot]"
git config user.email "41898282+github-actions[bot]@users.noreply.github.com"
git add -- 'infra/**/values-prod.yaml'
git commit -m "chore(infra): pin published images from ${GITHUB_SHA} [skip ci]"
git push
