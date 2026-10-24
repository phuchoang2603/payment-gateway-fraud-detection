#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

if [[ "${GITHUB_EVENT_NAME:-}" != "push" || "${GITHUB_REF:-}" != "refs/heads/main" ]]; then
  echo "production image pins require a main push" >&2
  exit 1
fi

tag="main-${GITHUB_SHA}"

pin() {
  local file="$1" pattern="$2" replacement="$3" count
  count=$(grep -cE "$pattern" "$file")
  if [[ "$count" -ne 1 ]]; then
    echo "expected exactly one image tag in $file, found $count" >&2
    exit 1
  fi
  sed -i -E "s|${pattern}|${replacement}|" "$file"
}

pin infra/charts/payment-gateway/values-prod.yaml '^imageTag: .*$' "imageTag: ${tag}"
pin infra/charts/kafka/values-prod.yaml '^  imageTag: .*$' "  imageTag: ${tag}"
if git diff --quiet; then
  echo "image tags already at ${tag}"
  exit 0
fi
git config user.name "github-actions[bot]"
git config user.email "41898282+github-actions[bot]@users.noreply.github.com"
git add infra/charts/payment-gateway/values-prod.yaml infra/charts/kafka/values-prod.yaml
git commit -m "chore(infra): track service images ${tag} [skip ci]"
git push
