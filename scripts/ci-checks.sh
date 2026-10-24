#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

check_projects() {
  local manifest="$1" check="$2" project
  while IFS= read -r -d '' project; do
    project="$(dirname "$project")"
    printf '\nChecking %s\n' "$project"
    "$check" "$project"
  done < <(git ls-files -z -- "src/**/$manifest")
}

check_python() (
  cd "$1"
  uv run --locked ruff check .
  uv run --locked ruff format --check .
  if [[ -d tests ]]; then
    uv run --locked pytest -q
  fi
)

check_go() (
  local reports="$report_dir/${1#src/}"
  mkdir -p "$reports"
  cd "$1"
  export GOWORK=off
  golangci-lint run ./...
  go test -p 1 -race -count=1 -coverprofile "$reports/coverage.out" ./...
  go tool cover -func "$reports/coverage.out" | tee "$reports/coverage.txt"
  go tool cover -html "$reports/coverage.out" -o "$reports/coverage.html"
)

check_data_product() {
  local product="$1" name contract reference
  name="$(basename "$product")"
  for contract in "$product"/*.odcs.yaml; do
    reference="docs/data-ml/data-products/$name/$(basename "$contract" .odcs.yaml).md"
    uvx --from "datacontract-cli==$DATACONTRACT_CLI" datacontract lint "$contract" --all-errors
    uvx --from "datacontract-cli==$DATACONTRACT_CLI" datacontract export markdown "$contract" --output "$TMPDIR_CHECKS/reference.md"
    diff -u "$reference" "$TMPDIR_CHECKS/reference.md"
  done
  (
    cd "$product"
    uvx --from "dataproduct-cli==$DATAPRODUCT_CLI" --with "datacontract-cli==$DATACONTRACT_CLI" \
      dataproduct lint product.odps.yaml --all-errors --local-references
  )
}

check_chart() {
  local chart="$1" values
  helm lint "$chart"
  helm template check "$chart" > /dev/null
  for values in "$chart"/values-*.yaml; do
    [[ -f "$values" ]] || continue
    helm lint "$chart" -f "$values"
    helm template check "$chart" -f "$values" > /dev/null
  done
}

DATACONTRACT_CLI=1.2.7
DATAPRODUCT_CLI=0.3.2

case "${1:-}" in
  go)
    report_dir="$(realpath -m "${CI_TEST_REPORTS:-$(mktemp -d)}")"
    mkdir -p "$report_dir"
    if [[ -n "${2:-}" ]]; then
      check_go "$2"
    else
      check_projects go.mod check_go
    fi
    ;;
  contracts)
    TMPDIR_CHECKS="$(mktemp -d)"
    trap 'rm -rf "$TMPDIR_CHECKS"' EXIT
    while IFS= read -r -d '' product; do
      check_data_product "$(dirname "$product")"
    done < <(find contracts/data-products -mindepth 2 -maxdepth 2 -name product.odps.yaml -print0 | sort -z)
    ;;
  python)
    check_projects pyproject.toml check_python
    ;;
  helm)
    while IFS= read -r -d '' manifest; do
      check_chart "$(dirname "$manifest")"
    done < <(find infra -type f -name Chart.yaml -print0)
    ;;
  *)
    printf 'Usage: %s {contracts|python|go|helm}\n' "$0" >&2
    exit 2
    ;;
esac
