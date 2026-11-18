#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

DATACONTRACT_CLI=1.2.7
DATAPRODUCT_CLI=0.3.2
IMPORT_LINTER=2.16

each_project() {
  local manifest="$1" check="$2" project
  while IFS= read -r -d '' project; do
    project="$(dirname "$project")"
    printf '\nLinting %s\n' "$project"
    "$check" "$project"
  done < <(git ls-files -z -- "src/**/$manifest")
}

lint_go() (
  cd "$1"
  GOWORK=off golangci-lint run ./...
)

lint_python() (
  cd "$1"
  ruff check .
  ruff format --check .
  if grep -q '^\[tool\.importlinter\]' pyproject.toml; then
    uvx --from "import-linter==$IMPORT_LINTER" lint-imports
  fi
)

lint_python_scripts() {
  ruff check --target-version py311 scripts/codegen-proto.py scripts/release.py
  ruff format --check scripts/codegen-proto.py scripts/release.py
}

lint_data_product() {
  local product="$1" name contract reference
  name="$(basename "$product")"
  for contract in "$product"/*.odcs.yaml; do
    reference="docs/data/products/$name/$(basename "$contract" .odcs.yaml).md"
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

lint_contracts() {
  TMPDIR_CHECKS="$(mktemp -d)"
  trap 'rm -rf "$TMPDIR_CHECKS"' EXIT
  local product
  while IFS= read -r -d '' product; do
    lint_data_product "$(dirname "$product")"
  done < <(find contracts/data-products -mindepth 2 -maxdepth 2 -name product.odps.yaml -print0 | sort -z)
}

lint_chart() {
  local chart="$1" values
  helm lint "$chart"
  helm template check "$chart" > /dev/null
  for values in "$chart"/values-*.yaml; do
    [[ -f "$values" ]] || continue
    helm lint "$chart" -f "$values"
    helm template check "$chart" -f "$values" > /dev/null
  done
}

lint_sql_jobs() {
  local rendered
  rendered="$(helm template lakehouse infra/charts/lakehouse -f infra/charts/lakehouse/values-prod.yaml && helm template clickhouse infra/charts/clickhouse -f infra/charts/clickhouse/values-prod.yaml)" || return 1
  if grep -qw curl <<<"$rendered"; then
    echo "lakehouse: a rendered job calls curl, which the ClickHouse image does not ship" >&2
    return 1
  fi
  if grep -qw clickhouse-client <<<"$rendered"; then
    echo "lakehouse: a rendered SQL job runs the ClickHouse binary, which needs AVX2 on the node" >&2
    return 1
  fi
  if grep -Eq -- '--password([ =]|$)' <<<"$rendered"; then
    echo "lakehouse: a rendered job passes a password as a command-line argument" >&2
    return 1
  fi
}

lint_helm() {
  local manifest
  while IFS= read -r -d '' manifest; do
    lint_chart "$(dirname "$manifest")"
  done < <(find infra -type f -name Chart.yaml -print0)
  lint_sql_jobs
}

ast-grep test
lint_python_scripts
each_project go.mod lint_go
each_project pyproject.toml lint_python
lint_contracts
lint_helm
mvn -B -q -f src/data-pipeline/streaming/jvm/pom.xml fmt:check
