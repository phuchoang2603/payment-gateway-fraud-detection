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
  local reports="$report_dir/${1#src/}"
  mkdir -p "$reports"
  cd "$1"
  uv run --locked ruff check .
  uv run --locked ruff format --check .
  if [[ -d tests ]]; then
    # A project sets a minimum total coverage in [tool.ci] coverage-fail-under; the default of 0 reports only.
    local floor
    floor="$(python3 -c 'import tomllib; print(tomllib.load(open("pyproject.toml", "rb")).get("tool", {}).get("ci", {}).get("coverage-fail-under", 0))')"
    COVERAGE_FILE="$reports/.coverage" uv run --locked --with "pytest-cov==$PYTEST_COV" pytest -q --durations=10 \
      --cov=. --cov-config="$coverage_config" --cov-fail-under="$floor" --cov-report=term \
      --cov-report="xml:$reports/coverage.xml" --cov-report="html:$reports/coverage-html"
    # A project whose code runs in a second, incompatible environment names it in [tool.ci] test-groups; the tests
    # that need it skip in the default environment and run here.
    local group
    for group in $(python3 -c 'import tomllib; print(" ".join(tomllib.load(open("pyproject.toml", "rb")).get("tool", {}).get("ci", {}).get("test-groups", [])))'); do
      uv run --locked --no-default-groups --group dev --group "$group" pytest -q --durations=5
    done
    if [[ "$1" == src/llm/rag_serving && -n "${MUTATION_BASE:-}" ]] &&
      ! git diff --quiet "$MUTATION_BASE" HEAD -- fraud_rag_serving/app.py tests pyproject.toml ../common ../uv.lock ../../../scripts/ci-checks.sh; then
      local mutation_status=0
      uv run --locked --with mutmut==3.8.1 mutmut run --max-children 2 > "$reports/mutation.txt" 2>&1 || mutation_status=$?
      cat "$reports/mutation.txt"
      uv run --locked --with mutmut==3.8.1 mutmut export-cicd-stats
      if [[ -f mutants/mutmut-cicd-stats.json ]]; then
        cp mutants/mutmut-cicd-stats.json "$reports/mutation-stats.json"
      fi
      if [[ "$mutation_status" != 0 ]]; then
        return "$mutation_status"
      fi
      uv run --locked --with mutmut==3.8.1 mutmut results > "$reports/mutation-results.txt"
      cat "$reports/mutation-results.txt"
    fi
  fi
)

check_python_scripts() {
  uvx --from ruff==0.16.6 ruff check --target-version py311 scripts/schema_registry.py scripts/codegen-proto.py scripts/release.py
  uvx --from ruff==0.16.6 ruff format --check scripts/schema_registry.py scripts/codegen-proto.py scripts/release.py
}

check_go() (
  local reports="$report_dir/${1#src/}"
  mkdir -p "$reports"
  cd "$1"
  export GOWORK=off
  # A module that pins golangci-lint as a Go tool builds it against its own x/tools, which can read newer
  # toolchains' export data than the release binary.
  if go tool -n golangci-lint > /dev/null 2>&1; then
    go tool golangci-lint run ./...
  else
    golangci-lint run ./...
  fi
  go test -p 1 -race -count=1 -coverprofile "$reports/coverage.out" ./...
  go tool cover -func "$reports/coverage.out" | tee "$reports/coverage.txt"
  go tool cover -html "$reports/coverage.out" -o "$reports/coverage.html"
)

check_data_product() {
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
PYTEST_COV=7.0.0

check_sql_jobs() {
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
    report_dir="$(realpath -m "${CI_TEST_REPORTS:-$(mktemp -d)}")"
    coverage_config="$(realpath scripts/coverage.toml)"
    mkdir -p "$report_dir"
    if [[ -n "${2:-}" ]]; then
      check_python "$2"
    else
      check_python_scripts
      check_projects pyproject.toml check_python
    fi
    ;;
  python-scripts)
    check_python_scripts
    ;;
  helm)
    while IFS= read -r -d '' manifest; do
      check_chart "$(dirname "$manifest")"
    done < <(find infra -type f -name Chart.yaml -print0)
    check_sql_jobs || exit 1
    ;;
  *)
    printf 'Usage: %s {contracts|python [project]|python-scripts|go [module]|helm}\n' "$0" >&2
    exit 2
    ;;
esac
