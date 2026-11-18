#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

DATACONTRACT_CLI=1.2.7
DATAPRODUCT_CLI=0.3.2
PYTEST_COV=7.0.0

report_dir="$(realpath -m "${CI_TEST_REPORTS:-$(mktemp -d)}")"
coverage_config="$(realpath scripts/coverage.toml)"
mkdir -p "$report_dir"

check_projects() {
  local manifest="$1" check="$2" project
  while IFS= read -r -d '' project; do
    project="$(dirname "$project")"
    printf '\nChecking %s\n' "$project"
    "$check" "$project"
  done < <(git ls-files -z -- "src/**/$manifest")
}

has_own_devenv() {
  [[ -n "${DEVENV_ROOT:-}" && -f "$1/devenv.yaml" && "$DEVENV_ROOT" != "$PWD/$1" ]]
}

in_project_devenv() {
  local mode="$1" project="$2"
  (cd "$project" && CI_TEST_REPORTS="$report_dir" devenv shell -- bash "$OLDPWD/scripts/ci-checks.sh" "$mode" "$project")
}

check_python() (
  if has_own_devenv "$1"; then
    in_project_devenv python "$1"
    return
  fi
  local reports="$report_dir/${1#src/}"
  mkdir -p "$reports"
  cd "$1"
  uv run --locked ruff check .
  uv run --locked ruff format --check .
  if [[ -d tests ]]; then
    local floor
    floor="$(python3 -c 'import tomllib; print(tomllib.load(open("pyproject.toml", "rb")).get("tool", {}).get("ci", {}).get("coverage-fail-under", 0))')"
    COVERAGE_FILE="$reports/.coverage" uv run --locked --with "pytest-cov==$PYTEST_COV" pytest -q --durations=10 \
      --cov=. --cov-config="$coverage_config" --cov-fail-under="$floor" --cov-report=term \
      --cov-report="xml:$reports/coverage.xml" --cov-report="html:$reports/coverage-html"
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
  uvx --from ruff==0.16.6 ruff check --target-version py311 scripts/codegen-proto.py scripts/release.py
  uvx --from ruff==0.16.6 ruff format --check scripts/codegen-proto.py scripts/release.py
}

check_go() (
  local reports="$report_dir/${1#src/}"
  mkdir -p "$reports"
  cd "$1"
  export GOWORK=off
  if go tool -n golangci-lint > /dev/null 2>&1; then
    go tool golangci-lint run ./...
  else
    golangci-lint run ./...
  fi
  go test -p 1 -race -count=1 -coverprofile "$reports/coverage.out" ./...
  go tool cover -func "$reports/coverage.out" | tee "$reports/coverage.txt"
  go tool cover -html "$reports/coverage.out" -o "$reports/coverage.html"
)

check_go_mutations() (
  local project="${1:?Set the Go module directory}" mode="${2:-diff}"
  : "${CI_TEST_REPORTS:?Set CI_TEST_REPORTS to the CI artifact directory}"
  local mutation_reports
  mutation_reports="$(realpath -m "$CI_TEST_REPORTS")/${project#src/}"
  mkdir -p "$mutation_reports"
  cd "$project"

  local module_name exclude
  module_name="$(basename "$PWD")"
  exclude="(^|/)(proto|sqlcgen|testutil|testsupport|testvectors|db|config|metrics|transport|app)/|(^|/)cmd/${module_name}/|(_test|\.pb|_grpc\.pb)\.go$"
  local args=() candidates file eligible=false
  case "$mode" in
    diff)
      : "${MUTATION_BASE:?Set MUTATION_BASE to the reviewed base commit}"
      args+=(--diff "$MUTATION_BASE")
      candidates="$(git -c diff.relative=true diff --merge-base --name-only --diff-filter=ACMR "$MUTATION_BASE" -- '*.go')"
      ;;
    full)
      candidates="$(git ls-files -- '*.go')"
      ;;
    *)
      printf 'Usage: %s go-mutations <module> {diff|full}\n' "$0" >&2
      return 2
      ;;
  esac

  while IFS= read -r file; do
    if [[ "$file" == *.go && -f "$file" && ! "$file" =~ $exclude ]]; then
      eligible=true
      break
    fi
  done <<< "$candidates"
  if [[ "$eligible" == false ]]; then
    printf 'Skipped %s: no eligible Go source in %s scope.\n' "$project" "$mode" | tee "$mutation_reports/mutation.txt"
    if [[ -n "${GITHUB_STEP_SUMMARY:-}" ]]; then
      cat "$mutation_reports/mutation.txt" >> "$GITHUB_STEP_SUMMARY"
    fi
    return 0
  fi

  local mutation_dir status=0
  mutation_dir="$(mktemp -d)"
  trap 'rm -rf "$mutation_dir"' EXIT
  cp go.mod "$mutation_dir/go.mod"
  cp go.sum "$mutation_dir/go.sum"
  if [[ -d ../shared && "$(basename "$PWD")" != shared ]]; then
    go mod edit -modfile "$mutation_dir/go.mod" \
      -replace "github.com/phuchoang2603/payment-gateway-fraud-detection/shared=$(cd ../shared && pwd)"
  fi
  GOWORK=off GOFLAGS="-p=1 -modfile=$mutation_dir/go.mod" \
    GIT_CONFIG_COUNT=1 GIT_CONFIG_KEY_0=diff.relative GIT_CONFIG_VALUE_0=true \
    timeout --signal=TERM --kill-after=30s "${MUTATION_BUDGET:-15m}" \
    gremlins unleash "${args[@]}" --exclude-files "$exclude" --workers 2 --test-cpu 1 \
    --output "$mutation_reports/mutation.json" | tee "$mutation_reports/mutation.txt" || status=$?
  if [[ -n "${GITHUB_STEP_SUMMARY:-}" ]]; then
    {
      printf '\n### Mutation report: %s (%s)\n\n```text\n' "$project" "$mode"
      tail -n 18 "$mutation_reports/mutation.txt"
      printf '\n```\n'
    } >> "$GITHUB_STEP_SUMMARY"
  fi
  return "$status"
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

check_contracts() {
  TMPDIR_CHECKS="$(mktemp -d)"
  trap 'rm -rf "$TMPDIR_CHECKS"' EXIT
  local product
  while IFS= read -r -d '' product; do
    check_data_product "$(dirname "$product")"
  done < <(find contracts/data-products -mindepth 2 -maxdepth 2 -name product.odps.yaml -print0 | sort -z)
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

check_helm() {
  local manifest
  while IFS= read -r -d '' manifest; do
    check_chart "$(dirname "$manifest")"
  done < <(find infra -type f -name Chart.yaml -print0)
  check_sql_jobs
}

usage() {
  printf 'Usage: %s [all|go [module]|go-mutations <module> {diff|full}|python [project]|python-scripts|contracts|helm]\n' "$0" >&2
  exit 2
}

case "${1:-all}" in
  all)
    check_python_scripts
    check_projects go.mod check_go
    check_projects pyproject.toml check_python
    check_contracts
    check_helm
    ;;
  go)
    if [[ -n "${2:-}" ]]; then
      check_go "$2"
    else
      check_projects go.mod check_go
    fi
    ;;
  go-mutations)
    check_go_mutations "${2:-}" "${3:-diff}"
    ;;
  python)
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
  contracts)
    check_contracts
    ;;
  helm)
    check_helm
    ;;
  *)
    usage
    ;;
esac
