#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

PYTEST_COV=7.0.0

report_dir="$(realpath -m "${CI_TEST_REPORTS:-$(mktemp -d)}")"
coverage_config="$(realpath scripts/coverage.toml)"
mkdir -p "$report_dir"

summarize() {
  [[ -n "${GITHUB_STEP_SUMMARY:-}" ]] || return 0
  local title="$1" file="$2"
  {
    printf '\n### %s\n\n```text\n' "$title"
    cat "$file"
    printf '\n```\n'
  } >> "$GITHUB_STEP_SUMMARY"
}

each_project() {
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
  (cd "$project" && CI_TEST_REPORTS="$report_dir" devenv shell -- bash "$OLDPWD/scripts/check.sh" "$mode" "$project" "${@:3}")
}

check_python() (
  [[ -d "$1/tests" ]] || return 0
  if has_own_devenv "$1"; then
    in_project_devenv python "$@"
    return
  fi
  local reports="$report_dir/${1#src/}"
  mkdir -p "$reports"
  cd "$1"
  local floor
  floor="$(python3 -c 'import tomllib; print(tomllib.load(open("pyproject.toml", "rb")).get("tool", {}).get("ci", {}).get("coverage-fail-under", 0))')"
  COVERAGE_FILE="$reports/.coverage" uv run --locked --with "pytest-cov==$PYTEST_COV" pytest -q --durations=10 \
    --cov=. --cov-config="$coverage_config" --cov-fail-under="$floor" --cov-report=term \
    --cov-report="xml:$reports/coverage.xml" --cov-report="html:$reports/coverage-html" "${@:2}" | tee "$reports/pytest.txt"
  summarize "Python tests: $1" "$reports/pytest.txt"
  local group
  for group in $(python3 -c 'import tomllib; print(" ".join(tomllib.load(open("pyproject.toml", "rb")).get("tool", {}).get("ci", {}).get("test-groups", [])))'); do
    uv run --locked --no-default-groups --group dev --group "$group" pytest -q --durations=5
  done
  if [[ "$1" == src/llm/rag_serving && -n "${MUTATION_BASE:-}" ]] &&
    ! git diff --quiet "$MUTATION_BASE" HEAD -- fraud_rag_serving/app.py tests pyproject.toml ../common ../uv.lock ../../../scripts/check.sh; then
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
    summarize "Mutation results: $1" "$reports/mutation-results.txt"
  fi
)

check_go() (
  local reports="$report_dir/${1#src/}"
  mkdir -p "$reports"
  cd "$1"
  export GOWORK=off
  go test -race -count=1 -coverprofile "$reports/coverage.out" ./...
  go tool cover -func "$reports/coverage.out" | tee "$reports/coverage.txt"
  go tool cover -html "$reports/coverage.out" -o "$reports/coverage.html"
  summarize "Go coverage: $1" "$reports/coverage.txt"
)

check_go_mutations() (
  local project="${1:?Set the Go module directory}" mode="${2:-diff}"
  local mutation_reports="$report_dir/${project#src/}"
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
    summarize "Mutation report: $project ($mode)" "$mutation_reports/mutation.txt"
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
  tail -n 18 "$mutation_reports/mutation.txt" > "$mutation_reports/mutation-tail.txt"
  summarize "Mutation report: $project ($mode)" "$mutation_reports/mutation-tail.txt"
  return "$status"
)

check_jvm() (
  if has_own_devenv src/data-pipeline/streaming; then
    in_project_devenv jvm src/data-pipeline/streaming
    return
  fi
  cd src/data-pipeline/streaming
  mkdir -p jvm/target
  PYTHONPATH=.:.. uv run --locked --no-default-groups --group dev python jvm/parity_fixture.py jvm/target/parity.json
  mvn -B -f jvm/pom.xml verify
)

usage() {
  printf 'Usage: %s [all|go [module]|go-mutations <module> {diff|full}|python [project [pytest args]]|jvm]\n' "$0" >&2
  exit 2
}

case "${1:-all}" in
  all)
    each_project go.mod check_go
    each_project pyproject.toml check_python
    check_jvm
    ;;
  go)
    if [[ -n "${2:-}" ]]; then
      check_go "$2"
    else
      each_project go.mod check_go
    fi
    ;;
  go-mutations)
    check_go_mutations "${2:-}" "${3:-diff}"
    ;;
  python)
    if [[ -n "${2:-}" ]]; then
      check_python "${@:2}"
    else
      each_project pyproject.toml check_python
    fi
    ;;
  jvm)
    check_jvm
    ;;
  *)
    usage
    ;;
esac
