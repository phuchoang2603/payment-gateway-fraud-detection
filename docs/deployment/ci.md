# Continuous integration

GitHub Actions runs every check on pull requests and `main`. Tests and image builds run only in CI; local work is limited to non-test checks (lint, format, schema validation, Helm rendering, `openspec validate --strict`) inside `devenv shell`.

## Workflows

| Workflow | Jobs |
|---|---|
| [`ci.yml`](../../.github/workflows/ci.yml) | Contract lint and generated-reference diff, Helm lint and render of every chart and values file |
| [`go-ci.yml`](../../.github/workflows/go-ci.yml) | Per-module Go lint (golangci-lint, gofumpt), tests including Postgres integration tests through testcontainers, and mutation testing |
| [`python-ci.yml`](../../.github/workflows/python-ci.yml) | Ruff lint and format and pytest for each Python project |
| [`release.yml`](../../.github/workflows/release.yml) and [`build-image.yml`](../../.github/workflows/build-image.yml) | Release planning, affected image builds, and publication of the dev or prod release; see [CD](cd.md) |
| [`wiki.yml`](../../.github/workflows/wiki.yml) | Publishes `docs/` to the project wiki from `main` |

Check logic lives in [`scripts/ci-checks.sh`](../../scripts/ci-checks.sh) so workflow steps stay short.

On pull requests, the Go and Python workflows test only the projects the change affects: [`scripts/ci-matrix.py`](../../scripts/ci-matrix.py) keeps a project when the diff touches its directory, a local dependency its manifest declares (Go `replace` paths, uv workspace and path sources, pytest import paths), `contracts/`, `scripts/` or the workflows. Pushes to `main` test every project.

## Test design

- Tests protect business rules, boundaries, failure modes and integration contracts; there are no configuration-only or getter tests.
- Equivalence partitions and boundary values parametrize rule tests, for example the fraud policy's amount and account-age boundaries and the generator's invalid schema schedules.
- Property-style determinism checks: the generator produces equal records across worker counts, shard splits and interruption.
- Property-based tests generate the inputs and check invariants instead of fixed examples (Hypothesis in Python, `rapid` in Go):
  - **Idempotent ranges:** any split of a Silver or Gold range into consecutive runs, followed by any rerun of part of it, builds the same tables as one backfill; the daily schedule is always one of the examples.
  - **Streaming windows:** any arrival sequence with late starts, redeliveries and checkpoint restores matches the reference snapshot.
  - **Payment lifecycle:** any command sequence keeps one active attempt, moves phases forward only, accepts nothing after a terminal status and leaves the state untouched when it rejects a command.
  - **Service rules:** fraud rules fire exactly when their conditions hold and never flip with a higher amount; backoff stays within its cap; webhook signatures change with every input; fingerprints ignore formatting only; card numbers pass Luhn exactly when valid.
- Go mutation testing runs on the changed code of each pull request.
- The Feast request view and the feature server are tested with fixtures and mocks: the server through FastAPI's test client against a Feast store double, and the view through equivalence partitions and boundary values of the country pair, account age, 30-day history and scoring hour, plus a Hypothesis property that the same request always gives the same features.

## Evidence

Coverage from CI (`python-test-evidence-*` and `go-test-evidence-*` artifacts), 2026-11-01. Go counts only handwritten code; sqlc and protobuf output is excluded.

| Project | Line coverage | Branch coverage |
|---|---|---|
| data-pipeline/streaming | 89.0% | 84.1% |
| data-pipeline/simulator | 87.1% | 78.4% |
| data-pipeline/batch | 86.1% | 70.6% |
| data-pipeline/feast | 93.4% | 87.2% |
| data-pipeline/openmetadata | 50.2% | 39.7% |
| llm/rag_serving | 97.1% | 90.0% |
| llm/common | 68.8% | 45.0% |
| llm/rag_pipeline | 61.7% | 73.6% |

| Go module | Domain package | Whole module | Mutation (full run, 2026-10-26) |
|---|---|---|---|
| payments | 92.5% | 55.0% | 269 killed, 0 lived, 106 not covered: efficacy 100%, mutator coverage 71.7% |
| fraud | 100% | 60.3% | 11 killed, 0 lived: 100%, 100% |
| notifier | 88.3% | 50.6% | 68 killed, 0 lived, 25 not covered: 100%, 73.1% |
| edge | 87.3% | 59.6% | 42 killed, 0 lived, 51 not covered: 100%, 45.2% |
| vault-simulator | 90.0% | 48.2% | 29 killed, 0 lived, 17 not covered: 100%, 63.0% |
| processor-simulator | 100% | 37.2% | 6 killed, 0 lived, 40 not covered: 100%, 13.0% |
| shared | | 42.1% | 64 killed, 0 lived, 25 not covered: 100%, 71.9% |

Every mutant that a test reaches is killed. The business rules sit in the domain packages, which reach 87–100%; the rest of each module is gRPC and HTTP wiring, repositories covered by the Postgres integration tests, and process startup, which the per-module totals include.

**Pending evidence:** a Locust load test of the feature-retrieval API, once Feast retrieval is enabled.
