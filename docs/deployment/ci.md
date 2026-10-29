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

## Test design

- Tests protect business rules, boundaries, failure modes and integration contracts; there are no configuration-only or getter tests.
- Equivalence partitions and boundary values parametrize rule tests, for example the fraud policy's amount and account-age boundaries and the generator's invalid schema schedules.
- Property-style determinism checks: the generator produces equal records across worker counts, shard splits and interruption.
- Go mutation testing runs per module in CI.

**Pending evidence:** coverage reports, mutation scores on changed code (target above 80%), property-based idempotency tests with Hypothesis, and a Locust load test of the feature-retrieval API. Captures and links are added here as they land.
