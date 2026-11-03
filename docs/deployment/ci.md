# CI, validation and verification

GitHub Actions validates pull requests and `main`; successful checks gate [image publication and deployment](cd.md#release-flow). Tests and builds run in CI. Local checks run in `devenv shell` (format, lint, schema validation and Helm rendering).

## CI checks

| Workflow | Validation |
|---|---|
| [CI](../../.github/workflows/ci.yml) | Contract lint, generated-reference diff, Helm lint/render for all charts and values |
| [Go CI](../../.github/workflows/go-ci.yml) | Lint, unit tests, Postgres integration tests through testcontainers, changed-code mutation testing on PRs |
| [Python CI](../../.github/workflows/python-ci.yml) | Ruff lint/format and pytest per project |

[`scripts/ci-checks.sh`](../../scripts/ci-checks.sh) holds shared checks. On PRs, [`ci-matrix.py`](../../scripts/ci-matrix.py) selects affected projects and local dependencies; `main` tests every project.

## Test techniques

| FSDS criterion | Implementation example |
|---|---|
| API tests with fixtures and mocks | [Feast server tests](../../src/data-pipeline/feast/tests/test_server.py), [fixtures](../../src/data-pipeline/feast/tests/conftest.py): FastAPI client against a store double |
| Equivalence partitioning and boundary values | [Fraud policy](../../src/payment-gateway/fraud/internal/domain/policy_test.go): amount/account-age boundaries; [request features](../../src/data-pipeline/feast/tests/test_request.py): country pair, history and scoring hour |
| Mutation testing, >80% on changed code | [Go CI](../../.github/workflows/go-ci.yml); the recorded full-run results below separate killed mutants from untested mutants |
| Property-based idempotency | [Gold ranges](../../src/data-pipeline/batch/tests/gold/test_gold_ranges.py): reruns and range splits; [payment lifecycle](../../src/payment-gateway/payments/internal/domain/payment_property_test.go): arbitrary command sequences preserve state invariants |
| API throughput and latency | [Knowledge API Locust report](../llm/rag.md#load-test-and-autoscaling); Feast retrieval load-test evidence remains pending |

## Coverage and mutation results

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

Domain coverage is 87–100%; whole-module coverage includes transport, repositories and startup. These figures do **not** establish the rubric's >90% coverage target for every project. Captures of the CI test/coverage output are still missing; the tables record artifact results.

## Deployed verification

The 2026-10-23 acceptance run against prod is recorded in the [gateway verification evidence](../../openspec/changes/archive/2026-10-23-build-minimum-payment-gateway/verification.md): synthetic tokenization; approve, risk-decline, processor-decline, timeout-then-recovery and expiry paths; decline-then-approve through the hosted page with signed webhooks; Redis fallback; Connect restart and Kafka outage catch-up; byte-equal topic payloads; and the ledger lifecycle check. A short smoke run does not establish the monthly availability objective or the nominal load target.

On 2026-11-03, Edge canary acceptance covered no-data rejection, fault rejection and healthy promotion ([screenshots and thresholds](cd.md#edge-progressive-delivery)). The healthy run returned 14,400/14,400 checkout GETs as HTTP 200 over 20 minutes at 12 requests/s, with client p95 105 ms. This measures checkout reads, not feature-retrieval capacity or payment processing.
