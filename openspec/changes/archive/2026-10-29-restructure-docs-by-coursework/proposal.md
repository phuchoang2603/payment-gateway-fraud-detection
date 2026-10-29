## Why

The documentation grew around the earlier mini and final coursework stages and still described replaced pipelines (landing archiver, old DP1, v8–v17 sources). It is reorganized to follow the coursework proposal and rubric so each capability has one page for its explanation and evidence.

## What Changes

- `docs/` is reorganized into `overview/` (proposal, architecture, roadmap), `payment-gateway/` (architecture, flows, conventions, Docker images), `data/` (offline and online generators and pipelines, storage, orchestration and governance), `ml/` (feature store, training, serving, background), `llm/` (agents) and `deployment/` (CI, CD, infrastructure, security, observability). Stale and superseded content is removed; the README follows the rubric's README requirements.
- Generated data-product references move from `docs/data-ml/data-products/<product>/` to `docs/data/products/<product>/`, and CI checks the new path.
- The functional and non-functional requirements move into `docs/overview/proposal.md`.

## Capabilities

### New Capabilities

### Modified Capabilities

- `dataset-contracts`: the generated-reference location changes.
- `focused-verification`: the Go service acceptance requirement links the requirements at their new location.

## Impact

`docs/`, `README.md`, `scripts/ci-checks.sh`. No code or runtime behaviour changes.
