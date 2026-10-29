## Why

The functional and non-functional requirement tables in the domain proposal restated design goals without adding decisions, and the data documentation split each problem's generation and solution across two pages. The proposal keeps the domain, data problems and AI problem; each data path gets one page that shows how a generated problem is solved.

## What Changes

- Remove the functional and non-functional requirement tables from `docs/overview/proposal.md`.
- Merge `docs/data/offline-generator.md` and `offline-pipeline.md` into `docs/data/offline.md`, and `online-generator.md` and `online-pipeline.md` into `docs/data/online.md`, each with one table mapping a data problem to how it is generated, how it is solved and its evidence.

## Capabilities

### New Capabilities

### Modified Capabilities

- `focused-verification`: the Go service acceptance requirement no longer links the removed requirement tables.

## Impact

`docs/` only. No code or runtime behaviour changes.
