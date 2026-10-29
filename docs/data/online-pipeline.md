# Online pipeline

A PyFlink job reads the gateway's committed payment events from Kafka, computes window features per customer and per payment method, and writes them to Feast's online store through an ordered writer. Go Fraud reads them in shadow mode; rules stay authoritative. Code: [`src/data-pipeline/streaming`](../../src/data-pipeline/streaming) and [`src/data-pipeline/feast`](../../src/data-pipeline/feast); deployment: [`infra/charts/fraud-features`](../../infra/charts/fraud-features).

**Status:** deployed in dev in shadow mode; disabled in prod. [PR #101](https://github.com/phuchoang2603/payment-gateway-fraud-detection/pull/101) moves the job to feature contract v2 (attempt starts keyed on client `attempted_at`, 5-minute, 1-hour and 24-hour windows, distinct devices over 24 hours) and adds explicit late-data and duplicate handling; this page is updated with it.

## Runtime

- Flink and PyFlink 2.2.1 on the platform's Flink operator, Kubernetes HA, checkpoints and savepoints in object storage under `runs/payment-gateway/flink/`.
- Parallelism one; each manager requests one CPU, the TaskManager 2 GiB.
- Each epoch has its own feature journal topic and state; a state layout change starts a fresh epoch instead of restoring incompatible state.
- The Feast writer runs as one replica, reads back accepted revisions, and only then commits Kafka offsets.

## Windows, late data and duplicates

| Concern | Handling |
|---|---|
| Windows | Per-customer and per-method counts over `[t − window, t)` with explicit per-window coverage; missing history is null, never zero |
| Late data | Event time from the attempt; attempts older than the longest window are counted and logged instead of updating state (PR #101) |
| Duplicates | Deduplication by event identity with bounded state retention |
| Restart | Checkpoint recovery restores offsets and state |

Long 7- and 30-day windows are computed offline by DP3, not by Flink; there is no HybridSource bootstrap. See [feature store](../ml/feature-store.md).

Evidence (Flink UI captures, baseline versus handled late and duplicate cases, window code walkthrough) is added here with PR #101's acceptance.
