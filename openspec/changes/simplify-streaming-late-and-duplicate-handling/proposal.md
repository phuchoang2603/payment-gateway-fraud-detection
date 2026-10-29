## Why

The coursework requires late and duplicate events in the stream and processing that handles them. The current design simulates both with a separate `payments.events.v1.faulty` topic, a buffering forwarder and assessed-attempt matching in Flink. That is hard to reason about and does not show late data arriving the way it does in practice. Lateness and duplicates should originate where they occur in real systems, and every consumer should read one payment topic.

## What Changes

- **BREAKING** Remove `payments.events.v1.faulty`, the hold/burst forwarding harness and its bounded-backlog behavior.
- The live simulator holds a configured share of attempts for a configured delay (store-and-forward) and sends them with their original client click time.
- The live simulator re-publishes a configured share of committed payment events as byte-identical copies onto `payments.events.v1` (at-least-once redelivery).
- Edge accepts an optional `simulated_client.attempted_at` for synthetic integrations and rejects times after server time. Payments already records a supplied attempt time.
- **BREAKING** Replace `fraud-snapshot-v1` with `fraud-snapshot-v2`:
  - customer and method attempt counts over 5m/1h/24h;
  - customer distinct devices over 24h;
  - no assessed-only eligibility, 24h amount sum or 7-day features.
- **BREAKING** Flink counts unique attempt starts by client attempt time. Inputs older than the 24h horizon are counted as too late instead of failing the job. Start/decision matching is removed.
- Feast views, the Go fraud Feast client and assessment evidence move to the v2 values. The Redis 10s/60s guard is unchanged.
- A new live feature epoch and journal topic replace the v17 epoch. No existing feature data, offsets or checkpoints are migrated.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `streaming-fraud-features`: start-only event-time windows, too-late accounting, dedup bounded by the event-time horizon.
- `fraud-feature-contracts`: v2 feature set and eligibility.
- `transport-fault-injection`: redelivery onto the shared topic replaces the faulty-topic forwarder.
- `checkout-api`: optional synthetic client attempt time.
- `live-traffic-simulation`: store-and-forward attempts and late/duplicate evidence without the archiver.
- `talos-gitops-deployment`: the simulator workload embeds late and duplicate injection; no archiver statement.
- `focused-verification`: redelivery verification replaces delivery-fault verification.

## Impact

- Contracts: `contracts/features/` definition, examples and `features/v1/features.proto` with generated Go/Python code.
- Data pipeline: `src/data-pipeline/streaming`, `features/reference.py`, Feast schema/writer, the live simulator and its tests.
- Go: Edge HTTP attempt parsing and the fraud Feast adapter/evidence. Payments, Fraud policy and the velocity guard are unchanged.
- Infra: Kafka topics, the fraud-features epoch/definition hash and live-traffic settings.
- Docs: affected passages in the data-pipeline, ML and payment-flow guides.
- Out of scope: label rechecks/drain, the drift schedule and drift detection, HybridSource, Spark/Silver work (owned by `rework-simulator-spark-coursework`) and the other real/simulated Go service boundaries.
