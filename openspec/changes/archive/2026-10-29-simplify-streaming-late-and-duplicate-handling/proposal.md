## Why

The coursework requires late and duplicate events in the stream and processing that handles them. The current design simulates both with a separate `payments.events.v1.faulty` topic, a buffering forwarder and assessed-attempt matching in Flink. That is hard to reason about and does not show late data arriving the way it does in practice. Lateness and duplicates should originate where they occur in real systems, and every consumer should read one payment topic.

## What Changes

- **BREAKING** Remove `payments.events.v1.faulty`, the hold/burst forwarding harness and its bounded-backlog behavior.
- The live simulator holds a configured share of attempts for a configured delay (store-and-forward) and sends them with their original client click time.
- The live simulator re-publishes a configured share of committed payment events as byte-identical copies onto `payments.events.v1` (at-least-once redelivery).
- Edge accepts an optional `simulated_client.attempted_at` for synthetic integrations and rejects times after server time. Payments already records a supplied attempt time.
- Add `fraud-snapshot-v2` next to the unchanged v1 definition, with its own `features.v2` protobuf package, examples and reference calculation:
  - customer and method attempt counts over 5m/1h/24h;
  - customer distinct devices over 24h;
  - no assessed-only eligibility, 24h amount sum or 7-day features.
- **BREAKING** Flink counts unique attempt starts by client attempt time. Inputs older than the 24h horizon are counted as too late instead of failing the job. Start/decision matching is removed.
- Per-record data errors (undecodable or invalid events, invalid attempt context, conflicting duplicates) go to an exactly-once dead-letter topic `fraud.feature-rejects.v1.<epoch>` instead of crash-looping the job; invariant and system errors still fail it.
- The streaming output ends at the feature journal `fraud.feature-updates.v2.live-v18` and the dead-letter topic. Feast and the fraud service's feature retrieval are disabled in every environment and stay on v1 code; serving moves to the later feature-store change in the roadmap build order. Rules stay authoritative, and the Redis 10s/60s guard is unchanged.
- A new live feature epoch `live-v18` replaces v17. No existing feature data, offsets or checkpoints are migrated.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `streaming-fraud-features`: start-only event-time windows, too-late accounting, dedup bounded by the event-time horizon.
- `fraud-feature-contracts`: the v2 streaming definition and its conformance examples, alongside unchanged v1.
- `transport-fault-injection`: redelivery onto the shared topic replaces the faulty-topic forwarder.
- `checkout-api`: optional synthetic client attempt time.
- `live-traffic-simulation`: store-and-forward attempts and late/duplicate evidence without the archiver.
- `talos-gitops-deployment`: the simulator workload embeds late and duplicate injection; no archiver statement.
- `focused-verification`: redelivery verification replaces delivery-fault verification.
- `fraud-decision`: historical-feature retrieval is disabled until the feature-store change, and shadow-only once enabled.

## Impact

- Contracts: `contracts/features/fraud-snapshot-v2.*` and `contracts/features/v2/features.proto`, with Python bindings for the streaming project only.
- Data pipeline: `src/data-pipeline/streaming` (including the Flink image integration test, moved from the Feast project), `features/reference_v2.py`, the live simulator and its tests.
- Go: Edge HTTP attempt parsing only. Payments and Fraud are unchanged.
- Infra: Kafka topics, the schema-registration image, the fraud-features epoch and topic with Feast disabled, fraud feature retrieval disabled, and live-traffic settings.
- Docs: affected passages in `docs/data/online.md`, `docs/ml/feature-store.md` and `docs/payment-gateway/flows.md`.
- Out of scope: label rechecks/drain, the drift schedule and drift detection, HybridSource, Spark/Silver work (owned by `rework-simulator-spark-coursework`) and the other real/simulated Go service boundaries.
