## Why

#44 needs live payment traffic that continues the historical dataset, plus configurable burst, late-arrival and duplicate delivery for the streaming work in #49. Strict streaming forbids fabricating payment events, so traffic must come through gateway commands and every delivery fault must be applied after Payments has committed events. `build-minimum-payment-gateway` already publishes committed events to `payments.events.v1`, but nothing generates live traffic or delivery faults.

## What Changes

- Add a `live` simulator command. It rebuilds the profiles and scenario schedule of a historical dataset from its manifest, then drives clean gateway commands at a configured rate on wall-clock time. It publishes simulation-truth labels and compressed-delay chargeback labels to `fraud.labels.v1`. The client never injects faults on purpose.
- Obtain opaque tokens during setup by posting each fictional card's deterministic test card number to the gateway's synthetic-only `POST /v1/payment-methods`; business/scenario selection remains seeded, while token values are random and never logged or published. The live client has no vault credentials and cannot resolve fixtures or bypass checkout.
- Add a `faults` command, a topic-to-topic fault injector from `payments.events.v1` to `payments.events.v1.faulty`. It applies burst holds, late delivery and duplicate delivery, each independently configured. Faults are selected by a seeded hash of the event ID, so the same events are affected on every run. The clean topic stays as ground truth.
- Add the `payments.events.v1.faulty` and `fraud.labels.v1` topics to the gateway's `kafka` chart, and a simulator image and chart with opt-in workloads under the configured prod root; do not restore the retired dev root.
- Rewrite #44 around this scope. Drift moves to the final-coursework extension.

## Capabilities

### New Capabilities

- `live-traffic-simulation`: Seeded live gateway traffic that continues a historical dataset's entities and scenarios, with live label delivery.
- `transport-fault-injection`: Deterministic burst, late and duplicate delivery between the clean and the faulty event topics, with observed rates.

### Modified Capabilities

- `talos-gitops-deployment`: Adds the faulty and labels topics, simulator workloads and the simulator image.
- `focused-verification`: Adds fault-injection and dataset-continuation behavior tests.

## Impact

- `src/data-pipeline/simulator`: `live` and `faults` commands (httpx, confluent-kafka), `configs/live-v1.yaml`.
- `infra/charts/kafka` gains two topics; new `infra/charts/simulator`, `infra/docker/simulator.Dockerfile`, a `simulator` image job and an app-of-apps child.
- Depends on `build-minimum-payment-gateway` (append-only event ledger, Kafka cluster with `payments.events.v1` publication, edge with the payment-method route, deterministic identities, synthetic fingerprint key) and the archived `generate-historical-synthetic-data` (`historical-data-generation` spec: reproducible profiles and manifest digest).
- Docs: `docs/data-ml/live-traffic-evidence.md`, roadmap #44 row, and the fault-injection step in the data pipeline architecture doc.
- Not included: Flink windows and deduplication (#49), drift scenarios (final coursework), Kafka authentication (#63), marketplace traffic (#41).
