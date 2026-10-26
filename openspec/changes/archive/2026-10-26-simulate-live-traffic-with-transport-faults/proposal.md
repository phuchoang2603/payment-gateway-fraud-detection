## Why

The gateway already publishes committed payment events, but the platform needs repeatable live traffic using the historical entities, delivery-fault experiments, and a durable input for recurring DP1 batches. Build this with one Python simulator workload and a Python Kafka-to-Parquet archiver from the same package/image; retain the existing Connect worker for authoritative CDC.

## What Changes

- Add a Python live runner that reuses historical profile/identity and behavior logic, drives gateway HTTP/JSON commands, and starts traffic at current machine time. Remove maximum-gap restrictions and requirements to regenerate history to match today.
- Adapt the vault's synthetic card mapping to all 3.9 million v8 card identities preserved in v12. **BREAKING:** replace obsolete fixture assumptions without a legacy compatibility path; opaque token issuance remains gateway-owned.
- Generate Python protobuf bindings from the same root `contracts/` used by Go. Package event/label bindings with Python consumers and the simulator; retain protobuf Kafka payloads and existing gRPC contracts, with no Avro migration or schema registry.
- Run traffic generation, label scheduling and an optional post-commit delivery-fault loop in one process. The fault loop forwards unchanged committed events from `payments.events.v1` to `payments.events.v1.faulty` with seeded delay, duplication and burst holds.
- Add a Python archiver that decodes root-contract protobuf into historical-compatible payment-event and label Parquet business columns under `s3://synthetic-source/streaming/`. Retain original bytes and Kafka provenance in reserved metadata. Bound batches by records, bytes and time, publish immutable content-addressed objects before committing offsets, and allow honest at-least-once replay. Reuse the existing synthetic-source identity with a streaming-prefix policy extension and no deletion. No S3 Connect plugin, custom transform, registry or Flink deployment is required.
- Use one current environment, shared codebase and common configuration. Synthetic historical/live truth is valid for analytics and training; exclude scenario/oracle-only fields from features and enforce as-of label availability.
- Define a landing interface for later scheduled bounded DP1 ingestion into bronze, followed by DP2 silver/gold and DP3 features. DP1 multi-source configuration, incremental commits and contextual runtime validation implementation are deferred to the next slice. Historical exports/backfills remain another input mode of reusable DP1. Preserve immutable sources, published contracts/config bands and all previous evidence.

## Capabilities

### New Capabilities

- `live-traffic-simulation`: Current-time gateway traffic using historical identities, protobuf labels, generated Python contracts and the completed-file landing interface to DP1.
- `transport-fault-injection`: Optional bounded delivery faults on committed Kafka events, embedded in the live workload with at-least-once recovery.

### Modified Capabilities

- `talos-gitops-deployment`: One simulator workload, clean/faulty/label topics and a Python archiver workload.
- `focused-verification`: Cross-language contracts, identity continuation, fault recovery, historical-compatible Parquet landing and recovery evidence.

## Impact

- `src/data-pipeline/simulator`: reusable historical model modules, live runtime, protobuf transport bindings and common live configuration; httpx, confluent-kafka and the protobuf runtime.
- `contracts/` and existing codegen/build/CI integration: Python generation alongside Go from the same source definitions; no parallel hand-maintained schema.
- Vault synthetic card mapping and its affected callers/verification; payment event authority and gateway APIs remain unchanged.
- Use the shared Python image for an independently enabled archiver workload with existing synthetic-source credentials, prefix-scoped policy access and object-store egress. Existing CDC Connect remains unchanged; delivery faults stay embedded in the Python workload.
- This change owns simulator and Kafka landing implementation/interface alignment. DP1 owns bronze implementation and runtime validation-policy changes. Generator distribution/cardinality bands are informational diagnostics under that planned policy; integrity/conservation/schema/contract/lifecycle-context/linkage/leakage remain enforced.
- Out of scope: Avro/registry, virtual time and historical catch-up, separate fault service, actual-production isolation, drift extensions and duplicate rewriting/regeneration of v8/v11/v12.
