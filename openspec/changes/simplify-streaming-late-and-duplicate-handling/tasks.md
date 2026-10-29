## 1. Feature contract v2

- [x] 1.1 Replace `fraud-snapshot-v1` definition/examples with `fraud-snapshot-v2` (customer 5m/1h/24h counts + 24h distinct devices; method 5m/1h/24h counts; start-only eligibility; late/too-late examples) and update `features/reference.py` and its conformance test
- [x] 1.2 Add `contracts/features/v2/features.proto` for the v2 journal message and generate the streaming descriptor only; keep `features.v1`, its bindings and the v1 reference unchanged, with the v2 reference in `features/reference_v2.py`

## 2. Flink

- [x] 2.1 Decode only attempt starts with client attempt time; remove start/decision matching and 7-day state
- [x] 2.2 Implement 5m/1h/24h windows and 24h distinct devices; dedup state expiring by event time; too-late counting/logging without job failure
- [x] 2.3 Update streaming state/runtime tests and the image checkpoint integration test for duplicates, late inside/beyond horizon and restart

## 3. Journal-only output

- [x] 3.1 Move the Flink image integration test into the streaming project with v2 decoding, leaving Feast unchanged
- [x] 3.2 Publish to `fraud.feature-updates.v2.<epoch>` with its own registry subject; disable Feast and the fraud service's feature retrieval in every environment

## 4. Edge client attempt time

- [x] 4.1 Parse optional `simulated_client.attempted_at` for synthetic integrations, reject times after server time, pass it to Payments; add HTTP tests and update `payment-flows.md`

## 5. Live simulator

- [x] 5.1 Add seeded store-and-forward holding with bounded held attempts and evidence; send `attempted_at` for held attempts
- [x] 5.2 Replace the fault forwarder with the redelivery loop on `payments.events.v1`; replace its tests

## 6. Infra and docs

- [x] 6.1 Remove `payments.events.v1.faulty` and its metadata pattern; add the `live-v18` v2 journal topic; update the fraud-features epoch and topic and live-traffic settings
- [x] 6.2 Update affected data-pipeline/ML doc passages and run local lint/format/helm/OpenSpec checks

## 7. Dead-letter path

- [x] 7.1 Classify per-record data errors (`Rejected`) in decode and both dedup stages; keep invariant and system errors fatal and too-late attempts on `too_late`
- [x] 7.2 Route rejects through one side output to an exactly-once `fraud.feature-rejects.v1.<epoch>` sink with source-coordinate headers, a per-class `rejected` metric and a log line
- [x] 7.3 Declare the topic in the Kafka chart; cover malformed and conflicting records and a fatal invariant in the existing runtime and Kafka checkpoint tests
- [x] 7.4 Update the spec, design and the "Poison records" row in `docs/data/online.md`

## 8. Coursework evidence

- [x] 8.1 Add `duplicates_dropped` per dedup scope and bucketed lateness counters (`lt_1m`, `1m_5m`, `5m_15m`, `15m_24h`, `too_late`) to the streaming job, with shared classification used by the job and the benchmark
- [x] 8.2 Add the seeded replay benchmark `fraud_streaming.benchmark` with handled, no-dedup, arrival-time and baseline variants compared with `reference_v2`, plus a CI test
- [x] 8.3 Document the window code, metrics and benchmark in `docs/data/online.md`
- [ ] 8.4 Record the benchmark report in `docs/data/online.md` from a CI-produced run (`cd src/data-pipeline/streaming && PYTHONPATH=.. uv run --locked python -m fraud_streaming.benchmark`); keep the cells Pending until then
- [ ] 8.5 Dev live verification (needs explicit approval before running):
  1. Confirm in dev that Argo synced this change, that `fraud.feature-updates.v2.live-v18` and `fraud.feature-rejects.v1.live-v18` exist, and that the FlinkDeployment `fraud-streaming-live-v18` in namespace `data` is RUNNING with a completed checkpoint. Record the image tag and Flink job ID.
  2. Through a trusted dev-only PR, set in `infra/charts/live-traffic/values-dev.yaml`: `simulator.enabled: true`, `config.duration_seconds: 3600`, `payments_per_second: 1`, `late_rate: 0.05`, `late_min_seconds: 300`, `late_max_seconds: 600`, `redelivery.enabled: true`, `redelivery.rate: 0.015`. Merge, and record the commit SHA and the simulator's `run_id` from the `traffic_start` log in namespace `payment-gateway` (`kubectl -n payment-gateway logs deploy/live-simulator`).
  3. After the run, collect configured versus observed counts:
     - simulator `traffic_stop` (`late_sent`, `late_unsent`, `late_delay_seconds`) and `redelivery_stop` (`consumed`, `selected`, `acknowledged`, `observed_rate`) log lines;
     - Flink metrics `too_late`, `scope.event.duplicates_dropped`, `scope.attempt.duplicates_dropped`, `lateness.*.attempts` and `reject_class.*.rejected`.
  4. Port-forward the Flink UI (`kubectl -n data port-forward svc/fraud-streaming-live-v18-rest 8081`) and capture into `docs/data/images/`:
     - the job graph;
     - the checkpoints tab;
     - the dedup operator's metrics showing `too_late`, `duplicates_dropped`, lateness and `rejected`;
     - backpressure for each operator.
  5. Once Bronze Kafka mode is deployed, run it over the same offset range, and compute the duplicate delivery rate and the distribution of `recorded_at − attempted_at` from `bronze.payment_events`. Compare these with the simulator and Flink numbers.
  6. Disable the simulator again with a dev-only PR (`simulator.enabled: false`).
  7. Record run IDs, SHAs, the configured versus observed table and the screenshots in `docs/data/online.md`.
