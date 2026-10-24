## 1. Topics

- [ ] 1.1 Add `payments.events.v1.faulty` (3 partitions, 7 days) and `fraud.labels.v1` (1 partition, 30 days) as connector-less entities in the gateway's `infra/charts/kafka` values and prod values; verify `helm template` renders three topics and no new connector

## 2. Simulator configuration and fault injector

- [ ] 2.1 Add httpx and confluent-kafka to `src/data-pipeline/simulator`, the pydantic model for `configs/live-v1.yaml` (`dataset`, `traffic`, `faults`, `kafka`) and the `live`/`faults` CLI entry points; verify `uv sync --locked` and that an invalid delay range is rejected at startup
- [ ] 2.2 Implement hash-based selection and delays for duplicate and late delivery, plus the wall-clock burst hold; verify a test that two runs over the same event IDs select the same events with the same delays
- [ ] 2.3 Implement the consume, schedule and produce loop with unchanged key/value pass-through, per-partition commits trailing the oldest unpublished offset, the `max_pending` pause and the periodic JSON summary; verify the all-faults-disabled pass-through test and the restart-with-held-records test against a CI Kafka service container

## 3. Live simulator

- [ ] 3.1 Implement dataset continuation: read the manifest, rebuild profiles and the scenario schedule with the generator's per-stage and per-day streams, and refuse on digest mismatch, start before history end or gap above `max_gap`; verify the digest-mismatch refusal test
- [ ] 3.2 Implement setup tokenization through `POST /v1/payment-methods` using each card's deterministic test card number, then seeded Poisson arrivals, payment creation with the full snapshot and attempts with seeded idempotency keys and simulated client context; reuse the original key/token/request on transport or 503/504 retries, verify one recorded attempt after a forced edge timeout and stable historical/live fingerprints after token reissuance, and exclude tokens from logs/manifests/labels
- [ ] 3.3 Implement truth labels and compressed chargebacks that check payment status when due, published to `fraud.labels.v1`; verify against the prod Kubernetes gateway after GitOps rollout that a pending attempt gets no chargeback and a succeeded fraudulent attempt does

## 4. Image and deployment

- [ ] 4.1 Add `infra/docker/simulator.Dockerfile` and a `simulator` job in `release.yml` using `build-image.yml`, and add the Kafka service container to the CI Python job; verify same-repository PR builds publish disposable PR tags, fork PRs build without publication, main builds publish main tags, and CI passes
- [ ] 4.2 Add `infra/charts/simulator` with `live` and `faults` Deployments (one replica, `Recreate`), disabled by default and explicitly enabled for bounded synthetic runs through the configured prod root; inject the synthetic integration's API key and Edge network access only into `live`; verify Helm rendering introduces no dev root and the fault injector has no gateway credentials

## 5. Evidence and documentation

- [ ] 5.1 In a controlled synthetic evidence run, generate a continuation dataset ending on the run date, run the simulator, and verify a historical customer's live attempts carry the same `customer_id` and fingerprints as its historical events
- [ ] 5.2 Run the injector with duplicate, late and burst enabled one at a time, then together; record the configured and observed summaries, and verify that per-payment version order in the clean topic and the Postgres histories are unchanged
- [ ] 5.3 Add `docs/data-ml/live-traffic-evidence.md` with configuration, seeds, commands, summaries, and the label join by `attempt_id`; update the roadmap #44 row, the fault-injection step in the data pipeline architecture doc and the gitops runtime table with the `simulator` application; link the page from the sidebar; verify links resolve
- [ ] 5.4 Run `openspec validate simulate-live-traffic-with-transport-faults --strict` and verify it passes
