## 1. Feature contract v2

- [x] 1.1 Replace `fraud-snapshot-v1` definition/examples with `fraud-snapshot-v2` (customer 5m/1h/24h counts + 24h distinct devices; method 5m/1h/24h counts; start-only eligibility; late/too-late examples) and update `features/reference.py` and its conformance test
- [x] 1.2 Renumber `features/v1/features.proto` for v2 values, coverage and evidence; drop `computed_at`/`published_at`; regenerate Go and Python bindings and the streaming descriptor

## 2. Flink

- [x] 2.1 Decode only attempt starts with client attempt time; remove start/decision matching and 7-day state
- [x] 2.2 Implement 5m/1h/24h windows and 24h distinct devices; dedup state expiring by event time; too-late counting/logging without job failure
- [x] 2.3 Update streaming state/runtime tests and the image checkpoint integration test for duplicates, late inside/beyond horizon and restart

## 3. Feast and Go fraud

- [x] 3.1 Update Feast schema/views and writer tests for v2 values
- [x] 3.2 Update the Go fraud Feast client, application values and evidence for v2; remove 7-day logic; update tests

## 4. Edge client attempt time

- [x] 4.1 Parse optional `simulated_client.attempted_at` for synthetic integrations, reject times after server time, pass it to Payments; add HTTP tests and update `payment-flows.md`

## 5. Live simulator

- [x] 5.1 Add seeded store-and-forward holding with bounded held attempts and evidence; send `attempted_at` for held attempts
- [x] 5.2 Replace the fault forwarder with the redelivery loop on `payments.events.v1`; replace its tests

## 6. Infra and docs

- [x] 6.1 Remove `payments.events.v1.faulty` and its metadata pattern; add the `live-v18` journal topic; update fraud-features epoch/hash/writer group and live-traffic settings
- [x] 6.2 Update affected data-pipeline/ML doc passages and run local lint/format/helm/OpenSpec checks

## 7. Dead-letter path

- [x] 7.1 Classify per-record data errors (`Rejected`) in decode and both dedup stages; keep invariant and system errors fatal and too-late attempts on `too_late`
- [x] 7.2 Route rejects through one side output to an exactly-once `fraud.feature-rejects.v1.<epoch>` sink with source-coordinate headers, a per-class `rejected` metric and a log line
- [x] 7.3 Declare the topic in the Kafka chart; cover malformed and conflicting records and a fatal invariant in the existing runtime and Kafka checkpoint tests
- [x] 7.4 Update the spec, design and the "Poison records" row in `docs/data/online.md`
