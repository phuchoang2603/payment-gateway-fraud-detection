# Gateway verification evidence

Run on 2026-10-23 against the prod cluster with Argo CD syncing branch `feat/build-minimum-payment-gateway` (PR 82). All checks used the deployed workloads, CNPG cluster, Redis, Kafka, Debezium Connect and notifier in namespace `payment-gateway`; no service, database or broker was started locally. Edge JSON APIs were called from a temporary pod in the `ecommerce` namespace because the AuthorizationPolicy admits only that namespace and the ingress gateway.

## Functional smoke matrix

Synthetic integration `01234567-89ab-4cde-8fab-0123456789ab` through `POST /v1/payment-methods`, `POST /v1/payments` and `POST /v1/checkout/sessions/{session}/attempts`:

| Scenario | Observed |
| --- | --- |
| Tokenize allow-listed card | `200` with opaque `vt_…` token and fixture fingerprint `b1da4f34…` |
| Approve | `SUCCEEDED` |
| Repeated attempt with the same `Idempotency-Key` | `200`, same `attempt_id`, no additional ledger rows |
| Processor decline (`4000000000009995`) | `DECLINED` / `INSUFFICIENT_FUNDS` |
| Decline then approve on the same payment | first `DECLINED`, second `SUCCEEDED` |
| Risk decline (amount 250001 minor units) | `DECLINED` / `high_amount`, no processor request |
| Injected processor timeout (`PROCESSOR_TIMEOUT_RATE=1`) | `PENDING`, ledger `processor_outcome_unknown`; after restoring `0` the recovery worker recorded `payment_succeeded` |
| Expiry | worker moved an open payment past `expires_at` to `expired` and appended `payment_expired` |
| Redis outage | Redis pod replaced with an unpullable image; create and attempt still returned `201`/`SUCCEEDED` through the Postgres fallback, with one attempt row and the same five ledger events; cache errors logged `idempotency cache unavailable; using database`; Redis restored with the password, `maxmemory 1610612736` and `noeviction` |

## Hosted checkout and real integration

A temporary non-synthetic integration (`gateway-smoke`, removed after the run) used a cluster-local webhook URL. The buyer flows were driven through `https://pay-gateway.phuchoang.sbs` over the Cloudflare tunnel:

- `GET /checkout/{session}?token=…` exchanged the URL token for the secure cookie and rendered the form.
- Posting the insufficient-funds card returned `200` and re-rendered the form with "The card has insufficient funds."
- Posting an allow-listed card with a fresh per-render key returned `303` to `https://shop.phuchoang.sbs/orders/<id>`, matching the CSP `form-action` allowance.

## Webhook delivery

The receiver verified `Gateway-Signature: t=<unix>,v1=<hex>` against the shared secret and recorded these deliveries:

| Event | Status | Signature | Delivery |
| --- | --- | --- | --- |
| `attempt.declined` | `DECLINED` | verified | attempt 1 |
| `payment.succeeded` | `SUCCEEDED` | verified | attempt 1, answered `500` |
| `payment.succeeded` | `SUCCEEDED` | verified | attempt 2, same event ID, answered `200` |
| `payment.expired` | `EXPIRED` | verified | attempt 1 |

The notifier's `webhook_deliveries` row for the retried success recorded `attempts=2`, `last_status=200`, `delivered`, which shows a stable event ID and body across retries. The existing marketplace integration deliveries recorded `last_status=204` and the marketplace's `gateway:<event_id>` inbox key provides its deduplication.

## Event publication

The internal topic `payments.events.v1` was consumed from the beginning and compared with `payment_events`:

- 153 ledger events across 34 payments matched 153 topic messages byte for byte after the fault runs; the topic later reached 161 messages for the 161 ledger rows.
- Every message key equals its ledger `payment_id`, and each partition carried that payment's versions in ascending order.
- No payload contained `vt_` tokens, allow-listed card numbers, `card_number`, `payment_method_token`, card references or checkout tokens. `processor_requests` held no plaintext token and all 26 rows had `token_cleared_at` set.

`cmd/ledger-check` (new ledger invariant tool) reported `153 events across 34 payments satisfy the lifecycle`: version 1 is `payment_created`, versions are contiguous, payload envelopes match their rows, risk and processor phases precede their outcomes, declines and successes reference an active attempt, and expiry only follows payments without an active attempt.

## Fault resilience

| Fault | Observed |
| --- | --- |
| Connect restart | Deleted the Connect pod, then created two payments; both succeeded while the topic offsets stayed at 130. After Connect became ready it published all new events and the topic matched the ledger again. |
| Kafka outage | Froze both Kafka JVM processes; broker clients timed out, two payments still committed 10 events, and topic offsets stayed at 143. After resuming the brokers, Connect published through 153 with no gaps. |
| Redis outage | Payments continued with database fallback and no duplicate effect (see the smoke matrix). |

## Access controls

| Check | Result |
| --- | --- |
| Edge service account to `vault.v1.ProcessingService/ResolveForProcessing` | deadline exceeded (denied) |
| Processor service account to the same method | reached the vault (allowed) |
| `payments_app` to the `notifier` and `vault` databases | `pg_hba.conf rejects connection` |
| `payments_app` to `payments` | connected |
| Payments pod to Redis | `PONG` with the secret; `NOAUTH` without it |
| Edge-labelled pod to Redis | timed out |

## Reproduction

Export ledger rows from the CNPG primary and validate them with the tool:

```bash
kubectl -n payment-gateway exec payment-gateway-pg-1 -c postgres -- psql -U postgres -d payments -Atc \
  "select json_agg(x) from (select global_position, event_id, payment_id, aggregate_version, event_type, encode(payload,'base64') as payload_b64 from payment_events order by global_position) x" \
  > ledger.json
cd src/payment-gateway/payments && GOWORK=off go run ./cmd/ledger-check /path/to/ledger.json
```

Compare the topic with the ledger by consuming `payments.events.v1` with a Kafka client that exposes record keys and raw values, then match each value against `encode(payload,'base64')`.

## Limits

This is a synthetic acceptance run on one prod cluster. It does not establish the 99.9% monthly objective, the 50 attempts/s load target, PCI DSS compliance, ISO 8583 behavior, issuer authentication, authorization/capture, settlement or reconciliation. The expiry check moved one payment's `expires_at` into the past; no other ledger input was altered.
