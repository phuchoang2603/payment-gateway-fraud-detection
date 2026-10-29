# Online generator

The live simulator drives current-time traffic through the gateway's public APIs, so Payments creates the authoritative events that reach Kafka through outbox CDC. It continues the same entity world as the historical source: it rebuilds the world from the manifest's recorded configuration and checks the manifest's `world_digest` before sending anything. Code: [`src/data-pipeline/simulator/simulator/live`](../../src/data-pipeline/simulator/simulator/live).

**Status:** disabled in both environments. [PR #101](https://github.com/phuchoang2603/payment-gateway-fraud-detection/pull/101) reworks the streaming problems below; this page is updated with it.

## Traffic and labels

- Tokenizes synthetic cards, creates payments and submits attempts through Edge with run-scoped idempotency keys; retries reuse the same key and content.
- Publishes simulation-truth labels after each attempt and schedules chargebacks for successful fraud on `fraud.labels.v1`. Label availability is the actual publication time. The schedule is held in memory, so an interrupted run reports unfinished labels instead of claiming them.
- Configuration comes from `SIMULATOR_CONFIG`, rendered by the `live-traffic` chart.

## Streaming data problems

| Problem | Mechanism | Configuration |
|---|---|---|
| Late arrivals | Store-and-forward clients hold an attempt and submit it later, carrying the original client `attempted_at` | `live-traffic` values; reworked in PR #101 |
| Duplicates | Committed events are redelivered to the event topic, on top of at-least-once CDC | `live-traffic` values; reworked in PR #101 |

Evidence (configured versus observed rates, run IDs) is added here when live traffic is enabled.
