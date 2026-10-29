# Payment gateway architecture

The gateway and its fraud decisioning path. The [verification evidence](../../openspec/changes/archive/2026-10-23-build-minimum-payment-gateway/verification.md) records what the prod deployment proves. It supplies Edge, Payments, notifier, vault and processor simulators, plus Go fraud decisioning; it takes synthetic payment-method tokens through hosted checkout, evaluates risk, executes simulated processor operations, and delivers durable payment outcomes.

## How the gateway is organized

- [Payment flows](flows.md): identity, checkout and payment-authority contracts, with the checkout and recovery sequence.
- [Service conventions](conventions.md): code layout, dependency direction and per-service ownership rules.
- [Docker images](docker.md): image builds for every service.
- [Platform architecture](../overview/architecture.md): deployable units, ownership boundaries and the full deployment diagram.

## Gateway design

The gateway implements the following foundation. Production verification is recorded in [CD](../deployment/cd.md#gateway-verification).

| Component                     | Design                                                                                                                                                                                                                                                                                                                                                                                                                |
| ----------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Edge and Payments             | Edge loads hashed integration keys and checkout configuration. Payments derives merchant/customer UUIDv5 IDs and owns payment commands; there is no Accounts service. Fraud and provider calls use bounded deadlines.                                                                                                                                                                                                 |
| Payment events                | Versioned payment and attempt rows are authoritative. Each transition atomically updates state and idempotency and inserts exactly one immutable ledger event; commands never rehydrate state from events.                                                                                                                                                                                                            |
| Financial journal             | A later Payments-owned balanced financial journal, separate from workflow events. An authorization hold, captured payment and settled funds are different facts.                                                                                                                                                                                                                                                      |
| Redis idempotency cache       | Redis atomically claims request-bound in-flight keys first, then stores completed results with TTL and noeviction; durable Postgres constraints arbitrate retries and Redis loss falls back to Postgres. Authorization submission counters use a separate persistent Redis deployment and credentials; Feast uses a third deployment. Counters begin observe-only and report unknown coverage during recovery warmup. |
| Event publication             | The committed event table supplies the publication boundary. Debezium WAL CDC publishes stored payloads to Kafka without a second outbox table. Sequence allocation is not commit order; checkpoint CDC offsets and preserve per-payment versions.                                                                                                                                                                    |
| Webhook delivery              | The notifier owns durable inbox/retry state and signed merchant webhooks; consumers deduplicate stable event IDs. Neither Kafka nor merchant webhook availability blocks the synchronous processor path.                                                                                                                                                                                                              |
| Processor adapter             | An adapter maps canonical operations to the processor simulator now and a synthetic bank profile later. ISO 8583 and provider HTTP APIs remain outside payment-domain and fraud contracts. Live connectivity needs separate production scope.                                                                                                                                                                         |
| Card vault                    | A separately deployed synthetic vault with its own database, encrypted fictional fixtures, opaque tokens and restricted APIs. It uses Edge-only allow-listed test-card tokenization to model credential isolation; PCI DSS compliance is outside this simulator boundary.                                                                                                                                             |
| Bank files and reconciliation | Excluded for simplicity. Keep status-based processor recovery and explicit pending outcomes; do not claim settlement verification.                                                                                                                                                                                                                                                                                    |

The initial simulator exposes a combined `Sale`; `PaymentSucceeded` records that provider result, not bank settlement. Separate authorization/capture and refunds require their own lifecycle and journal work. The initial availability objective is NFR-03. Database sharding is an explicit later scaling increment under NFR-06, not part of the first deployment.

### Bank adapter and ISO 8583

Yes, the target must transmit and parse ISO 8583 payloads **when the selected bank/acquirer requires that interface**. The bank-facing adapter owns this translation; browsers, merchant APIs, fraud RPCs, event streams and analytical tables keep their own versioned contracts. [ISO 8583:2023](https://www.iso.org/standard/79451.html) specifies message structure/format and explicitly excludes transport and settlement. A standard number alone is insufficient to connect to a bank.

Before implementing a real bank connection, obtain its integration profile and certification requirements:

- Pin the ISO edition, MTIs, required/conditional fields, bitmap and character/binary encoding, length prefixes/framing, amount/currency mapping and response-code semantics.
- Specify connection lifecycle, network management/echo, deadlines, authentication/encryption, and MAC/HSM/key-management responsibilities where the profile requires them. Do not assume a universal TCP, TLS or HTTP transport.
- Persist a mapping from gateway operation identity to provider context and transaction identifiers. STAN/RRN are correlation fields within a profile, not globally unique replacements for gateway IDs.
- Define duplicate, late, unmatched and malformed response handling; timeout recovery uses the profile's supported query or reversal/advice. If no safe resolution is available, retain pending status and surface it for inspection. A missing response must not trigger an unqualified resend of a financial request. Automated settlement reconciliation is outside scope.
- Keep raw card credentials and sensitive ISO wire fields out of ordinary logs, event payloads, Kafka and the ML lake. Real credential retrieval/encoding must stay within the deliberately scoped cardholder-data environment.

Delivery proceeds from the gRPC operation simulator to a **named synthetic ISO profile with an actual encoded request/response exchange**. The synthetic ISO increment must prove byte fixtures, response correlation, duplicates, late responses and uncertain-outcome recovery without live cards. A selected bank's certified profile would require a separate production scope, including how to verify financial outcomes; live connectivity is not a deliverable of this simplified project. The current gRPC simulator is not an ISO 8583 implementation. The [minimum gateway plan](../../openspec/changes/archive/2026-10-23-build-minimum-payment-gateway/design.md#adopted-bank-integration-and-policy-direction) records these follow-up gates.

### Delivery sequence

The first working slice is hosted checkout → rules-based fraud decision → simulated capture → durable financial outcome → signed webhook delivery → marketplace integration. Demonstrate duplicates, timeouts and recovery before layering in learned risk models. The fraud contract leaves feature retrieval and model inference independently deployable as ML capabilities arrive.

Later gateway work adds authorization/capture and refunds; database sharding follows measured load. Feature serving and model inference arrive through the [ML serving](../ml/serving.md) path.

The domain is in the [proposal](../overview/proposal.md); status is in the [roadmap](../overview/roadmap.md).

## Feature-contract boundary

The attempt event carries the immutable checkout context in event schema 3. See [payment flows](flows.md) and the [feature store](../ml/feature-store.md). Rules-only Fraud persists assessment claims and immutable evidence in its own database. Submission counters are observe-only; observation failures preserve valid rules outcomes. Shadow feature retrieval is disabled in every environment until the [feature-store change](../overview/roadmap.md#feature-store-build-order); retrieval acceptance and KServe integration are pending.

Authorization counts use Redis millisecond time and `(r-window,r]`; retries retain their original registration. Process changes start a conservative 60 s coverage warmup. Runtime variables and initial observation thresholds are in [CD runtime configuration](../deployment/cd.md#runtime-configuration). The Flink controller is installed through `talos-proxmox`; the feature chart owns epoch-scoped paths below `runs/payment-gateway/flink/` in the environment platform bucket and object credentials from gateway Doppler. Checkpoint recovery for the v2 job is covered in CI and [recorded in dev](../data/online.md#restart); outage/load evidence is pending.
