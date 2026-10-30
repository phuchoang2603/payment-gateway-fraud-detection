---
doc_id: decline-codes
title: Decline and reason-code glossary
audience: both
synthetic: true
consistent_with:
  - src/payment-gateway/fraud/internal/domain/policy.go
  - src/payment-gateway/processor-simulator/internal/domain/sale.go
  - src/payment-gateway/payments/internal/domain/payment.go
  - src/payment-gateway/edge/internal/transport/http/pages.go
  - contracts/payments/v1/events.proto
---
Synthetic document for the payment-gateway-fraud-detection demo; not a real policy. It explains every decline code the gateway can produce today.

## How a decline is recorded
An attempt is declined by exactly one source. `DECLINE_SOURCE_RISK` means the Fraud service declined it before any processor call. `DECLINE_SOURCE_PROCESSOR` means the processor declined it after the risk check approved it. The decline is recorded as an `AttemptDeclined` event with the source and a reason. A risk decline uses the first reason code of the assessment in lower case, for example `high_amount`. A processor decline uses the processor's reason as sent, for example `INSUFFICIENT_FUNDS`. A declined attempt leaves the payment open, so the buyer can try again until the payment expires.

## HIGH_AMOUNT
Source: risk. The checkout total is above 200000 minor units (2,000.00 in a two-decimal currency). The threshold is applied to the minor-unit amount regardless of currency. Merchant-facing wording: "The payment could not be approved." Retry advice: a retry with the same amount is declined again under the same policy version; a smaller order is assessed afresh.

## NEW_ACCOUNT_GEO_MISMATCH
Source: risk. All of these hold at attempt time: the buyer's IP country was observed and differs from the shipping country, the buyer account is less than 7 days old, and the total is above 20000 minor units (200.00). Merchant-facing wording: "The payment could not be approved." Retry advice: the decision depends on account age, so the same order from the same new account is declined again until the account is 7 days old or the shipping and IP countries match.

## INSUFFICIENT_FUNDS
Source: processor. The issuer reported that the card cannot cover the amount. Buyer-facing text on the hosted page: "The card has insufficient funds." Retry advice: the buyer may retry with another card in the same session.

## ISSUER_DECLINED
Source: processor. The issuer declined without a more specific reason. Buyer-facing text: "The card issuer declined the payment." Retry advice: the buyer may retry with another card; repeating the same card rarely changes the outcome.

## INVALID_CARD
Source: processor. The card was not accepted, for example an unknown or unusable card. Buyer-facing text: "The card was not accepted." Retry advice: the buyer should use a different card.

## Attempt statuses
The checkout API reports an attempt as `SUCCEEDED`, `DECLINED` with a reason, or `PENDING`. `PENDING` means the processor outcome is not known yet; it resolves only to success or a processor decline, never to expiry, and the merchant should poll the payment instead of retrying.

## API errors that are not declines
These HTTP responses from the edge are errors, not declines, and record no decline: 400 invalid input, 401 failed authentication, 404 unknown or foreign resource, 409 idempotency conflict (the same key was reused with a different request), 503 a dependency is unavailable, 504 the deadline expired. A 503 or 504 on an attempt is safe to retry with the same idempotency key.
