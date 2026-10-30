---
doc_id: merchant-faq
title: Merchant support FAQ
audience: merchant
synthetic: true
consistent_with:
  - openspec/specs/checkout-api/spec.md
  - src/payment-gateway/edge/internal/transport/http/pages.go
---
Synthetic document for the payment-gateway-fraud-detection demo; not real support guidance.

## Why was my customer's payment declined?
Either the risk check or the processor declined the attempt. The attempt's reason tells you which: lower-case codes such as `high_amount` come from the risk check, upper-case codes such as `ISSUER_DECLINED` come from the processor. See the decline-code glossary.

## Can the customer try again?
Yes. A declined attempt leaves the payment open, and the customer can start another attempt in the same checkout session until the payment expires.

## How long does a payment stay open?
A payment expires a fixed time after creation, initially 15 minutes, unless an attempt succeeds first. It cannot expire while a processor outcome is still unknown.

## What does PENDING mean?
The processor has not confirmed the outcome yet. Poll the payment; it resolves to succeeded or declined. Do not create a new payment for the same order.

## I got HTTP 409. What happened?
You reused an idempotency key with a different request. Use the original request for retries, or a new key for a genuinely new request.

## I got HTTP 503 or 504. Was the customer charged?
Possibly not processed. Retry with the same idempotency key; the gateway returns the original result if the first request went through.

## Can I refund or dispute a payment through the gateway?
No. The gateway performs sales only and has no refund or dispute operation.

## Can I see why the risk check declined a payment?
You see the reason code. The thresholds behind the risk rules are not shared with merchants.

## Do I need to verify webhook signatures?
Yes. Verify every notification's signature before acting on it, and handle duplicates.

## Can I send card numbers through the API?
No. Attempts carry an opaque token from the hosted checkout page, and attempts containing card fields are rejected. Only synthetic integrations may tokenize allow-listed test cards through a dedicated route.

## Why does the hosted page say the card was not accepted?
The processor returned `INVALID_CARD`. Ask the customer to use a different card.

## Why does the hosted page say the card has insufficient funds?
The processor returned `INSUFFICIENT_FUNDS`. The customer can retry with another card.

## Is the data real?
No. Cards, customers and merchants in this demo are synthetic.
