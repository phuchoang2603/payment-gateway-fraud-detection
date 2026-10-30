---
doc_id: merchant-terms
title: Merchant integration terms (excerpt)
audience: merchant
synthetic: true
consistent_with:
  - openspec/specs/checkout-api/spec.md
  - docs/deployment/security.md
---
Synthetic document for the payment-gateway-fraud-detection demo; not a real agreement.

## Card data
Merchants never send card numbers or security codes with a payment attempt; an attempt that includes a card-number or security-code field is rejected. Buyers enter card details on the gateway's hosted checkout page, which exchanges them for an opaque token. Only synthetic integrations may tokenize an allow-listed test card through the synthetic-only payment-method route. All card data in this demo is synthetic.

## Idempotency
Every create-payment and attempt request carries an idempotency key. Reusing a key with the same request returns the original result; reusing it with a different request is a conflict (HTTP 409). Merchants retry network failures with the same key, never with a new one.

## Webhooks
Payment notifications are signed. Merchants verify the signature with their webhook secret before acting on a notification, and treat notifications as at-least-once: the same event can arrive more than once.

## Data minimization
Merchants send only what checkout needs: order reference, amounts, items, account creation times and shipping location. Buyer names, street lines and contact details are not part of the checkout contract; addresses are reduced to fingerprints.

## Availability
A 503 or 504 response means the request may not have been processed; retry with the same idempotency key. Payments expire if no attempt succeeds before the payment's expiry time.
