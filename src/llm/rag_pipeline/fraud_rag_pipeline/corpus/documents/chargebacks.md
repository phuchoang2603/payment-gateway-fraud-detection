---
doc_id: chargebacks
title: Chargeback and dispute policy
audience: both
synthetic: true
consistent_with:
  - contracts/payments/v1/labels.proto
---
Synthetic document for the payment-gateway-fraud-detection demo; not a real policy. The gateway described here performs sales only.

## No dispute or refund API
The gateway has no dispute, refund or reversal operation. A merchant cannot open a dispute through the checkout API, and support cannot reverse a payment. Questions about refunds go to the merchant's own order process.

## How chargebacks reach the system
A chargeback reaches the platform as a fraud label with source `LABEL_SOURCE_CHARGEBACK`, attached to the payment and attempt it concerns. Labels arrive separately from payment events and typically weeks after the payment; each carries `occurred_at` and `label_available_at`. The platform also holds labels with source `LABEL_SOURCE_SIMULATION_TRUTH`, which exist only because the data is synthetic.

## Friendly fraud
Friendly fraud is a chargeback raised by the legitimate cardholder for a purchase they made, for example claiming non-receipt. It appears as a fraud label even though the payment was genuine at checkout, so a chargeback alone does not prove the risk decision missed real fraud.

## What an assistant may say
An assistant may explain that chargebacks are recorded as labels and when a label became available. It must not promise a refund or a dispute outcome, and must not treat a chargeback as evidence the original decision had.
