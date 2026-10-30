---
doc_id: fraud-patterns
title: Card-not-present fraud patterns
audience: analyst
synthetic: true
consistent_with:
  - contracts/payments/v1/labels.proto
---
Synthetic document for the payment-gateway-fraud-detection demo; a primer, not a detection policy. It describes generic signals only.

## Card testing
Fraudsters check whether stolen card numbers work by making many small attempts, often across many cards from one device or network. Signals: bursts of attempts per payment method or device in minutes, many declines with issuer reasons, small amounts.

## Stolen card on a new device
A stolen card is used from a device and network the cardholder has never used. Signals: new device fingerprint for the payment method, IP country differing from shipping country, a recently created account.

## Account takeover
A fraudster logs into a genuine, established account and spends with its saved methods. Signals: an old account with a sudden change of device, network or shipping location, and orders unlike its history.

## High-value fraud
A single large order maximizes the gain before the card is blocked. Signals: totals far above the account's or merchant's usual range.

## Friendly fraud
The genuine cardholder makes the purchase, then disputes it. It is invisible at checkout and appears only later as a chargeback label, so it should not be counted as a missed detection at decision time.
