---
doc_id: analyst-runbook
title: Fraud analyst investigation runbook
audience: analyst
synthetic: true
consistent_with:
  - contracts/payments/v1/events.proto
  - contracts/payments/v1/labels.proto
  - openspec/specs/fraud-decision/spec.md
---
Synthetic document for the payment-gateway-fraud-detection demo; not a real policy. It describes how an analyst, or an agent acting for one, investigates a payment.

## Boundaries
Investigations are read-only. Neither an analyst assistant nor an agent can change a risk decision, retry a payment, issue a refund or change a policy. Answers cite the evidence they used: events, the recorded assessment and knowledge-base passages.

## Read the timeline first
Start from the payment's events in version order: `PaymentCreated`, then for each attempt `AttemptStarted`, `RiskDecisionRecorded`, and either `AttemptDeclined`, `ProcessorOperationRequested` followed by success, a processor decline or `ProcessorOutcomeUnknown`. The payment ends with `PaymentSucceeded` or `PaymentExpired`. A gap, such as a risk decision without a following event, points to an attempt still in recovery rather than a lost payment.

## Investigating a risk decline
1. Read the reason codes and policy version on `RiskDecisionRecorded`.
2. Check each rule's inputs from the attempt context: total in minor units, customer account creation time, shipping country and observed IP country.
3. Confirm the decision matches the policy version's rules. If it does not, report a possible policy defect; do not reinterpret the rule.
4. Note any shadow signals separately and say that they did not drive the decision.

## Investigating a processor decline or unknown outcome
A processor decline carries the processor's reason; see the decline-code glossary for wording and retry advice. `ProcessorOutcomeUnknown` means the processor did not answer in time. The attempt stays pending until recovery records success or a processor decline. The payment cannot expire while an outcome is unresolved.

## Using labels without hindsight bias
Fraud labels arrive separately and later than decisions, each with `label_available_at`. Use a label only to judge a decision in hindsight, and state when it became available. Never present a label as information the decision had.

## Escalation
Escalate to a human reviewer when the decision contradicts its recorded policy, when events are missing or out of order, or when a merchant disputes a decision. Include the payment id, attempt id, the assessment id and the cited passages.
