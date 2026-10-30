---
doc_id: risk-policy
title: Risk policy rules-v1
audience: analyst
synthetic: true
consistent_with:
  - src/payment-gateway/fraud/internal/domain/policy.go
  - openspec/specs/fraud-decision/spec.md
  - openspec/specs/authorization-velocity/spec.md
---
Synthetic document for the payment-gateway-fraud-detection demo; not a real policy. It states the rules the Fraud service enforces today and why.

## Scope and version
Policy version `rules-v1` is the only enforced policy. Every assessment records the policy version it was made under, so a decision can always be explained against the rules in force at the time. A change to a rule or threshold ships as a new policy version; old assessments keep their recorded version.

## Rules
An attempt is declined when at least one rule fires; all firing rules are recorded as reason codes, in rule order.
- `HIGH_AMOUNT`: the total is above 200000 minor units. Rationale: large single orders carry the highest loss per fraudulent attempt.
- `NEW_ACCOUNT_GEO_MISMATCH`: the buyer's observed IP country differs from the shipping country, the buyer account is less than 7 days old, and the total is above 20000 minor units. Rationale: new accounts shipping abroad from another country are a common card-not-present fraud pattern, and the amount floor keeps small orders from being blocked. When the IP country is not observed, the rule does not fire.

## What is not enforced
Authorization velocity counters run in observe-only mode: thresholds are evaluated and recorded but never decline an attempt. Streaming features and any model score are shadow signals: they are retrieved and stored with the evidence but do not change the decision under `rules-v1`.

## Explaining a decision
An explanation must use only what the assessment recorded at decision time: the reason codes, the policy version, the evidence hash and the decision-time features. A fraud label that arrives later, such as a chargeback, can say whether a decision was right in hindsight, but it was never an input to the decision.
