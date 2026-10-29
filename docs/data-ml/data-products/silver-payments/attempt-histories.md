# urn:datacontract:fraud:silver-attempt-histories
## Info
*Scoped attempt histories and explicit incomplete payment context.*
- **name:** Silver payment attempt histories
- **version:** 1.1.0
- **status:** active
- **team:** Fraud data platform

## Terms of Use
### Usage
Resolve event identities through canonical events and filter pending context where complete history is required.

### Purpose
Scoped attempt histories and explicit incomplete payment context.

### Limitations
The history is not a predecision feature vector.

## Servers


## Schema
### attempt_histories
*One scoped payment attempt with ordered canonical event references.*

| Field | Type | Attributes |
| ----- | ---- | ---------- |
|  integration_id | string | *Integration identity.*<br />• `required` |
|  payment_id | string | *Scoped payment identity.*<br />• `required` |
|  attempt_id | string | *Scoped attempt identity.*<br />• `required` |
|  events | array | *Event references ordered by aggregate version and event identity.*<br />• `required` |
| &#x2007;&#x21b3; items | object | *No description.* |
| &#x2007;&#x2007;&#x21b3; sequence | integer | *Aggregate version.*<br />• `required` |
| &#x2007;&#x2007;&#x21b3; event_id | string | *Canonical event identity.*<br />• `required` |
| &#x2007;&#x2007;&#x21b3; occurred_at | timestamp | *Event occurrence time.*<br />• `required` |
|  started_events | integer | *Number of attempt start events.*<br />• `required` |
|  attempt_started_at | timestamp | *Server lifecycle occurrence time of AttemptStarted when available.* |
|  attempted_at | timestamp | *Client attempt time from AttemptStarted.signals.attempted_at for attempt-based feature windows; it can precede lifecycle occurrence and does not establish online availability.* |
|  created_events | integer | *Number of payment creation events.*<br />• `required` |
|  terminal_events | integer | *Number of terminal payment events.*<br />• `required` |
|  payment_first_version | integer | *Earliest selected aggregate version.*<br />• `required` |
|  payment_last_version | integer | *Latest selected aggregate version.*<br />• `required` |
|  payment_event_count | integer | *Number of selected canonical payment events.*<br />• `required` |
|  pending_context | boolean | *True when required payment or attempt context has not arrived.*<br />• `required` |
|  _candidate_id | string | *Candidate that last changed this row; the accepted receipt identifies the complete snapshot.*<br />• `required` |

## SLA Properties
