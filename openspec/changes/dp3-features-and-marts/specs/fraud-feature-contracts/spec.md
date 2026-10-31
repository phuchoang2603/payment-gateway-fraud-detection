## ADDED Requirements

### Requirement: Batch long-window definition v1
`fraud-history-batch-v1` SHALL define the daily-anchored batch history quantities for customers and payment methods over 7- and 30-day windows. Every quantity SHALL use the same anchor, eligibility and coverage rules:

- **Anchor:** `s` is a UTC midnight.
- **Attempt time:** the client attempt time when observed, otherwise the server start time.
- **Eligibility:** a lifecycle-valid attempt is eligible only when its attempt time is in `[s − window, s)` and its server start is before `s`.
- **Outcome-based quantities:** these count only outcomes that occurred before `s`.
- **Rates:** each rate SHALL name its numerator and denominator, and SHALL be null when the denominator is zero.
- **Coverage:** a window not fully covered by history SHALL be null.
- **Keys:** keys SHALL include integration identity, and unknown devices SHALL stay absent.

The definition SHALL include hand-checkable conformance examples for:

- window boundaries;
- expiry with no new input;
- client and server time disagreement;
- outcomes after the anchor;
- an empty denominator;
- coverage;
- integration separation.

The DP3 job SHALL reproduce those examples in CI. Labels, scenarios and the scored attempt's own decision and outcome SHALL NOT be inputs.

#### Scenario: Outcome after the anchor

- **WHEN** an attempt inside the 7-day window is declined after the anchor
- **THEN** it counts toward the attempt count but not toward the declined or decided counts at that anchor

#### Scenario: Window lower boundary

- **WHEN** an eligible attempt has attempt time exactly `s − 7 days`
- **THEN** it counts in the 7-day window at `s`, while an attempt at `s` does not
