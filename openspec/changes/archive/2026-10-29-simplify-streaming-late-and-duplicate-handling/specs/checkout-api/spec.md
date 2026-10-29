## MODIFIED Requirements

### Requirement: Client context

Attempt requests SHALL accept device fingerprint and user-agent family. For synthetic integrations only, they SHALL also accept simulated IP country, IP network and client attempt time. A client attempt time SHALL be RFC 3339 and SHALL NOT be after server time; no lower bound applies. Without it, the attempt time SHALL be server time. Hosted page attempts SHALL take the IP network from the trusted ingress gateway's forwarded client address, user-agent family from the request and the device fingerprint from page script. For real integrations, IP country SHALL be absent until a geolocation source exists.

#### Scenario: Synthetic attempt with simulated geography

- **WHEN** a synthetic integration submits an attempt with a simulated IP country
- **THEN** the recorded attempt signals carry that country

#### Scenario: Store-and-forward attempt time

- **WHEN** a synthetic integration submits an attempt with a client attempt time ten minutes in the past
- **THEN** the recorded attempt signals carry that time while event recording time remains server time

#### Scenario: Future or non-synthetic attempt time

- **WHEN** the client attempt time is after server time, or the integration is not synthetic
- **THEN** the request is rejected without recording an attempt
