## ADDED Requirements

### Requirement: Late and duplicate injection verification

Verification SHALL reuse focused scenarios for seeded late and redelivery selection, zero-rate pass-through, store-and-forward click-time preservation and byte-identical same-partition redelivery that skips its own copies. Broker-backed redelivery tests SHALL remain opt-in against an explicitly supplied integration broker; default Python CI SHALL NOT start Kafka or require Testcontainers. It SHALL NOT add redundant logging-shape or render-only tests. Evidence SHALL distinguish deliberately injected duplicates from possible restart replay and SHALL NOT claim exactly-once recovery.

#### Scenario: Selected event is redelivered

- **WHEN** the integration check publishes one event with redelivery selected
- **THEN** exactly one marked, byte-identical copy appears on the same partition and is not copied again

## REMOVED Requirements

### Requirement: Delivery-fault runtime verification

**Reason**: The hold/burst forwarder and its bounded backlog are removed.

**Migration**: Replaced by "Late and duplicate injection verification".
