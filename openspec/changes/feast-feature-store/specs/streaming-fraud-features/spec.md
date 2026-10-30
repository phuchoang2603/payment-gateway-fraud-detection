## REMOVED Requirements

### Requirement: Journal-only streaming output
**Reason**: The feature-store change consumes the v2 journal. Push job A serves it from Redis, and push job B appends it to the offline stream feature tables. Training history for the same windows comes from replaying recorded events through this job under a history epoch.

**Migration**: Flink keeps publishing `fraud-snapshot-v2` updates to `fraud.feature-updates.v2.<epoch>` and rejects to `fraud.feature-rejects.v1.<epoch>`, unchanged. The push jobs, the feature server and Fraud's retrieval are rendered where the feature store is enabled (dev and prod), as the `feature-store` and `fraud-decision` requirements specify.
