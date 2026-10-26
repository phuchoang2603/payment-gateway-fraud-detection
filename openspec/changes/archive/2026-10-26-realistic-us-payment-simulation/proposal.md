## Why

The platform needs a realistic, reproducible synthetic payment dataset for model training. Improve the generator's customer behavior, fraud patterns and label reliability without turning the work into a gateway redesign or a new feature-platform project.

## What Changes

- Generate US electronics ecommerce in USD, with varied customer activity, repeat purchases, plausible baskets and local-time/weekend/seasonal patterns.
- Use IANA timezones internally to schedule regional behavior correctly across DST; persist event timestamps in UTC using the existing contracts.
- Fix moving account creation dates and unrealistic device/network assignments. Include legitimate sharing, changes and missing signals using existing observable fields.
- Generate overlapping legitimate/fraud behavior and multi-attempt campaigns so a single artificial field does not reveal the target. Keep truth/scenario information outside features.
- Keep complete payment lifecycles and delayed labels consistent, with reporting probability and maturity made explicit. Preserve deterministic IDs, seeded reproducibility, resumability and immutable Parquet publication.
- Use a small, versioned set of public calibration references and documented assumptions. Publish v13 only as a small smoke/reference export with a concise report covering distributions, label maturity and training limitations; full-volume generation remains an operator-run Python job outside this slice.
- **BREAKING:** publish a new algorithm/profile identity under an unused dataset version, without a legacy generator compatibility path. Preserve v8/v11/v12 and their evidence unchanged.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `historical-data-generation`: realistic US behavior, source-informed calibration and reliable training-dataset generation.

## Impact

- Primarily `src/data-pipeline/simulator`: configuration, profiles, schedules, behavior, historical generation and manifest/report metadata. Reuse existing helpers rather than creating another simulator implementation.
- Keep existing HTTP/protobuf/Parquet business schemas, archiver and deployments. No new browser timezone fields, gateway response changes, live lifecycle redesign or mandatory vault remapping in this slice. Any later live selection must independently satisfy its existing manifest/identity checks.
- Retain measured resource evidence and bounded smoke execution. Remove the unpublished partial full-scale v13 before its smoke reuse and remove the local full-scale config. The project physical-volume requirement remains for the operator-run Python generation; downstream DP1 acceptance remains separate.
- Out of scope: Flink/Feast integration, feature pipelines, model training, DP1 multi-source implementation, new services and full-scale production rollout. The authorized follow-up removes tag-based publication and resets/exercises streaming against published smoke v13 before operator-run full-volume Python generation.
