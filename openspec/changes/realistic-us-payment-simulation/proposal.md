## Why

The current generator makes fraud unusually easy to identify and produces historical signals that differ from live checkout collection. Before using these histories to train a model, replace those shortcuts with a reproducible US ecommerce population and consistent historical/live observations.

## What Changes

- Limit the first regenerated population to US customers and merchants using USD, realistic regional/time-zone variation, and configurable domestic travel. Canada, the UK and cross-border calibration are deferred.
- Pin public aggregate/reference sources into versioned offline calibration inputs with provenance, licenses, hashes, population limitations and explicit assumptions. Generation must not fetch the internet per transaction or copy identifiable customers.
- Use one Python behavior model for historical generation and current-time live traffic: stable account birth dates, heterogeneous purchasing habits, sessions, shared households/devices/networks, device changes and regional seasonality.
- Separate latent identities and fraud intent from checkout-observable fingerprints, browser fields and missing signals. Do not derive IP country from shipping country or fabricate GeoIP enrichment that checkout does not provide.
- Replace deterministic fraud markers with overlapping legitimate/fraud behavior and campaign-level patterns. Separate fraud intent, risk-policy decisions, processor outcomes and observed delayed labels; retain simulation truth outside features.
- Align historical lifecycle/prefilter semantics with the gateway and make policy revisions explicit. Preserve gateway authority for live events, protobuf transport, Parquet output and the existing Python archiver.
- **BREAKING:** introduce a new generation algorithm and identity/profile version without an old-algorithm runtime compatibility path. Coordinate vault mapping and live manifest selection when switching; preserve published v8/v11/v12 data, manifests and evidence unchanged.
- Include bounded resource calibration and new immutable historical generation in the implementation plan. Select an unused dataset version and measure projected time/storage before the full run; do not assume a version number or carry over calendar-rewrite throughput.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `historical-data-generation`: source-backed US calibration, shared historical/live behavior and observation semantics, fraud/label separation, and an immutable regeneration handoff.

## Impact

- `src/data-pipeline/simulator`: profiles, schedules, purchase behavior, historical lifecycle generation, live model/label scheduling, manifest provenance and configuration.
- Gateway checkout signal mapping, synthetic vault identity mapping and historical risk-policy parity at the existing contract boundary. Change contracts only when the agreed observable behavior cannot fit current fields, updating affected consumers together.
- New versioned calibration inputs and a new immutable historical dataset; existing accepted datasets, runtime selection and prior evidence remain untouched until the replacement is published and explicitly selected.
- The completed `simulate-live-traffic-with-transport-faults` slice remains intact. This is a separate follow-up plan in the same PR, not a claim that realism work is implemented.
- Out of scope: DP1 multi-source implementation, model training, Flink deployment, new services, Avro/schema registry, actual-production isolation, and changes to archiver immutability/recovery behavior. DP1 quality acceptance remains downstream and separate from source publication.
