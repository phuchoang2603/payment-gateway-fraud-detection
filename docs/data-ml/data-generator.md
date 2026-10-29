# Data generator

The historical generator writes reproducible synthetic payment histories to object storage, outside operational payment storage. The live simulator reuses the historical entity world and sends commands through the gateway. See [verification](verification.md) for the accepted dev v17 results, screenshots and diagnostic interpretation.

## Configuration

[`historical-smoke.yaml`](../../src/data-pipeline/simulator/configs/historical-smoke.yaml) configures the seed, entity pools, 90 purchase-start days, merchant/category skew, duplicate rate, schema schedule and execution budgets. One dataset supports both data-problem demonstrations and later feature preparation. Lifecycle and label tails can extend beyond the purchase period.

The image's `simulator` entrypoint reads `/app/configs/historical-smoke.yaml`. Mount replacement YAML there or select it with `SIMULATOR_CONFIG`. Set `dataset_version` and `output` in YAML; credentials come from the runtime environment. A changed logical input requires a new dataset version. Never overwrite a completed source.

Publication writes the completion manifest last. It pins configuration, generator/contracts, physical schema definitions and the exact Parquet inventory with hashes, rows and bytes. Compatible unfinished generation resumes; completed sources remain immutable. Publication alone does not accept Bronze or Silver.

## Schema evolution

[`physical-schemas-v1.yaml`](../../contracts/data-products/historical-payments/physical-schemas-v1.yaml) declares physical shapes independently of event-envelope versions. Configuration selects an initial version and strictly increasing UTC effective times; events select their shape by occurrence time, and duplicate deliveries retain it. Dev v17 demonstrates physical 3 → 4 while the event envelope remains version 3.

Schema 4 adds optional assessment references; missing schema-3 fields normalize to null. Synthetic references describe synthetic assessment evidence, not a persisted Fraud document or served feature vector. Later versions need an explicit shape, compatible contracts and lossless normalization. Schedules can skip declared intermediate versions without a pair-specific migration. Keep published feature definitions such as `fraud-snapshot-v1.yaml` available for source-manifest verification even when serving adopts v2. Preserve published definition files and their hashes; publish a new definition file when adding shapes.

## Inspecting generated data

The executed [generator notebook](../../src/data-pipeline/simulator/notebooks/historical-sanity.ipynb) contains configuration, inventory verification, volume, skew plots, observed cardinality, schema evolution and duplicate measurements. Pipeline acceptance and Spark findings are in [verification](verification.md).

Missing chargebacks are unknown, not negatives. Labels become usable only at `label_available_at`; scenario/oracle truth, risk decisions and future outcomes must stay out of predecision features. Development diagnostics establish neither full-scale acceptance nor real-world model quality.
