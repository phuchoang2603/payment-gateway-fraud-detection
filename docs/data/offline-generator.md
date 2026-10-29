# Offline generator

The historical generator writes synthetic payment histories and delayed fraud labels as immutable, per-day Parquet datasets in object storage, standing in for an export from another department that the Bronze pipeline pulls in. The same code runs as the `generate-historical` Argo WorkflowTemplate in the cluster and locally for development. Code: [`src/data-pipeline/simulator/simulator`](../../src/data-pipeline/simulator/simulator) (`world.py`, `lifecycle.py`, `historical.py`, `store.py`).

## Data problems

| Problem | How it is generated | Configuration |
|---|---|---|
| Skew | Half of all ordinary purchases go to one hot merchant; the rest follow Zipf merchant and category popularity and each customer's familiar merchants | `world.behavior.hot_merchant_share`, `merchant_zipf_exponent`, `category_zipf_exponent` |
| Schema evolution | Events before the change date are written as `schema_version` 2 into `.v2` files that physically lack the fields the contract declares `sinceSchemaVersion: 3`; later events go to `.v3` files with the full shape | `schema_changes`, payment-events contract |
| Duplicates | A configured share of events is republished with the same `event_id` and content but a `recorded_at` 1–300 seconds later | `duplicate_rate` |
| High cardinality | Millions of customers, payment methods, devices and addresses | `world.counts` |
| Delayed labels | Simulation truth is available at the end of an attempt; chargebacks for successful fraud arrive 7–21 days later, reported with a configured probability | `world.label_delays` |

The contracts are the published interface: [payment events](products/historical-payments/payment-events.md) and [fraud labels](products/historical-payments/fraud-labels.md).

## Published datasets

| Dataset | Location | Run | Result |
|---|---|---|---|
| v18 (dev) | `s3://platform-dev/datasets/payment-gateway/historical/v18` | `generate-historical-v18-77fzr`, 1 shard, 11m50s | 823,963 events, 161,007 labels, 181 files, 112.4 MB; 1.996% duplicates; top merchant 56.3% of purchases, top 10 merchants 71.1%; 414,059 v2 and 409,904 v3 rows |
| v19 (prod, full scale) | `s3://platform-prod-142737101504-us-east-1/datasets/payment-gateway/historical/v19` | `generate-historical-v19-nvp7l`, 12 spot shards, 49 minutes | 983,727,083 events, 192,204,996 labels, 2,924 files over 365 days, 133.5 GB; sampled parts show about 2.0% duplicates and a 56% top-merchant share |

Both were checked by reading the manifest and Parquet files back from storage: no empty parts, and every `.v2` file lacks `assessment_id` while `.v3` files have it.

## Configuration

Configurations are YAML files baked into the simulator image under `/app/configs`:

| File | Dataset | Purpose |
|---|---|---|
| `historical-dev.yaml` | `v18`, 90 days from 2026-07-31 to 2026-10-28, 10k customers, schema change on 2026-09-15 | Development source in `platform-dev` |
| `historical-full.yaml` | `v19`, 365 days from 2025-10-29 to 2026-10-28, about 3M customers, schema change on 2026-04-30 | Full-scale source in the prod bucket; `minimum_source_bytes` is 100 GB |

Top-level keys hold only the logical dataset:

- `dataset_version`, `duration_days`, `duplicate_rate` (the share of events republished with a later `recorded_at`)
- `world.behavior.hot_merchant_share`: the share of ordinary purchases that go to the first merchant key; the Zipf tail takes the rest
- `minimum_source_bytes`: publication refuses a dataset whose physical Parquet bytes are below this value
- `schema_changes`: ordered `{effective_at, version}` entries. Event schema versions start at 2; a change to version 3 makes the fields declared with `sinceSchemaVersion: 3` in the payment-events contract present from `effective_at` (version 3 is the gateway's current shape)
- `output`: a local directory or an `s3://bucket/prefix` root without credentials
- `world`: `seed`, `integration_id`, `synthetic_key`, `start_date`, entity `counts`, purchase `behavior`, `scenarios`, `label_delays`

Credentials are never read from the configuration.

## Dates and label availability

Purchase-start days end on the last full day before the run, so the history reaches the present and recent windows are full. Lifecycle tails and delayed chargeback labels keep their real timestamps, so some labels have a `label_available_at` after the generation date and are not yet available. Consumers apply an as-of cutoff to `label_available_at` and `occurred_at` for any training or feature use.

## Settings

Run settings come from the environment:

- `GENERATOR_MODE`: `generate` (all days in one process), `shard` (one shard of the days) or `publish` (the manifest, once every day has a marker)
- `SHARD_INDEX`, `SHARD_COUNT`: the shard, used in `shard` mode
- `GENERATOR_WORKERS`: parallel processes in one pod; the chart sets it from `generator.workers`
- `GENERATOR_CONTRACTS`: the historical-payments contract directory (defaults to `/app/contracts/historical-payments` in the image)
- `SIMULATOR_CONFIG`: the YAML configuration

The workflow sets these per step. Locally:

```bash
cd src/data-pipeline/simulator
SIMULATOR_CONFIG=/path/to/copied.yaml GENERATOR_CONTRACTS=../../../contracts/data-products/historical-payments uv run --locked simulator
```

## Running

In the cluster, the environment selects its configuration, shard count, workers and resources through the data-pipelines chart (`generator.config`, `generator.shards`, `generator.workers`, `generator.resources`). Prod runs one shard per spot node in the `aws-cpu-burst` pool. Each shard pod generates the purchase-start days whose index modulo the shard count matches its shard, and a final publish pod writes the manifest. Submit a run with:

```bash
argo submit -n data --from workflowtemplate/generate-historical
```

## Output

Each dataset is written below `historical/<dataset_version>/`:

- `config.json`: the logical configuration, the generator digest and their hash, written first
- `payment_events/date=YYYY-MM-DD/part-NNNNN.v2.parquet` and `.v3.parquet`: payment events by `schema_version`, so each file's Parquet schema has exactly the fields its version declares
- `fraud_labels/date=YYYY-MM-DD/part-NNNNN.parquet`: labels
- `date=YYYY-MM-DD/_day.json`: written after all parts of a purchase-start day, listing their rows, bytes and SHA-256
- `manifest.json`: written last and only if absent, with the configuration, generator digest, contract identities, row and byte totals and the file list

The generator digest is a SHA-256 over the generator source files, the calibration file and the historical contracts. A rerun with the same configuration and digest skips complete days and regenerates incomplete ones, producing the same bytes. A rerun with a different configuration or digest is refused, and so is any version that already has a manifest.
