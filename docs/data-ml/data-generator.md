# Historical data generator

The historical generator writes synthetic payment histories and delayed fraud labels as immutable, per-day Parquet datasets. The same code runs as the `generate-historical` Argo WorkflowTemplate in the cluster and locally for development.

## Configuration

Configurations are YAML files baked into the simulator image under `/app/configs`:

| File | Dataset | Purpose |
|---|---|---|
| `historical-dev.yaml` | `v18`, 90 days from 2025-07-01, 10k customers | Development source in `platform-dev` |
| `historical-full.yaml` | `v19`, 365 days from 2025-01-01, about 3M customers | Full-scale source in the prod bucket; `minimum_source_bytes` is 100 GB |

Top-level keys hold only the logical dataset:

- `dataset_version`, `duration_days`, `duplicate_rate`
- `minimum_source_bytes`: publication refuses a dataset whose physical Parquet bytes are below this value
- `schema_changes`: ordered `{effective_at, version}` entries. Physical versions start at 1; a change to version 2 adds the fields that the payment-events contract declares with `sinceSchemaVersion: 2`
- `output`: a local directory or an `s3://bucket/prefix` root without credentials
- `world`: `seed`, `integration_id`, `synthetic_key`, `start_date`, entity `counts`, purchase `behavior`, `scenarios`, `label_delays`

Credentials are never read from the configuration.

## Settings

Run settings come from the environment:

- `GENERATOR_MODE`: `generate` (all days in one process), `shard` (one shard of the days) or `publish` (the manifest, once every day has a marker)
- `SHARD_INDEX`, `SHARD_COUNT`: the shard, used in `shard` mode
- `GENERATOR_WORKERS`: parallel processes in one pod; the chart sets it to the CPU request
- `GENERATOR_CONTRACTS`: the historical-payments contract directory (defaults to `/app/contracts/historical-payments` in the image)
- `SIMULATOR_CONFIG`: the YAML configuration

The workflow sets these per step. Locally:

```bash
cd src/data-pipeline/simulator
SIMULATOR_CONFIG=/path/to/copied.yaml GENERATOR_CONTRACTS=../../../contracts/data-products/historical-payments uv run --locked simulator
```

## Running

In the cluster, the environment selects its configuration, shard count and CPU through the data-pipelines chart (`generator.config`, `generator.shards`, `generator.resources`). Each shard pod generates the purchase-start days whose index modulo the shard count matches its shard, and a final publish pod writes the manifest. Submit a run with:

```bash
argo submit -n data --from workflowtemplate/generate-historical
```

## Output

Each dataset is written below `historical/<dataset_version>/`:

- `config.json`: the logical configuration, the generator digest and their hash, written first
- `payment_events/date=YYYY-MM-DD/part-NNNNN.v1.parquet` and `.v2.parquet`: payment events by physical schema version, so each file's Parquet schema has exactly the fields its version declares
- `fraud_labels/date=YYYY-MM-DD/part-NNNNN.parquet`: labels
- `date=YYYY-MM-DD/_day.json`: written after all parts of a purchase-start day, listing their rows, bytes and SHA-256
- `manifest.json`: written last and only if absent, with the configuration, generator digest, contract identities, row and byte totals and the file list

The generator digest is a SHA-256 over the generator source files, the calibration file and the historical contracts. A rerun with the same configuration and digest skips complete days and regenerates incomplete ones, producing the same bytes. A rerun with a different configuration or digest is refused, and so is any version that already has a manifest.
