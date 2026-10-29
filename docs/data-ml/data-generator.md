# Historical data generator

The historical generator writes synthetic payment histories and delayed fraud labels as immutable, per-day Parquet datasets. The same code runs as the `generate-historical` Argo WorkflowTemplate in the cluster and locally for development.

## Configuration

Configurations are YAML files baked into the simulator image under `/app/configs`:

| File | Dataset | Purpose |
|---|---|---|
| `historical-dev.yaml` | `v18`, 90 days from 2025-07-01, 10k customers | Development source in `platform-dev` |
| `historical-full.yaml` | `v19`, 365 days from 2025-01-01, about 3M customers | Full-scale source in the prod bucket; `minimum_source_bytes` is 100 GB |

Top-level keys:

- `dataset_version`, `output` (a local directory or an `s3://bucket/prefix` root without credentials), `duration_days`
- `chunk_customers`: customers per part; a day is split into `ceil(customers / chunk_customers)` parts per table
- `workers`, `row_group_rows`: execution settings that do not change records
- `duplicate_rate`: share of payment events exported twice, byte-identical to the original
- `minimum_source_bytes`: publication refuses a dataset whose physical Parquet bytes are below this value
- `schema_changes`: ordered `{effective_at, version}` entries; the physical schema starts at 3 and changes to 4 for events occurring at or after `effective_at`
- `world`: `seed`, `integration_id`, `synthetic_key`, `start_date`, entity `counts`, purchase `behavior`, `scenarios`, `label_delays`

Credentials are never read from the configuration. S3 access uses the standard AWS environment variables and `S3_ENDPOINT` / `S3_PATH_STYLE`.

## Running

In the cluster, the environment selects its configuration through the data-pipelines chart (`generator.config`). Submit a run with:

```bash
argo submit -n data --from workflowtemplate/generate-historical
```

Locally, copy a configuration, set `output` to a local directory and run from the simulator project:

```bash
cd src/data-pipeline/simulator
SIMULATOR_CONFIG=/path/to/copied.yaml uv run --locked simulator
```

## Output

Each dataset is written below `historical/<dataset_version>/`:

- `config.json`: the logical configuration and its hash, written first
- `payment_events/date=YYYY-MM-DD/part-NNNNN.parquet` and `fraud_labels/date=YYYY-MM-DD/part-NNNNN.parquet`: one part per customer chunk and table; each row of events carries `physical_schema_version`
- `date=YYYY-MM-DD/_day.json`: written after all parts of a purchase-start day, listing their rows, bytes and SHA-256
- `manifest.json`: written last and only if absent, listing every part with its checksum and totals, the configuration, seed, generator revision and contract identities

A rerun with the same configuration skips complete days and regenerates incomplete ones, producing the same bytes. A rerun with a different configuration is refused, and so is any version that already has a manifest. Worker count does not change the output.
