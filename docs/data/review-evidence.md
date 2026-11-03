# Review evidence

Use this map to review the implementation and its proof together. A source link establishes implemented behavior; a fixture establishes a controlled case; a UI capture establishes only what is visible for that run. Recorded AWS-era results stay as recorded after the GCP migration. New production dataset generation and downstream publication must be verified separately.

| Criterion | Implementation and explanation | Existing evidence | Remaining proof |
|---|---|---|---|
| Generator skew and cardinality ratio | [Generator measurements](offline.md#cardinality-and-skew-are-different-measurements), full-scale entity domains and a range-scoped ratio query | Recorded top-merchant shares; generator configuration | Observed distinct counts, null counts and ratios for the new GCP dataset |
| Schema evolution | [v2/v3 examples](offline.md#schema-evolution-and-duplicate-examples), physical field-presence tests and Spark `mergeSchema` | Recorded version counts and schema tests | Side-by-side source/merged schema and query-result captures; these are behavior comparisons, not performance speedups |
| Batch duplicates/conflicts | [Silver](offline.md#silver), exact event hash and earliest delivery, payment-level conflict quarantine | Fixture with identical and conflicting copies; recorded Bronze/Silver totals | Spark SQL input/output captures for the same pinned fixture or range |
| Skew optimization | [Controlled skew comparison](offline.md#controlled-skew-comparison), same input with baseline, AQE and salting | Equal-output and actual-skew-plan tests | Timed production report plus before/after final adaptive plans; DP1 write-stage images measure a different operation |
| Streaming baseline and window code | [Runtime and windows](online.md#streaming-features), [late/duplicate trace](online.md#late-too-late-and-duplicate-trace) | Job graph, seeded replay report and correctness chart | Live before/after contributions for the same event IDs |
| Too-late handling | 24-hour membership horizon in `Packet.too_late` and the Flink operators | Local replay injects 8 too-late attempts; live run injected none | Runtime input older than 24 hours, `too_late` counter delta, log and unchanged feature counts; a watermark image is insufficient |
| Pipeline integration in UI | [Orchestration](orchestration-governance.md#workflows) | [DP1 workflow](images/argo-dp1-bronze.png), [backfill DAG](images/argo-backfill-parallel-skip.png) | Fresh GCP workflow links/captures after successful publication |
| DP1/DP2 lineage and validation | [Catalog](orchestration-governance.md#catalog), source → Bronze → Silver lineage and native test results | [Lineage graph](images/openmetadata-lineage.png), [quality results](images/openmetadata-data-quality.png) | Per-pipeline run IDs and expanded input/output edges; table-level rule names, audited snapshot and result time in one capture |
| Schema design | [Product contracts](orchestration-governance.md#data-products-and-contracts), [table layout](storage.md#lakehouse), [warehouse indexes](storage.md#warehouse-indexing) | Contract-derived field/rule references and recorded planning results | No assessment sheet was supplied here; map its exact criteria to catalog columns/types, keys, partition specs and rule captures before claiming full coverage |
| Lakehouse/warehouse optimization | [Code and measurements](storage.md#implementation-references) | DP1 write-stage captures, planned files, ClickHouse scan/timing comparisons; negative Z-order result retained | Same-input rerun on GCP if a current-platform speedup is claimed |
| Inference setup and benchmark | [Setup](../llm/inference.md#setup-and-verify), vLLM benchmark workload and results | Dev model-call results, baseline/variants and direct/gateway comparison | Fresh raw benchmark output and rollout metadata for a new run; vLLM is the accepted harness |
| Global model config through gateway | [ModelConfig](../llm/agents.md#global-model-config), [route/mesh checks](../llm/gateway.md#checks-on-dev) | ModelConfig screenshot, chat/tool-call screenshots and gateway counters | One correlated request across config, trace and gateway route metrics for the new release |
| Async MCP and rollout behavior | [MCP serving and Helm](../llm/rag.md#deployment-rollout-and-fallback) | Async tool code, gateway tool-call checks and API load test | Failed-revision rollout capture; no automatic rollback or alternate-backend failover is implemented |

## Optimization review

| Area | Current choice | Refactor or measurement decision |
|---|---|---|
| Generator reruns | Source digest, immutable config and per-day markers | Keep fingerprinted files stable during v19 generation. Even a docstring changes their byte digest; document that code externally until a deliberate new generator identity is published. |
| Spark skew | AQE and a controlled salting benchmark | Measure the real adaptive plan before adopting salting; its replicated side may cost more than the split saves. |
| Silver deduplication | One range-bounded shuffle by `event_id` | Keep the content-conflict gate. Replacing it with arbitrary `dropDuplicates` would hide contradictory amounts and break quarantine. |
| Flink state | Incremental counts, expiry heaps, device reference counts and bounded entity state | Avoid whole-history scans. The recorded runtime cost is dominated by Python boundaries; merging dedup stages would change state identity, counters and recovery, so it needs a separate checkpoint-compatible experiment. |
| MCP serving | One shared async HTTP client, thread offload for synchronous Feast access, locked lazy initialization | Preserve the API and tests while documenting the concurrency boundary. A new async database client or thread-pool tuning needs concurrent tool-call latency and failure measurements first. |

No new throughput improvement is claimed from documentation or docstrings. The [online replay chart](online.md#replay-benchmark) compares correctness variants, while the Spark and vLLM tables name the operations and workloads they measured.
