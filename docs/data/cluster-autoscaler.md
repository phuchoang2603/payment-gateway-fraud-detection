# Cluster Autoscaler for batch and streaming (novel)

The heavy data jobs are bursts: the 100 GB generator, a monthly Spark backfill and the stream history replay each need tens of vCPU for hours, then nothing. On GCP prod they ran on a Spot node group that Cluster Autoscaler grows from zero when their pods are Pending and drains back to zero when the nodes sit idle, so the cluster pays only for a fixed baseline of three on-demand workers plus Spot nodes while a job runs.

```mermaid
flowchart LR
  job[Generator shard, Spark executor or Flink TaskManager<br/>nodeSelector nodepool=spot] -->|Pending| ca[Cluster Autoscaler]
  ca -->|grow MIG| spot[GCE Spot group<br/>0 to 12 e2-standard-4]
  spot -->|node Ready| job
  idle[Idle Spot node] -->|drain and remove| ca
  ctl[Drivers, JobManagers, Argo steps] --> cpu[On-demand cpu group<br/>3 e2-standard-4]
```

## How it works

- **Node groups.** The platform ([talos-proxmox](https://github.com/phuchoang2603/talos-proxmox/blob/main/docs/architecture/gcp-workers.md)) provisions a fixed `cpu` group of three on-demand `e2-standard-4` workers and a `spot` group of `e2-standard-4` Spot workers bounded at 0 to 12 nodes (48 vCPU, 192 GiB). Cluster Autoscaler v1.37.0 grows the Spot group for Pending pods; `kube-env` metadata describes the Talos reservations so it can simulate a node it has not started yet, which is what makes scale from zero work.
- **What goes on Spot.** Work that can be retried runs there: generator shards, Spark executors and Flink TaskManagers, selected by `talos.dev/nodepool: spot` in the [prod values](../../infra/charts/data-pipelines/values-prod.yaml). Work whose loss would restart a whole run stays on the on-demand group: Spark drivers, Flink JobManagers and Argo Workflows step pods, because a reclaimed resource-step pod makes Argo submit a second SparkApplication.
- **Sizing to the node.** A Spot node leaves about 3,270m CPU and 13,217 MiB for pods, so every Spot workload is sized to one node: a generator shard requests 3 CPU and 8 GiB, a Spark executor 3 cores with 9 GiB heap and 3 GiB overhead, and two history TaskManagers (1,400m, 5 GiB each) share a node.
- **Scale-down safety.** Pods of running jobs carry `cluster-autoscaler.kubernetes.io/safe-to-evict: "false"`, so the autoscaler never drains a node mid-job to consolidate; it removes a node only when nothing that annotation protects is left. A Spot reclaim is still possible, so every Spot job retries: Spark with graceful decommission and 10 retries, generator shards up to 10 times with complete days skipped, history units from their seeded topics.
- **One job family at a time.** The 12-node cap cannot hold a full generator run and a full Spark run together, so they run one after the other.

### Spark sizing and placement

| Workload | Placement and sizing |
|---|---|
| Silver and Bronze (`large`) | Spot pool (`talos.dev/nodepool: spot`): 11 executors × 3 cores = 33 executor cores; each 9 GiB heap + 3 GiB overhead, 24 GiB ephemeral storage |
| Gold (`gold`) | Same executors at 7 GiB heap + 5 GiB overhead: Gold runs no Python, but its native memory outgrew 3 GiB in May 2026 |
| Drivers | On-demand `cpu` pool (three `e2-standard-4` workers); 500m request, 4 GiB + 1.5 GiB for `large`, 2 GiB + 1.5 GiB for `gold` |
| Every executor | `MALLOC_ARENA_MAX=2`, graceful decommission, failure exclusion, 10 Argo retries |
| Shuffle | 1,024 partitions, AQE coalescing with 128 MB advisory size; large v19 shuffles require more than 3–4× core count |
| Kafka mode | One 3-core executor, three shuffle partitions matching the topics |
| Argo steps / SQL checks | Steps on on-demand workers; SQL executes on shared ClickHouse |

The spot pool is capped at 12 nodes / 48 vCPU / 192 GiB. Its only machine type is `e2-standard-4`, which has about 3,270m pod CPU after reservations, so one 3-core executor fits, and 16 GiB fits the executor memory.

## Runs on the Spot pool

### Scale from zero

On 2026-11-14 prod was brought back with a reduced app footprint and the Spot group at 0 nodes, and a bounded copy of the generator, `generate-historical-autoscaler-demo` (4 shards × 2 days, a world about 10× smaller, about 324 MB to a scratch prefix), was submitted. Each shard requests 3 CPU on `talos.dev/nodepool: spot`, so the four shards went Pending and the autoscaler added one node per shard:

| Step (UTC) | Time |
|---|---|
| Workflow submitted, 4 shards Pending, Spot at 0 | 09:30:08 |
| Autoscaler scale-up plan 0 → 4 | 09:30:18 |
| Nodes Ready | 09:31:19–09:31:27 |
| Shards Running, one per node | 09:31:46–09:31:55 |
| Shards done | 09:32:18–09:32:27 |
| Workflow Succeeded (2m40s, 5/5 steps) | 09:32:48 |
| Autoscaler removes the empty nodes | 09:42:32–09:43:03 |
| Spot group target back to 0 | 09:43:06 |

Pending to Running took about 1m40s: about 1m15s until the new nodes were Ready, then about 25 s to pull and start the shards. The last shard finished 10m18s before the group reached 0, matching `--scale-down-unneeded-time=10m` and `--scale-down-delay-after-add=10m`.

![Ready nodes by pool (Spot 0 → 4 → 0, on-demand cpu fixed at 3) and generator shard pods by phase](images/cluster-autoscaler-spot-nodes-2026-11-14.png)

The curves trail events by up to the 60 s scrape interval. The dashed line counts registered Spot Node objects: the cloud controller deletes them about 11 minutes after their VMs are gone, and its tail before 09:30 is an earlier drain.

![Argo Workflows: all four shards and the publish step Succeeded; shard 3 ran on prod-talos-spot-hczj](images/cluster-autoscaler-generator-workflow-2026-11-14.png)

Shards 0 to 3 ran on `spot-s7kr`, `spot-dqn9`, `spot-bv29` and `spot-hczj`. The Argo UI shows times in UTC−05:00. Both screenshots come from local Grafana and Argo UIs of prod's versions pointed at prod, because the prod hostnames now route to dev.

### Full-size runs

The full v19 generator, `generate-historical-v19-gcp-mstrk`, ran 12 shards of 3 workers, one shard per Spot node, and published 983,727,083 events and 192,204,996 labels in 2,924 files (133.5 GB) to GCS ([offline data](offline.md#publication)).

The GCP backfill `dp-backfill-v19-gcp-oct-b-6wrsz` ran Bronze, Silver, Gold and the marts for October 16–28, 2026, with the `large` executors on Spot nodes; all 15 tasks passed:

![GCP backfill on Spot executors: every stage succeeded](images/argo-gcp-backfill-2026-11-03.png)

The stream history replay ran six 4-day units at a time, each TaskManager on a Spot node and its JobManager on the on-demand group. Unit `20260907` on 2026-11-05 at 18:12 UTC (UI time UTC−05:00):

![GCP Flink history unit 20260907 on a Spot TaskManager: source, deduplication, windows and journal sinks](images/flink-jvm-history-20260907-2026-11-05.png)

All 23 units of 2026-07-29 to 2026-10-29 finished and published in about 2 days 8 hours, about 236 million rows per stream table ([historical replay](online.md#full-replay)).

## Limits

- Prod was retired on 2026-11-09 and came back only for the scale-from-zero run; dev runs on fixed Proxmox VMs with no autoscaler.
- A full 12-node burst brings the project to 72 vCPU, above its 68 vCPU quota.
- Spot reclaims are retried, not prevented, and a reclaim during a Spark stage repeats that stage's work.
