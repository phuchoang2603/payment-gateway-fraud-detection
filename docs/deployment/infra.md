# Infrastructure

Two Talos Kubernetes clusters run the same application charts. They are provisioned and operated from the platform repository, [talos-proxmox](https://github.com/phuchoang2603/talos-proxmox), with OpenTofu (Terraform-compatible) for Proxmox, AWS and cluster bootstrap, and Argo CD for platform components. This repository holds no Terraform stack or cloud credentials.

| | Dev | Prod |
|---|---|---|
| Infrastructure | Proxmox VMs: one control plane and two workers | AWS `us-east-1`, single availability zone |
| Baseline nodes | `dev-server1` (control plane), `dev-server2`, `dev-server3` | One On-Demand `m6a.xlarge` control plane and one On-Demand `m6a.xlarge` worker |
| Elastic capacity | None | Karpenter `aws-cpu-burst` Spot pool, up to 12 nodes / 48 vCPU / 192 GiB, large and xlarge instances, consolidated after 5 minutes |
| Volumes | `local-path` | Encrypted gp3 EBS |
| Object storage | MinIO `platform-dev` | Private versioned S3 |

The full-scale generator uses the burst pool: one shard per Spot xlarge node, selected by `karpenter.sh/nodepool: aws-cpu-burst` and `karpenter.sh/capacity-type: spot` in the prod values. Both clusters have one control plane and are not highly available.

## Platform and application ownership

| Owned by the platform | Owned by this repository |
|---|---|
| Talos, Cilium, Argo CD, Argo Workflows, Spark and Flink operators, External Secrets, Istio ambient mesh, ingress and Cloudflare tunnels | Application charts and their values |
| The Altinity ClickHouse operator, OpenMetadata, Kubeflow and the `ml` workspace | Polaris catalog and its registration, ClickHouse instance and users, ClickHouse `lakehouse` DataLakeCatalog, catalog connections |
| Victoria metrics and traces stores, Grafana | Application telemetry configuration |
| Buckets, storage policies and gateway storage identities | Dataset, run and lakehouse prefixes used by workloads |

Removing an application chart never removes shared services, buckets, catalog databases or namespaces.

Captures of the provisioning runs are added here.
