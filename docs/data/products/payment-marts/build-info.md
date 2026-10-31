# urn:datacontract:fraud:marts-build-info
## Info
*One row describing the latest marts build, its retention window, the days it rebuilt and the Silver snapshots it read.*
- **name:** Marts build info
- **version:** 1.0.0
- **status:** active
- **team:** Fraud data platform

## Terms of Use
### Usage
Read window_start and window_end before interpreting missing days in the marts as zero.

### Purpose
One row describing the latest marts build, its retention window, the days it rebuilt and the Silver snapshots it read.

### Limitations
Holds only the latest build; earlier builds are in the DP3 run reports.

## Servers
| Name | Type | Attributes |
| ---- | ---- | ---------- |
| clickhouse | clickhouse | *Native MergeTree table in the ClickHouse marts database. The rule placeholder {object} stands for the table.*<br />• **environment:** dev<br />• **database:** marts<br />• **host:** clickhouse.data.svc.cluster.local<br />• **port:** 8123 |

## Schema
### build_info
*Exactly one row.*

| Field | Type | Attributes |
| ----- | ---- | ---------- |
|  run_id | string | *Workflow run that built the marts.*<br />• `required` |
|  window_start | date | *First start day kept by the retention window.*<br />• `required` |
|  window_end | date | *Latest start day in Silver at the pinned snapshot.*<br />• `required` |
|  rebuilt_days | array | *Start days this build rebuilt: the DP3 run's days, the days its labels touched and the days whose chargeback window completes in its range.*<br />• `required` |
|  chargebacks_complete_before | date | *The day after the DP3 run's last day; a start day's chargeback window is complete when it ends by then.*<br />• `required` |
|  silver_attempts_snapshot_id | integer | *silver.attempts snapshot the build read.*<br />• `required` |
|  silver_labels_snapshot_id | integer | *silver.labels snapshot the build read.*<br />• `required` |
|  silver_payment_events_snapshot_id | integer | *silver.payment_events snapshot of the same DP3 run.*<br />• `required` |
|  built_at | timestamp | *Wall-clock time of the build.*<br />• `required` |

## SLA Properties
