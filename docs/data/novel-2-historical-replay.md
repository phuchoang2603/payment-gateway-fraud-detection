# Novel 2: historical replay through the live feature engine

Historical training features are reconstructed with the same JVM Flink operators that compute live features. The project-specific contribution combines shared feature logic, an offset-gated replay clock, isolated outputs and recoverable date units. Flink replay and point-in-time retrieval are established techniques; this design applies them together to reduce separate batch/stream implementations.

## Shared operators, historical clock

[`StreamJob`](../../src/data-pipeline/streaming/jvm/src/main/java/dev/fraud/stream/StreamJob.java) connects the same validation, event/attempt deduplication, customer/method windows and journal sinks in both modes. Live processing uses wall-clock timers. Replay uses bounded Kafka input and logical time derived from historical availability, so it can process old events without waiting for their original wall-clock intervals.

The seed writes ordered Bronze deliveries and clock controls to separate topics. Each control includes an input-offset boundary. [`ObserveSource`](../../src/data-pipeline/streaming/jvm/src/main/java/dev/fraud/stream/ObserveSource.java) buffers interleaved records and releases a clock control only after its preceding input boundary has been processed. Source progress, buffered records and pending controls are checkpointed together; a control read early from Kafka cannot advance time past unread deliveries.

[`Aggregate`](../../src/data-pipeline/streaming/jvm/src/main/java/dev/fraud/stream/Aggregate.java) applies attempt-time membership to the same 5-minute, 1-hour and 24-hour windows in both modes. The replay clock determines when historical information becomes available, while the window excludes the scored attempt itself. Each unit includes a one-day lead-in to establish its retained history.

## Isolation and recovery

| Mechanism | Purpose |
|---|---|
| Separate input, clock and feature-journal topics; history epoch | Keep replay separate from live traffic and live feature revisions |
| Seed report matched to date range, epoch, Bronze snapshot and topic end offsets | Reuse completed Kafka seeds after a failed or stopped attempt |
| Independent bounded date units | Run units concurrently while preserving one ordered input partition within each unit |
| Flink checkpoints and transactional Kafka journal | Recover operator progress and journal writes together |
| Wait for Flink `FINISHED`, then replace the unit's Iceberg rows | Publish history after processing, without pushing replay rows into Redis |
| Retain failed units' seeds; delete successful units' topics after writing | Preserve retry inputs and reclaim completed temporary data |

[`history.py`](../../src/data-pipeline/streaming/fraud_streaming/history.py) implements planning and seed reuse; [`stream-history`](../../infra/charts/data-pipelines/templates/stream-history.yaml) orders processing, writing and cleanup. Point-in-time retrieval and label cutoffs remain in the [feature-store note](../ml/feature-store.md#online-and-offline-paths), and customer-version joins in [Storage](storage.md#schemas-and-naming).

## Validation and remaining evaluation

The [JVM correctness comparison](online.md#local-correctness-comparison) checks 2,498 serialized snapshots from 503 deliveries against an independent reference, including late events, duplicates, conflicts and coverage boundaries. [`SourceRecoveryTest`](../../src/data-pipeline/streaming/jvm/src/test/java/dev/fraud/stream/SourceRecoveryTest.java) covers source-clock recovery; [`OperatorsTest`](../../src/data-pipeline/streaming/jvm/src/test/java/dev/fraud/stream/OperatorsTest.java) covers state and timer behavior; [`KafkaPipelineTest`](../../src/data-pipeline/streaming/jvm/src/test/java/dev/fraud/stream/KafkaPipelineTest.java) exercises Kafka integration.

Recorded platform screenshots and historical optimization measurements remain in [Historical replay](online.md#historical-replay). Those measurements describe the earlier runtime and do not establish a JVM speedup. The current full replay-to-training run has not yet completed.

The remaining evaluation is to compare live-style and replay outputs on identical deliveries, measure replay throughput and resource use on a fixed input, and compare recovery with retained seeds against reseeding. No end-to-end feature agreement, recovery speedup or model-quality improvement is claimed from the operator tests alone.
