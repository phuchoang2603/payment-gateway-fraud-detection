## ADDED Requirements

### Requirement: Reproducible model-server benchmark
The repository SHALL provide an on-demand benchmark job using inference-perf at a pinned version. It SHALL run an agent-shaped shared-prefix workload (three prefix groups with 1,500-token system prompts, 100-token questions and fixed 150-token outputs) at closed-loop concurrency 1, 2, 4, 8 and 16. It SHALL report output tokens per second, requests per second, TTFT and inter-token latency at p50 and p90, end-to-end latency, prefix-cache hit share and failures. The job SHALL authenticate with the benchmark client's own gateway key and SHALL NOT write the key to logs or results.

#### Scenario: Run against the model server
- **WHEN** a benchmark run is started for a named configuration
- **THEN** its report is written under the run's name in the environment's run storage with the configuration that produced it

### Requirement: Measured optimizations
The benchmark SHALL compare each optimization against the previous kept configuration, one change at a time: prefix caching, FP8 KV cache, n-gram speculative decoding, and PVC-cached cold start (and FP8 against AWQ weights where both run). It SHALL include one end-to-end run through the agent gateway with the final configuration.

#### Scenario: Optimization not kept
- **WHEN** an optimization lowers throughput or raises latency for the agent workload
- **THEN** the result is recorded and the previous configuration stays in the deployed model server

### Requirement: Evidence from real runs only
Recorded benchmark numbers SHALL come only from runs actually performed, each with the command, image digest, server arguments, environment and date. Results SHALL NOT be estimated, copied from other projects or extrapolated.

#### Scenario: Missing run
- **WHEN** a planned variant was not run
- **THEN** the documentation marks it as not run instead of showing a number
