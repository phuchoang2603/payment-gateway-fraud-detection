## ADDED Requirements

### Requirement: Shared metadata connections
Pipeline-specific discovery, profiling and snapshot validation SHALL consume centrally managed connector configuration while retaining their own run context, ordering and acceptance readback. Scheduled service discovery SHALL NOT substitute for candidate-specific pipeline acceptance or mutate its native quality definitions/results.

#### Scenario: Discover a newly created pipeline table
- **WHEN** ingestion creates candidate tables before scheduled discovery runs
- **THEN** the pipeline explicitly discovers them through the shared connection before executing its snapshot-specific checks
