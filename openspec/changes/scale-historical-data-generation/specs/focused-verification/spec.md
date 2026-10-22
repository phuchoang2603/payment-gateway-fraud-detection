## MODIFIED Requirements

### Requirement: Generator behavior tests

Simulator tests SHALL protect reproducibility for a small fixed configuration, identity vectors, Arrow schema parity with ODCS, schema evolution, duplicates, sorted file/row-group layout, truth separation and publication refusal on contract failure. Small tests SHALL also protect worker-independent records, verified resume, publication exclusivity and resource/size failure gates. They SHALL use local outputs and storage fault doubles without requiring object storage or network access beyond the pinned contract CLI.

#### Scenario: Offline test run

- **WHEN** simulator tests run in CI without storage credentials
- **THEN** they generate to a temporary directory, validate it against both contracts and verify these behaviors

#### Scenario: Interrupted upload and changed execution settings

- **WHEN** a small run stops around upload/checkpoint acknowledgement and resumes with changed parallelism
- **THEN** its records, counts, physical schemas and duplicate behavior equal an uninterrupted run
- **AND** corrupt output, incompatible inputs or a competing writer cannot be silently accepted

#### Scenario: Bounded-resource failure

- **WHEN** an injected scratch, spill or upload limit prevents completion
- **THEN** the test verifies diagnostic failure and absent completion manifest while valid checkpointed work remains resumable

#### Scenario: Normal command lifecycle

- **WHEN** the same command is run against prepared or completed output
- **THEN** it automatically resumes compatible preparation or finishes protection and cleanup without additional mode flags
- **AND** native validation remains the default while the scale configuration explicitly selects the DP1 gate

#### Scenario: Publication failure and retry

- **WHEN** accepted-object tagging loses its acknowledgement or cleanup partially fails
- **THEN** tests verify a visible failure, accurate successful-delete accounting and recovery by rerunning the same command without rewriting completed output

## ADDED Requirements

### Requirement: Measured generator scale evidence

Scale acceptance SHALL record the source/configuration/contract revisions, pilot and full-run commands, hardware, seed, elapsed time, throughput, peak process-tree memory and scratch, physical/uncompressed source bytes, distributions and validation reports. CI SHALL NOT generate 100 GB. Estimates and slices SHALL be labelled and SHALL NOT substitute for a measured full-source run.

#### Scenario: Accept the large dataset

- **WHEN** an operator reports completion of the 100 GB dataset
- **THEN** the evidence identifies its immutable completion manifest and actual source-file byte total, the full-data contract results for both tables through the declared source-native or DP1 Spark/GX gate and measured resource use
- **AND** it distinguishes pilot projections and subset checks from full-run observations
