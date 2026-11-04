## Purpose

Build the versioned `payment_risk_v1` training set entirely through the Feast feature service, so that training reads exactly the features serving uses.

## ADDED Requirements

### Requirement: Entity rows and labels from Feast
The training set SHALL have one row per entity row returned by the feature store's training entity rows for the run's window and cutoff: the simulation-truth label rows of the attempts in the window, with their request fields, at `event_timestamp = started_at`. The label SHALL be the simulation-truth label. The training code SHALL NOT build entity rows, labels or request fields itself.

#### Scenario: Attempt in the window

- **WHEN** an attempt started inside the window, and its simulation-truth label is available by the cutoff
- **THEN** the training set has one row for it, labelled with its simulation-truth label

### Requirement: Features from the payment_risk_v1 service
The feature columns SHALL be exactly the features of the `payment_risk_v1` feature service, including its on-demand request features, retrieved with `get_historical_features` with full feature names. The build SHALL NOT compute, fill or transform feature values, so nulls stay null. A window that the service's stream history does not cover SHALL fail the build.

#### Scenario: Window beyond the replay

- **WHEN** a run requests a window starting before the replayed stream history
- **THEN** the build fails, naming the covered range

### Requirement: Stable negative sampling
The training set SHALL keep every positive row, and every negative row whose stable hash bucket of the attempt key falls below the stored rate. The bucket SHALL be stored on the row, so that a consumer can sample a lower rate and weight negatives by the inverse rate. The same attempt SHALL get the same bucket in every run.

#### Scenario: Two runs over unchanged tables

- **WHEN** the training set is built twice from unchanged tables, with the same window and cutoff
- **THEN** both runs contain the same rows with the same values

### Requirement: Recorded provenance
A run SHALL record, at its start, the current snapshot ID of every table the feature store reads for it, and SHALL fail if any of them changed before the version is written. Each version SHALL record:

- those snapshot IDs;
- the window, the cutoff and the sampling rate;
- the feature-service name.

The build SHALL NOT write to any table it reads.

Every training run SHALL record, on its run record (the pipeline run and the Model Registry version it produces), the Iceberg snapshot IDs of `gold.feat_customer_daily`, `gold.feat_payment_method_daily`, each `gold.feat_stream_*` table and `gold.label_attempt` that its training-set version was built from, and the feature-service name.

#### Scenario: Table changes during a build

- **WHEN** a source table gets a new snapshot while a build is running
- **THEN** the build fails without writing a version

#### Scenario: Trace a model to its rows

- **WHEN** someone looks up a registered model version
- **THEN** its run record names the feature service and the snapshot IDs of every daily, stream and label table its training rows came from

### Requirement: Incremental training-set versions
The training set SHALL be one versioned lakehouse table. Each run SHALL produce exactly one new version, holding exactly that run's rows. That version SHALL store only the inserted, changed and deleted rows relative to the previous version, not a full copy. Any recorded version SHALL be readable later by its version identifier.

#### Scenario: Rebuild after a small change

- **WHEN** a run's rows differ from the previous version's in 1% of rows
- **THEN** the new version writes data for only those rows, and the previous version remains readable unchanged

### Requirement: Training-set validation
Before a version is used for training, validation SHALL fail the run when:

- a feature column is missing, or has the wrong type;
- the key is duplicated;
- a label is null;
- the per-day fraud rate is outside the recorded bounds.

The report SHALL state the null share per feature and day. It SHALL be recorded with the run whether the validation passes or fails.

#### Scenario: Duplicated attempt

- **WHEN** a version contains two rows for one attempt key
- **THEN** validation fails, and no training starts on that version
