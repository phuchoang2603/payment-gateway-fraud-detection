## Purpose

Train, evaluate and version the fraud model on the platform's Kubeflow components: a notebook for exploration, a Kubeflow pipeline for every recorded model, single-GPU training, Katib tuning, an evaluation gate and the Model Registry.

## ADDED Requirements

### Requirement: One training procedure for notebook and pipeline
The exploration notebook and the training pipeline SHALL run the same library functions for loading, splitting, training, evaluating and saving. The notebook SHALL run in a Kubeflow Notebook in the platform's ML workspace. It SHALL load a recorded training-set version, split it by time into training and validation data, train through the same GPU training step as the pipeline, evaluate on the validation data, and save the model to a file.

#### Scenario: Notebook and pipeline on the same version

- **WHEN** the notebook and a pipeline run train on the same training-set version with the same parameters and seed
- **THEN** they produce models with equal predictions on the same rows

### Requirement: Time-ordered split
Splits SHALL be chronological, never random, and SHALL be given as explicit, consecutive training, validation and test windows. Validation and test SHALL keep the stored negative sample with inverse-rate weights. Training SHALL use the stored sample, or a lower recorded rate drawn from the stored buckets, with inverse-rate weights. The pipeline SHALL fail when the windows overlap, are out of order, or when a row falls outside every window.

#### Scenario: Default windows

- **WHEN** a run uses training 2026-07-29..09-30, validation 2026-10-01..10-14 and test 2026-10-15..10-28
- **THEN** the windows are accepted, and every row lies in exactly one of them

#### Scenario: Overlapping windows

- **WHEN** a run's validation window starts before its training window ends
- **THEN** the run fails before training

### Requirement: Kubeflow training pipeline
A Kubeflow pipeline SHALL run these steps in order:

1. build or select a training-set version;
2. validate it;
3. split it by time;
4. optionally tune;
5. train;
6. evaluate and gate;
7. register in the Model Registry;
8. deploy a passing version.

This pipeline SHALL be the model's CI/CD: every registered version and deployment SHALL come from a pipeline run. Steps SHALL exchange references (version identifiers, object paths and run identifiers), never datasets. A failed validation or split check SHALL stop the run before training. The pipeline definition SHALL be deployed through Git, and each deployed definition SHALL be a new pipeline version.

#### Scenario: Validation failure

- **WHEN** the selected training-set version fails validation
- **THEN** the run stops, records the validation report, and trains, registers and deploys nothing

### Requirement: Single-GPU training
The train step SHALL train the gradient-boosted model on one GPU, through the platform's training operator, with the GPU allocated by an explicit claim. It SHALL NOT fall back to CPU training. Rows SHALL be ordered deterministically, so that the same inputs and parameters give the same model. The worker SHALL exit successfully only after the model file is stored. When no GPU can be provisioned, the step SHALL fail after its recorded timeout.

#### Scenario: GPU training run

- **WHEN** the train step runs
- **THEN** one worker pod on a GPU node trains the model, stores it, and exits successfully, and the run records the GPU type and the library versions

#### Scenario: No GPU capacity

- **WHEN** no GPU node can be provisioned
- **THEN** the step fails after its timeout, without training on CPU

### Requirement: Hyperparameter tuning
An optional tune step SHALL run a hyperparameter experiment with the platform's tuning service over a recorded search space. Trials SHALL each use one GPU. The experiment SHALL run a bounded number of trials, with no more in parallel than the GPU pool allows, and SHALL maximize validation PR-AUC. The best parameters SHALL be passed to the train step, and the experiment's name SHALL be recorded with the run.

#### Scenario: Tuning enabled

- **WHEN** a run enables tuning
- **THEN** the train step uses the experiment's best parameters, and the run records the experiment and its best trial

### Requirement: Evaluation gate
The evaluate step SHALL set the decision thresholds on validation:

- **decline threshold:** 0.1% of legitimate payments flagged;
- **step-up threshold:** 1% flagged.

On test, with the sampling weights, it SHALL report for the new model, a logistic-regression baseline and the current production version when one exists:

- PR-AUC, ROC-AUC and recall at 1% false-positive rate;
- precision and recall at the decline threshold.

The gate SHALL pass only when the model beats the baseline on test PR-AUC and, if a production version exists, gains at least the recorded PR-AUC margin over it without a worse recall at 1% false-positive rate.

#### Scenario: Worse than production

- **WHEN** a new model's test PR-AUC is below the production version's on the same test rows
- **THEN** the run reports both, does not pass, and registers and deploys nothing

### Requirement: Versioned models in the Model Registry
A passing model SHALL be registered as a new version of one registered model in the platform's Kubeflow Model Registry. The version SHALL record:

- the artifact URI;
- the hyperparameters and the test metrics;
- the thresholds;
- the feature service and its ordered feature list;
- the training-set version, and the Iceberg snapshot IDs of the daily, stream and label tables it was built from;
- the windows and the cutoff;
- the pipeline run.

The deployed version SHALL be marked `production`, and the version it replaces `archived`. A model file SHALL be loadable by XGBoost without executing code from object storage.

#### Scenario: Second passing model

- **WHEN** a second pipeline run passes the gate and deploys
- **THEN** the registry has two versions, the new one `production` and the first `archived`, each linking its own training-set version

### Requirement: Derived request features chosen with the model
The `payment_risk_v1` feature service's on-demand request view SHALL also provide derived features computed only from values already in its feature views and request fields, so no data contract changes. The candidates are:

- the payment amount relative to the entity's 30-day average amount (30-day amount sum divided by 30-day attempt count), null when the entity has no 30-day history;
- the hour of day of the scoring time;
- the days since the entity's latest daily row, kept only if the feature store can expose that row's timestamp to the on-demand view without a contract change.

The final list SHALL be chosen with the model, before the first registered version, by comparing validation PR-AUC with and without each candidate; candidates that don't improve it SHALL be dropped. The fraud service's online feature request SHALL carry the same request fields the view needs, so training and serving compute the same values.

#### Scenario: Entity without history

- **WHEN** an attempt's entity has no 30-day history
- **THEN** the relative-amount feature is null, not zero or infinite

#### Scenario: Candidate with no gain

- **WHEN** adding a candidate does not improve validation PR-AUC
- **THEN** it is not part of `payment_risk_v1` when the first version is registered

### Requirement: Training runs on the shared platform without owning it
Training workloads SHALL run in the platform's ML workspace on the platform's pipeline, training, tuning, serving, registry and notebook services, with the application's own service account and object and catalog credentials.

- Training and tuning SHALL schedule on the GPU pool.
- Pipeline and evaluation pods SHALL schedule on the interruptible CPU pool.
- The dataset step, a single pod running Spark in local mode, SHALL schedule on the interruptible CPU pool.

They SHALL NOT install or modify shared controllers, node pools or the namespace.

#### Scenario: Pipeline run

- **WHEN** a pipeline run starts
- **THEN** training and tuning pods run on GPU nodes and the other steps on the shared pools, with no new controller installed
