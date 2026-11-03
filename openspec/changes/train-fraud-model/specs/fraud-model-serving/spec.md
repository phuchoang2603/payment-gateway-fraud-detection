## Purpose

Serve the registered fraud model with NVIDIA Triton on the GPU, deployed and rolled back by Kubeflow pipelines. Every model score, online and offline, comes from the GPU.

## ADDED Requirements

### Requirement: Inference service contract
A registered model version SHALL be served by Triton's FIL backend, through one KServe InferenceService in the platform's ML workspace, loading the model by its Model Registry reference. The version's model directory SHALL hold a Triton repository entry whose version directory is the version's number and whose config names the ordered feature list and the registry version.

- **Request:** the Open Inference Protocol v2, one FP32 row in the packaged feature order, sent as binary tensor data so a missing value can be NaN.
- **Missing values:** a NaN feature SHALL be treated as missing by the model.
- **Response:** the probability of each class, and the version number of the model that produced it.
- **Consistency:** the same row SHALL get the same score on every call, and a score SHALL match CPU XGBoost on the same booster within 1e-5.

#### Scenario: Missing features

- **WHEN** a row sends NaN for some features
- **THEN** the service returns a score for it, with those features treated as missing

#### Scenario: Repeated call

- **WHEN** the same row is sent twice
- **THEN** the scores are identical

### Requirement: Scoring runs on the GPU
The served model, the gate's scoring and the notebook's scoring SHALL run on the GPU. Triton's instance group SHALL be `KIND_GPU`. Training, serving and scoring jobs SHALL share the cluster's GPU through one resource claim, so serving does not block training.

#### Scenario: No GPU is available

- **WHEN** the GPU node cannot be scheduled
- **THEN** the predictor stays pending and does not score on a CPU, and the fraud service records the model call as `timeout` or `error`

#### Scenario: Training while serving

- **WHEN** a fit job starts while the predictor holds the GPU
- **THEN** both run on the one GPU

### Requirement: Deployment and rollback by pipeline
The training pipeline's deploy step SHALL serve a version that passed the gate, and SHALL record the deployment on the registry version. A standalone deploy pipeline SHALL serve any registered version that passed its gate, and SHALL refuse one that did not. Rollback SHALL be that pipeline run with the previous version.

#### Scenario: Passing run deploys

- **WHEN** a training run passes the gate
- **THEN** the InferenceService serves the new version, and the registry marks it `production`

#### Scenario: Rollback

- **WHEN** the deploy pipeline deploys the previous version
- **THEN** the InferenceService returns to it, and the registry marks it `production`

### Requirement: Autoscaling and metrics
The predictor SHALL scale between a minimum and a maximum number of replicas with KEDA, on Triton's mean request queue time read from the platform's metrics store, and every replica SHALL use the shared GPU claim. Triton's metrics SHALL be scraped, and a Grafana dashboard in the platform's Grafana SHALL show the request rate, queue and compute time, replicas, GPU utilization and the fraud service's model calls.

#### Scenario: Requests start to queue

- **WHEN** the mean queue time stays above the target
- **THEN** KEDA adds a predictor replica, up to the maximum, and the new replica loads the same model version on the same GPU

#### Scenario: Rollout with autoscaling

- **WHEN** the deploy step replaces the predictor of an existing service
- **THEN** the service keeps the KEDA autoscaler class and its scaling settings
