## Purpose

Serve the registered fraud model through a KServe custom predictor, with the model inside the image,, deployed and rolled back by Kubeflow pipelines.

## ADDED Requirements

### Requirement: Inference service contract
A registered model version SHALL be served by a custom predictor, through one KServe InferenceService in the platform's ML workspace, loading the model by its Model Registry reference.

- **Request:** JSON with a list of instances, each a map of feature names to values.
- **Missing values:** a missing or null feature SHALL be treated as missing by the model. A feature the model does not know SHALL be ignored.
- **Response:** one fraud score per instance, and the model version that produced it.
- **Consistency:** the same instances SHALL get the same scores on every call, whatever the order of the keys.

#### Scenario: Missing features

- **WHEN** an instance omits features or sends them as null
- **THEN** the service returns a score for it, with those features treated as missing

#### Scenario: Repeated call

- **WHEN** the same instances are sent twice, with their keys in a different order
- **THEN** the scores are identical

### Requirement: Deployment and rollback by pipeline
The training pipeline's deploy step SHALL serve a version that passed the gate, and SHALL record the deployment on the registry version. A standalone deploy pipeline SHALL serve any registered version that passed its gate, and SHALL refuse one that did not. Rollback SHALL be that pipeline run with the previous version.

#### Scenario: Passing run deploys

- **WHEN** a training run passes the gate
- **THEN** the InferenceService serves the new version, and the registry marks it `production`

#### Scenario: Rollback

- **WHEN** the deploy pipeline deploys the previous version
- **THEN** the InferenceService returns to it, and the registry marks it `production`
