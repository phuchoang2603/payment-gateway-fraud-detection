## REMOVED Requirements

### Requirement: Prod-only profile ownership
**Reason**: The platform owns Kubeflow and its `ml` workspace, and application deployment must not create Profiles or namespaces (`talos-gitops-deployment`). The app-owned Profile and its chart were removed in #99.
**Migration**: Application ML workloads target the platform `ml` namespace; see "Private workspace integrations".

## MODIFIED Requirements

### Requirement: Private workspace integrations

The prod deployment SHALL place the application's ML resources in the platform-owned `ml` namespace:

- the training service account and its namespaced permissions;
- object-storage and Iceberg-catalog credentials;
- the exploration notebook;
- the training and deploy pipeline definitions.

The Model Registry, KServe, Katib, the training operator, the GPU runtime and capacity, and user access to `ml` SHALL be platform-owned. The InferenceService SHALL be created and updated by the deploy step, not by the Git release.

Credentials SHALL come from external secret stores, without secret values in Git. The deployment SHALL use the platform's controllers, its Argo Workflows controller and its pipeline artifact storage, instead of installing duplicate services. Dev SHALL NOT render these resources.

#### Scenario: Prepare pipeline and registry resources

- **WHEN** the secret stores can read the prod credentials and the platform `ml` namespace exists
- **THEN** `ml` has the training service account, the secrets, the notebook and the pipeline versions needed to train, register and deploy through the platform's services

#### Scenario: Missing secret prerequisite

- **WHEN** the storage or catalog credentials are absent from the secret store
- **THEN** the deployment embeds no replacement values, and the dependent workloads are not claimed ready

#### Scenario: Render dev

- **WHEN** the dev Argo CD root is rendered
- **THEN** it renders no ML workspace resources
