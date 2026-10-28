## ADDED Requirements

### Requirement: Independent raw and curated stages

Independent finite DP1 and DP2 workflows SHALL expose their ingest/transform, validate and publish/accept stages with readable dependencies. DP2 SHALL consume only an accepted DP1 receipt and its independently pinned event/label snapshots and source selections. An already accepted DP1 input SHALL be reusable without re-ingestion. Bronze SHALL remain persisted and DP1/DP2 acceptance SHALL remain separate, across independent workflow runs. Native discovery SHALL precede curated checks. The same stage graph SHALL handle any supported schema schedule without version-pair-specific workflow templates.

#### Scenario: Curated validation fails after raw acceptance

- **WHEN** DP1 succeeds but DP2 fails a business-quality rule
- **THEN** the separate workflows and receipts retain successful raw acceptance and failed curated acceptance without advertising candidate Silver as accepted

#### Scenario: Inspect coursework orchestration evidence

- **WHEN** a workflow is used as coursework evidence
- **THEN** actual Argo screenshots show stage names, order, run identity and outcomes, linked to the workflow code and immutable data receipts
- **AND** Argo/OpenMetadata evidence is labelled by its actual tool rather than presented as Airflow/DataHub screenshots
