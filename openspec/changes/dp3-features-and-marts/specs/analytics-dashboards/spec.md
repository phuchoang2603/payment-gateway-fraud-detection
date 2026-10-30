## Purpose

Visualize the ClickHouse marts in the platform Grafana through dashboards kept as code in this repository and read through a least-privilege datasource.

## ADDED Requirements

### Requirement: Dashboards as code
The repository SHALL define three Grafana dashboards as JSON, Payments overview, Merchant drill-down and Fraud outcomes, and deliver them as ConfigMaps labelled `grafana_dashboard: "1"` for the platform sidecar. Every panel SHALL query only `marts.*` tables through the datasource UID `clickhouse-marts`, filter on the dashboard time range, and use the documented mart denominators for rates. Dashboards SHALL NOT be edited in the Grafana UI as the source of truth.

#### Scenario: Deploy dashboards

- **WHEN** the data-pipelines application syncs with dashboards enabled
- **THEN** the three dashboards appear in Grafana's payments analytics folder and render from the marts once the datasource exists

#### Scenario: Merchant selection

- **WHEN** a user picks a merchant on the Merchant drill-down dashboard
- **THEN** every panel filters to that merchant and the selected time range

### Requirement: Platform datasource prerequisites
This repository SHALL NOT install Grafana plugins, datasources or secrets in the `observability` namespace. The change SHALL list each platform request precisely:

- the ClickHouse datasource plugin and its version;
- the datasource definition, with its UID, host, port, protocol, user and default database;
- the Doppler keys and the ExternalSecret;
- the sidecar namespace coverage;
- the network path to ClickHouse.

Dashboards and DP3 SHALL deploy and run without these requests met. Only the Grafana evidence SHALL wait for them. Dashboards SHALL NOT be catalogued in OpenMetadata.

#### Scenario: Datasource not yet provisioned

- **WHEN** the dashboards are delivered before the platform adds the datasource
- **THEN** the dashboards load with a missing-datasource state, and DP3 runs are unaffected
