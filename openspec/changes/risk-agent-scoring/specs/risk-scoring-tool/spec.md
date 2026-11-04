## Purpose

Let the risk agent score one payment with the served fraud model and see which features stand out, through a read-only MCP server that reads the same online features Go Fraud reads and calls the same model.

## ADDED Requirements

### Requirement: Read-only risk MCP server
The repository SHALL provide an MCP server named `risk` with two tools, `get_risk_features` and `score_payment_risk`. Both SHALL be read-only: they SHALL NOT write to any store, change a decision, or call anything that does. Analyst and agent calls SHALL reach the server only through the agent gateway; a dedicated rollout-analysis identity MAY call its preview Service directly for bounded pre-promotion checks.

#### Scenario: Agent lists tools
- **WHEN** a client with the agents' key lists tools on `/mcp/risk`
- **THEN** it receives `get_risk_features` and `score_payment_risk`, each described as read-only, and `score_payment_risk` states that its result is a re-score of the given inputs, not the recorded decision

### Requirement: Online features read as Go Fraud reads them
For a customer and a payment-method fingerprint, the tools SHALL read the `payment_risk_v1` feature service from the online store with the entities and request fields the caller gives, and SHALL report each of the five views (`customer_daily`, `payment_method_daily`, `customer_stream`, `payment_method_stream`, `request_features`) with a status and its values. A daily or stream view SHALL be `available` only when it has a row that is not newer than the scoring time and not older than its time to live, `missing` when it has no row, `future` or `expired` otherwise, and `error` or `timeout` when it could not be read. A view that is not `available` SHALL carry no values.

#### Scenario: Customer with no history
- **WHEN** the customer has no daily row
- **THEN** `customer_daily` is reported `missing` with no values, and the other views are reported as they are

#### Scenario: Stale stream row
- **WHEN** a stream view's newest row is older than its time to live
- **THEN** the view is reported `expired` and none of its values is used

#### Scenario: Feature store unreachable
- **WHEN** the feature server does not answer within the tool's deadline
- **THEN** the tool returns an error naming the feature store, and no features or score

### Requirement: Score from the served model
`score_payment_risk` SHALL lay the features out in the feature order the served model was packaged with, send one row to the model server over the Open Inference Protocol v2 with binary tensor data, and return the fraud probability, the model version that answered, the decline threshold the version carries, whether the probability is at or above that threshold, and each view's status. Numbers in a result SHALL carry at most four decimals, so that a gateway that masks long digit runs as card numbers does not alter them; the comparison with the threshold SHALL use the unrounded values. A feature of a view that is not `available`, or that the view holds no number for, SHALL be sent as missing. The tool SHALL NOT invent a value for a missing feature or a score when the model did not answer.

#### Scenario: All views available
- **WHEN** an agent scores a customer and method whose five views are available
- **THEN** the tool returns a probability between 0 and 1, the model version, the decline threshold and `available` for every view

#### Scenario: One view missing
- **WHEN** a view is not available
- **THEN** the row sends that view's features as missing, the score is still returned, and the answer lists the view as not available so the agent can say the score used incomplete data

#### Scenario: Model server down
- **WHEN** the model server does not answer
- **THEN** the tool returns an error naming the model server and no score

### Requirement: A score matches its version's layout
The tool SHALL read the feature order, registry version and decline threshold from the model server's configuration, and SHALL return a score only when the version that answered is the version the row was laid out for. When another version answers, as during a rollout or a challenger split, the tool SHALL read the configuration again and score once more; if the versions still differ it SHALL return an error rather than a score.

#### Scenario: Rollout during a call
- **WHEN** a different model version answers than the one the row was laid out for
- **THEN** the tool re-reads the configuration, lays the row out again and returns the score of the version that answered, or an error if it cannot

### Requirement: Drivers are not attributions
Alongside the score the tool SHALL return the features that stand out, ranked by the feature's gain in the served model times how unusual the value is in the version's training reference profile, with each feature's value, its position in the training distribution and its share of the ranking. It SHALL state in its description and in its result that this is not a per-prediction attribution. It SHALL return drivers only when the reference profile belongs to the version that scored, and otherwise return none with the reason.

#### Scenario: Unusual amount
- **WHEN** a payment's amount falls in the top bin of the reference profile and the model gives that feature a high gain
- **THEN** the amount ranks among the returned drivers, with its value and its position, and the result states that the ranking is not an attribution

#### Scenario: Reference of another version
- **WHEN** the reference profile available is for a different model version than the one that scored
- **THEN** the score is returned with no drivers and a reason

### Requirement: Calls are bounded and observable
Each external read (feature server, model server, reference profile) SHALL have its own deadline inside the gateway's route timeout. The server SHALL export, for each tool, the count of calls by outcome and the call latency, and SHALL export the status each view was reported with.

#### Scenario: Tool calls
- **WHEN** an agent calls a tool successfully and then once while the model server is down
- **THEN** the counters show one success and one failure for that tool and the latency histogram has two observations

### Requirement: Access only through the gateway and to the feature store
Mesh authorization SHALL admit the agent gateway to the `risk` server, the metrics agent for scraping, and a dedicated rollout-analysis identity at its tool port for bounded preview checks. The online feature server's network policy SHALL admit the `risk` server in addition to Go Fraud. The server SHALL hold no credentials: the model server and the reference profile are cluster-local, and agents authenticate to the gateway.

#### Scenario: Direct call
- **WHEN** a pod without a gateway, metrics or rollout-analysis identity connects to the `risk` server
- **THEN** the mesh refuses the connection

#### Scenario: Feature read
- **WHEN** the `risk` server calls the online feature server
- **THEN** the connection is admitted, and a pod that is neither Go Fraud nor the `risk` server is still refused
