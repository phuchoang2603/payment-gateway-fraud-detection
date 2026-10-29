# LLM agents

**Status: planned.** Nothing on this page is built yet; it fixes the scope so later work and evidence have a home. Design details (tools, corpus, authorization, evaluation) get their own OpenSpec change before implementation.

## Use case

Fraud analysts ask about a specific payment or attempt. Agents assemble the recorded timeline, the decision-time features and the model score, retrieve the relevant investigation procedures, and answer with citations. Agents are read-only: they cannot change risk decisions, retry payments, issue refunds or promote models. Labels that became available after the decision may inform a retrospective answer but are never presented as information the original decision had.

## Components

| Component | Role | Rubric area |
|---|---|---|
| LLM inference platform | Self-hosted model server with a custom model, benchmarked | Inference platform |
| Agent gateway | Single entrypoint for MCP servers and agent-to-agent calls, with authentication and policy | MCP and A2A gateway |
| Agent registry | Published agents and their versions | Registry and governance |
| RAG pipeline | Investigation procedures and policy documents → chunks → embeddings stored through Feast, with lineage in OpenMetadata | RAG data pipeline |
| Evidence MCP tool | Reads one payment's events, labels and online features by ID | Feature-retrieval MCP tool |
| Scoring tool | Calls the KServe fraud model for a stored attempt | ML inference tool |
| Evidence agent | Uses the evidence tool and RAG to explain a payment | Agent with MCP tool |
| Risk agent | Uses the scoring tool and RAG to assess similar attempts | Second agent |
| Coordinator agent | Routes an analyst question to the two agents and merges the answer | Coordinator agent |

## Boundaries

- Agents read from the stores in the [platform architecture](../overview/architecture.md) and the curated tables in [offline data](../data/offline.md). They never sit in the payment decision path.
- Customer and merchant data stays access-controlled; agents run in a sandbox with scoped credentials.
- Telemetry reuses the platform's existing observability stack.
