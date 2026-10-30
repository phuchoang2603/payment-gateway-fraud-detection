# LLM agents

Fraud analysts ask an agent about decline codes, risk decisions and payment behaviour; it answers from the knowledge base and cites the passages it used. Agents are read-only: they cannot change risk decisions, retry payments, issue refunds or promote models, and they never sit in the payment decision path.

**Status:** phase 1 is built and running on dev: the inference platform, the agent gateway, the RAG pipeline with its Web API and MCP tool, and `evidence-agent`. The scoring tool, the second agent, the coordinator and the A2A gateway are later phases ([roadmap](../overview/roadmap.md)).

- Chart: [`infra/charts/llm-platform`](../../infra/charts/llm-platform): [`agents.yaml`](../../infra/charts/llm-platform/templates/agents.yaml) (model config, MCP servers, agents, worker pool), [`sandbox-egress.yaml`](../../infra/charts/llm-platform/templates/sandbox-egress.yaml), [`catalog.yaml`](../../infra/charts/llm-platform/templates/catalog.yaml) and [`files/catalog.py`](../../infra/charts/llm-platform/files/catalog.py) (registry, Langfuse, OpenMetadata), prompt [`files/prompts/evidence-agent.md`](../../infra/charts/llm-platform/files/prompts/evidence-agent.md).
- Runtime: kagent 0.10.4 with Agent Substrate 0.0.9, installed by the platform.

## Who does what

| Component | Owns |
|---|---|
| kagent | Defining agents: model config, tools, prompt reference, UI chat, A2A endpoint |
| Agent Substrate | Running them: one gVisor actor per conversation, suspended to a snapshot when idle and resumed on any worker |
| agentgateway | Every model and tool call: keys, limits, PII masking ([gateway](gateway.md)) |
| `rag-api` | The knowledge tools; their list lives in its code ([RAG](rag.md)) |
| Langfuse | The prompt's versions (published from Git) and the agents' traces ([observability](observability.md)) |
| agentregistry | The catalog of agents and MCP servers |
| OpenMetadata | Models, MCP servers and agents linked to their data ([observability](observability.md#ai-assets-in-openmetadata)) |

## Global model config

One kagent `ModelConfig`, `default-model-config`, used by every agent: provider OpenAI, model `qwen3-8b`, the agents' gateway key, and the base URL of the gateway's `/v1` route reached through the sandbox egress proxy. Changing the model, the gateway or the key for all agents is one edit.

![kagent model config: default-model-config in llm-agents, base URL http://sandbox-egress.llm.svc.cluster.local/v1, key Secret llm-gateway-agents](images/agents-modelconfig.png)

## evidence-agent

`evidence-agent` 0.3.0 is a declarative kagent agent with the read-only fraud-analyst prompt and the `knowledge` MCP server. It references the server without a tool list, so it gets every tool the server lists; kagent discovers them:

![kagent MCP page: llm-agents/knowledge with search_knowledge and get_knowledge_chunk](images/agents-mcp-tools.png)

A chat in the kagent UI: the agent calls `search_knowledge` once through the gateway and answers with the conditions and what to check, citing the chunk ids it used:

![kagent chat: question, one successful search_knowledge call, and the answer](images/agents-chat-tool-call.png)

![The end of the same answer, with the cited chunk ids](images/agents-chat-citations.png)

The kagent UI is published by the platform behind its Dex login:

![The kagent UI redirects to the platform's Dex login](images/agents-ui-dex-login.png)

The agent panel shows "Unknown Tool" because kagent's UI only names tools listed on the agent; the chat and the MCP page show the discovered tools.

## Sandbox and warm-up

Agents run only as kagent `SandboxAgent`s on Agent Substrate, in their own namespace `llm-agents`:

![kagent agents: llm-agents/evidence-agent with the "Sandbox: Agent Substrate" lock](images/agents-kagent-list.png)

- **Isolation.** Each conversation is its own gVisor actor. Its network allow-list holds only `sandbox-egress`, a small reverse proxy in `llm` that forwards only the gateway's model and MCP routes and Langfuse's trace intake, and calls them with its own mesh identity. A NetworkPolicy admits only the Substrate workers, the kagent controller and kubelet probes to the proxy.
- **Why a separate namespace.** A `SandboxAgent` can only use a WorkerPool in its own namespace, and Substrate workers run privileged gVisor sandboxes outside the mesh. `llm-agents` is platform-created with Pod Security `privileged` and no mesh enrollment, and holds only the agents and their pool; `llm` stays `restricted`.
- **Warm-up.** Each agent revision has a golden snapshot taken once it is warm; a new conversation starts from it, an idle one is snapshotted and suspended (taking no worker), and a later message resumes it on any free worker with its context.

### Several conversations on few workers

Three conversations sent at once to the two-worker pool (2026-10-30):

```text
fleet-…-1: completed after 13.3 s, 1 attempt,   tools=['search_knowledge']
fleet-…-2: completed after 26.2 s, 6 attempts,  tools=['search_knowledge']   # waited while both workers were busy
fleet-…-0: completed after 45.8 s, 10 attempts, tools=['search_knowledge']   # waited while both workers were busy
resume of fleet-…-0 after 60 s idle: completed after 5.2 s: "The decline code you asked about is NEW_ACCOUNT_GEO_MISMATCH."

Substrate API calls per conversation actor:
  CreateActor  ×1 each   SuspendActor ×1 each   ResumeActor  15 / 3 / 8
```

Every conversation was created once, suspended after its answer and resumed for the follow-up; two workers served three conversations, and the third waited for a free worker instead of failing.

## Registry

The release catalog job publishes the agents and MCP servers from the chart values to agentregistry (prompts are in Langfuse, tools come from the servers):

![agentregistry: the knowledge MCP server, 0.1.0](images/agents-registry-mcp-server.png)

![agentregistry: evidenceagent 0.3.0, model config and prompt reference](images/agents-registry-agent.png)

## Prompts

The system prompt is a file in Git, [`files/prompts/evidence-agent.md`](../../infra/charts/llm-platform/files/prompts/evidence-agent.md), versioned with the agent. kagent reads it from a ConfigMap rendered from that file (kagent 0.10.4 cannot fetch a prompt at runtime). The release catalog job publishes the same text to Langfuse as `evidence-agent-system-prompt`, labelled `production`, creating a new version only when the text changed; Langfuse is the place to browse, compare and try prompt versions ([observability](observability.md#langfuse)).

## Autoscaling

| Workload | Mechanism | Why |
|---|---|---|
| Agent conversations | Substrate actors: created on demand, suspended to zero when idle, resumed on any worker | Idle conversations cost nothing; any number wait in snapshots |
| Agent worker pool (`llm-agents`) | Fixed at 2 workers | One worker runs one active conversation. KEDA cannot scale the pool yet: Substrate 0.0.9's WorkerPool scale subresource has no `status.selector`, which the HPA behind KEDA requires |
| Knowledge API (`rag-api`) | KEDA, 2 to 4 replicas on its combined REST and MCP request rate | See [RAG](rag.md#load-test-and-autoscaling) |
| Model servers | One replica per model | One GPU per environment |
