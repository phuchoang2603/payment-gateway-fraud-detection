"""kagent's Python runtime (`kagent-adk static`) with the Langfuse prompt plugin and remote A2A sub-agents.

It reads the configuration kagent compiles for the Agent exactly as `kagent-adk static` does. Two additions come from
the Harness environment:

- LANGFUSE_PROMPT names the agent's prompt in Langfuse (LANGFUSE_HOST, LANGFUSE_PUBLIC_KEY and LANGFUSE_SECRET_KEY
  reach Langfuse; the keys are placeholders that Substrate's egress gateway replaces).
- REMOTE_AGENTS lists other agents this one calls over A2A through the agent gateway, as JSON
  `[{"name", "description", "url"}]`. Their requests carry a placeholder Authorization header that the egress
  gateway replaces with the agents' gateway key.
"""

import argparse
import json
import os

PLACEHOLDER = "placeholder"


def add_remote_agents(agent_config, remote_agents: list[dict]) -> None:
    for remote in remote_agents:
        entry = agent_config.remote_agents.add()
        entry.name = remote["name"]
        entry.description = remote["description"]
        entry.url = remote["url"].rstrip("/")
        entry.headers["Authorization"] = PLACEHOLDER
        entry.isolate_sessions = True


def prompt_plugin():
    from langfuse import Langfuse

    from .plugin import LangfusePromptPlugin

    client = Langfuse(
        public_key=os.environ.get("LANGFUSE_PUBLIC_KEY", PLACEHOLDER),
        secret_key=os.environ.get("LANGFUSE_SECRET_KEY", PLACEHOLDER),
        base_url=os.environ["LANGFUSE_HOST"],
        tracing_enabled=False,
    )
    return LangfusePromptPlugin(client, os.environ["LANGFUSE_PROMPT"])


def main() -> None:  # pragma: no cover - mirrors kagent-adk's `static` command around the two additions
    import uvicorn
    from kagent.adk import KAgentApp
    from kagent.adk.cli import add_tools_with_config, create_sts_integration, parse_agent_card
    from kagent.adk.types import create_agent, parse_config
    from kagent.core import KAgentConfig, bootstrap, configure_logging, instrument_app
    from kagent.core._config_materialize import materialize_from_env
    from kagent.core.telemetry import conv

    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8081)
    parser.add_argument("--filepath", default="/config")
    args = parser.parse_args()

    materialize_from_env(args.filepath)
    with open(os.path.join(args.filepath, "config.json")) as file:
        config = parse_config(json.load(file))
    add_remote_agents(config.agent, json.loads(os.environ.get("REMOTE_AGENTS", "[]")))
    runtime = config.runtime
    app_cfg = KAgentConfig(runtime=runtime)
    configure_logging(runtime.log_level or "info")
    with open(os.path.join(args.filepath, "agent-card.json")) as file:
        agent_card = parse_agent_card(json.load(file))

    sts_integration = create_sts_integration(runtime.token_propagation)
    prompts = prompt_plugin()
    plugins = [plugin for plugin in (sts_integration, prompts) if plugin is not None]

    def root_agent_factory():
        root_agent = create_agent(
            config.agent, app_cfg.name, sts_integration, runtime.token_propagation.enabled, runtime_config=runtime
        )
        add_tools_with_config(root_agent, config)
        prompts.bind(root_agent.model)
        return root_agent

    server = KAgentApp(
        root_agent_factory,
        agent_card,
        app_cfg.api_url,
        app_cfg.app_name,
        plugins=plugins,
        stream=config.agent.stream,
        config=config,
    ).build()
    bootstrap(conv.KagentRuntimeValues.ADK_PYTHON, fallback_name=agent_card.name, config=runtime)
    instrument_app(server)
    uvicorn.run(server, host=args.host, port=args.port, log_level=runtime.log_level or "info", log_config=None)
