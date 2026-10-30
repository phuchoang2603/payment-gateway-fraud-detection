"""Publish one release's catalog from the chart values and files in Git.

- agentregistry: agents and MCP servers.
- Langfuse: each agent's system prompt, as a new production version only when the text changed.
- OpenMetadata: the LLM service, models, MCP servers with the tools they list, and agents as AI applications.
"""

import base64
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

CATALOG = Path("/etc/catalog")
MCP_ACCEPT = "application/json, text/event-stream"


def call(
    method: str, url: str, body=None, headers=None, content_type="application/json"
):
    data = (
        body if body is None or isinstance(body, bytes) else json.dumps(body).encode()
    )
    request = urllib.request.Request(
        url,
        data=data,
        method=method,
        headers={"Content-Type": content_type, **(headers or {})},
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            raw = response.read()
            if response.headers.get("Content-Type", "").startswith("text/event-stream"):
                events = [
                    line[len("data:") :].strip()
                    for line in raw.decode().splitlines()
                    if line.startswith("data:")
                ]
                raw = events[-1].encode() if events else b""
            return response.status, json.loads(raw or b"null"), response.headers
    except urllib.error.HTTPError as error:
        return error.code, error.read().decode(errors="replace")[:500], error.headers


def required(status: int, result, what: str):
    if status >= 300:
        sys.exit(f"{what} failed with HTTP {status}: {result}")
    return result


def publish_registry() -> None:
    status, result, _ = call(
        "POST",
        os.environ["REGISTRY_URL"] + "/v0/apply",
        (CATALOG / "registry.yaml").read_bytes(),
        content_type="application/yaml",
    )
    print(f"agentregistry: HTTP {status} {json.dumps(result)[:2000]}")
    if status >= 300 or '"status": "failed"' in json.dumps(result):
        sys.exit(1)


def publish_prompts(agents: list[dict]) -> None:
    base = os.environ["LANGFUSE_URL"].rstrip("/")
    keys = f"{os.environ['LANGFUSE_PUBLIC_KEY']}:{os.environ['LANGFUSE_SECRET_KEY']}"
    headers = {"Authorization": "Basic " + base64.b64encode(keys.encode()).decode()}
    for agent in agents:
        name = f"{agent['name']}-system-prompt"
        text = (CATALOG / f"prompt-{agent['name']}.md").read_text()
        url = (
            f"{base}/api/public/v2/prompts/{urllib.parse.quote(name)}?label=production"
        )
        status, current, _ = call("GET", url, headers=headers)
        if status == 200 and current["prompt"] == text:
            print(f"langfuse: {name} unchanged at version {current['version']}")
            continue
        if status not in (200, 404):
            sys.exit(f"langfuse: reading {name} failed with HTTP {status}: {current}")
        body = {
            "name": name,
            "type": "text",
            "prompt": text,
            "labels": ["production"],
            "tags": [agent["name"]],
            "commitMessage": f"{agent['name']} {agent['version']} from Git",
        }
        status, created, _ = call(
            "POST", f"{base}/api/public/v2/prompts", body, headers
        )
        required(status, created, f"langfuse: publishing {name}")
        print(f"langfuse: {name} published as version {created['version']}")


def list_tools(url: str) -> list[dict]:
    """The tools an MCP server lists through the gateway, so the catalog always matches the server."""
    headers = {
        "Accept": MCP_ACCEPT,
        "Authorization": f"Bearer {os.environ['GATEWAY_API_KEY']}",
    }
    initialize = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "protocolVersion": "2025-06-18",
            "capabilities": {},
            "clientInfo": {"name": "catalog", "version": "1"},
        },
    }
    status, result, response_headers = call("POST", url, initialize, headers)
    required(status, result, f"mcp: initialize {url}")
    if session := response_headers.get("Mcp-Session-Id"):
        headers["Mcp-Session-Id"] = session
    status, result, _ = call(
        "POST", url, {"jsonrpc": "2.0", "method": "notifications/initialized"}, headers
    )
    required(status, result, f"mcp: initialized {url}")
    status, result, _ = call(
        "POST", url, {"jsonrpc": "2.0", "id": 2, "method": "tools/list"}, headers
    )
    tools = required(status, result, f"mcp: tools/list {url}")["result"]["tools"]
    return [
        {"name": tool["name"], "description": (tool.get("description") or "").strip()}
        for tool in tools
    ]


def publish_openmetadata(assets: dict, agents: list[dict]) -> None:
    base = os.environ["OPENMETADATA_URL"].rstrip("/")
    headers = {"Authorization": f"Bearer {os.environ['OPENMETADATA_TOKEN']}"}

    def put(path: str, body: dict) -> dict:
        status, result, _ = call("PUT", f"{base}/v1/{path}", body, headers)
        entity = required(status, result, f"openmetadata: PUT {path}") or {}
        # PUT keeps an existing description; Git is the source, so replace it when it differs.
        if entity.get("id") and entity.get("description") != body.get("description"):
            patch = [
                {"op": "add", "path": "/description", "value": body["description"]}
            ]
            status, result, _ = call(
                "PATCH",
                f"{base}/v1/{path}/{entity['id']}",
                json.dumps(patch).encode(),
                headers,
                content_type="application/json-patch+json",
            )
            entity = required(status, result, f"openmetadata: PATCH {path} description")
        return entity

    def get(path: str) -> dict:
        status, result, _ = call("GET", f"{base}/v1/{path}", headers=headers)
        return required(status, result, f"openmetadata: GET {path}")

    llm = assets["llmService"]
    put(
        "services/llmServices",
        {
            "name": llm["name"],
            "serviceType": "CustomLLM",
            "description": f"Self-hosted models behind the agent gateway at {llm['baseUrl']}; clients use API keys.",
        },
    )
    models = {}
    for model in assets["models"]:
        models[model["name"]] = put(
            "llmModels",
            {
                "name": model["name"],
                "service": llm["name"],
                "baseModel": model["repo"],
                "modelVersion": model["revision"],
                "modelProvider": model["repo"].split("/")[0],
                "description": model["description"],
                "deploymentInfo": {
                    "deploymentType": "SelfHosted",
                    "endpoint": llm["baseUrl"],
                },
            },
        )
    servers = {}
    put(
        "services/mcpServices",
        {
            "name": assets["mcpService"],
            "serviceType": "Mcp",
            "description": "MCP servers behind the agent gateway.",
        },
    )
    for server in assets["mcpServers"]:
        tools = list_tools(server["url"])
        servers[server["name"]] = put(
            "mcpServers",
            {
                "name": server["name"],
                "service": assets["mcpService"],
                "serverType": "DataAccess",
                "transportType": "StreamableHTTP",
                "developmentStage": "Development",
                "description": server["description"],
                "connectionConfig": {"url": server["url"]},
                "capabilities": {"toolsSupported": True},
                "tools": [
                    {
                        "name": tool["name"],
                        "description": tool["description"],
                        "toolCategory": "SearchOperation",
                        "riskLevel": "Low",
                        "sideEffects": False,
                        "idempotent": True,
                    }
                    for tool in tools
                ],
            },
        )
        print(
            f"openmetadata: MCP server {server['name']} with tools {[tool['name'] for tool in tools]}"
        )
    tables = {
        server["name"]: [
            get(f"tables/name/{urllib.parse.quote(fqn, safe='')}")
            for fqn in server["tables"]
        ]
        for server in assets["mcpServers"]
    }
    for agent in agents:
        primary = models[assets["primaryModel"]]
        delegates = (
            f" Delegates to {', '.join(agent['agents'])} over A2A."
            if agent["agents"]
            else ""
        )
        put(
            "aiApplications",
            {
                "name": agent["name"],
                "applicationType": "Agent",
                "developmentStage": "Development",
                "description": f"{agent['description']} kagent agent, version {agent['version']}.{delegates}",
                "primaryModel": primary["fullyQualifiedName"],
                "modelConfigurations": [
                    {
                        "model": {"id": primary["id"], "type": "llmModel"},
                        "purpose": "Primary",
                    }
                ],
                "tools": [
                    {"id": servers[name]["id"], "type": "mcpServer"}
                    for name in agent["mcpServers"]
                ],
                "knowledgeBases": [
                    {"id": table["id"], "type": "table"}
                    for name in agent["mcpServers"]
                    for table in tables[name]
                ],
                "framework": {"name": "Custom", "language": "Go"},
            },
        )
        print(f"openmetadata: agent {agent['name']} {agent['version']}")


if __name__ == "__main__":
    agents = json.loads((CATALOG / "agents.json").read_text())
    publish_registry()
    publish_prompts(agents)
    publish_openmetadata(json.loads((CATALOG / "assets.json").read_text()), agents)
