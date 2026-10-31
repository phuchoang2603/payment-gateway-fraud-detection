"""Publish one release's catalog from the chart values and files in Git.

- agentregistry: agents and MCP servers.
- Langfuse: each agent's system prompt (and its A/B variant) when Langfuse does not have it yet.
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
            return response.status, json.loads(response.read() or b"null")
    except urllib.error.HTTPError as error:
        return error.code, error.read().decode(errors="replace")[:500]


def required(status: int, result, what: str):
    if status >= 300:
        sys.exit(f"{what} failed with HTTP {status}: {result}")
    return result


def publish_registry() -> None:
    status, result = call(
        "POST",
        os.environ["REGISTRY_URL"] + "/v0/apply",
        (CATALOG / "registry.yaml").read_bytes(),
        content_type="application/yaml",
    )
    print(f"agentregistry: HTTP {status} {json.dumps(result)[:2000]}")
    if status >= 300 or '"status": "failed"' in json.dumps(result):
        sys.exit(1)


def seed_prompts(agents: list[dict]) -> None:
    """Create each agent prompt that Langfuse does not have yet. Langfuse is the source; an existing prompt is
    never changed, so edits and A/B tests made there are kept."""
    base = os.environ["LANGFUSE_URL"].rstrip("/")
    keys = f"{os.environ['LANGFUSE_PUBLIC_KEY']}:{os.environ['LANGFUSE_SECRET_KEY']}"
    headers = {"Authorization": "Basic " + base64.b64encode(keys.encode()).decode()}
    for agent in agents:
        name = f"{agent['name']}-system-prompt"
        status, current = call(
            "GET",
            f"{base}/api/public/v2/prompts/{urllib.parse.quote(name)}",
            headers=headers,
        )
        if status == 200:
            print(
                f"langfuse: {name} exists at version {current['version']}; left as is"
            )
            continue
        if status != 404:
            sys.exit(f"langfuse: reading {name} failed with HTTP {status}: {current}")
        text = (CATALOG / f"prompt-{agent['name']}.md").read_text()
        test = agent.get("abTest")
        versions = [
            (
                ["production"],
                {"abTest": {"label": test["label"], "share": test["share"]}}
                if test
                else {},
            )
        ]
        if test:
            versions.append(([test["label"]], test.get("config", {})))
        for labels, config in versions:
            body = {
                "name": name,
                "type": "text",
                "prompt": text,
                "config": config,
                "labels": labels,
                "tags": [agent["name"]],
                "commitMessage": f"{agent['name']} {agent['version']} seed from Git",
            }
            status, created = call(
                "POST", f"{base}/api/public/v2/prompts", body, headers
            )
            required(status, created, f"langfuse: seeding {name} {labels}")
            print(f"langfuse: {name} seeded as version {created['version']} {labels}")


if __name__ == "__main__":
    publish_registry()
    seed_prompts(json.loads((CATALOG / "agents.json").read_text()))
