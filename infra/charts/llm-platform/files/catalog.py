"""Publish one release's catalog from the chart values and files in Git.

- agentregistry: agents and MCP servers.
- Langfuse: each agent's system prompt, as a new production version only when the text changed.
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
        status, current = call("GET", url, headers=headers)
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
        status, created = call("POST", f"{base}/api/public/v2/prompts", body, headers)
        required(status, created, f"langfuse: publishing {name}")
        print(f"langfuse: {name} published as version {created['version']}")


if __name__ == "__main__":
    publish_registry()
    publish_prompts(json.loads((CATALOG / "agents.json").read_text()))
