import asyncio
import json
import math
import os

import httpx


def validate(tool, result):
    if result.get("isError"):
        raise ValueError("preview returned an MCP tool error")
    data = result.get("structuredContent")
    if not isinstance(data, dict):
        raise ValueError("preview returned no structured result")
    if tool == "score_payment_risk":
        score, threshold = data.get("score"), data.get("decline_threshold")
        if (
            type(score) not in (int, float)
            or not math.isfinite(score)
            or not 0 <= score <= 1
            or type(threshold) not in (int, float)
            or not math.isfinite(threshold)
            or not 0 < threshold <= 1
            or not data.get("model_version")
        ):
            raise ValueError("preview returned an invalid score contract")
        views = data.get("views")
        if not isinstance(views, dict) or not all(
            isinstance(views.get(view), dict) and views[view].get("status") == "available"
            for view in ("customer_daily", "payment_method_daily", "request_features")
        ):
            raise ValueError("preview fixture lacks available daily or request features")
    elif tool == "get_knowledge_chunk":
        if not all(data.get(field) for field in ("chunk_id", "text", "knowledge_version")):
            raise ValueError("preview returned no versioned knowledge passage")
    else:
        raise ValueError("unsupported analysis tool")


async def analyze(url, tool, arguments):
    headers = {"Accept": "application/json, text/event-stream"}
    async with asyncio.timeout(45), httpx.AsyncClient(timeout=30, trust_env=False) as client:

        async def rpc(method, params):
            response = await client.post(
                url, headers=headers, json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
            )
            response.raise_for_status()
            answer = response.json()
            if answer.get("error"):
                raise ValueError("preview returned a JSON-RPC error")
            return answer["result"]

        initialized = await rpc(
            "initialize",
            {
                "protocolVersion": "2025-06-18",
                "capabilities": {},
                "clientInfo": {"name": "rollout-analysis", "version": "1"},
            },
        )
        headers["MCP-Protocol-Version"] = initialized["protocolVersion"]
        tools = await rpc("tools/list", {})
        if tool not in {entry["name"] for entry in tools["tools"]}:
            raise ValueError("preview does not expose the required tool")
        result = await rpc("tools/call", {"name": tool, "arguments": arguments})
        validate(tool, result)
        print(json.dumps({"tool": tool, "outcome": "success"}))


if __name__ == "__main__":
    asyncio.run(analyze(os.environ["MCP_URL"], os.environ["MCP_TOOL"], json.loads(os.environ["MCP_ARGUMENTS"])))
