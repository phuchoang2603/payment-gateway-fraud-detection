import asyncio
import math

import httpx
import pytest

from fraud_risk_serving.analysis import analyze, validate


@pytest.mark.parametrize(
    "result",
    [
        {"isError": True, "structuredContent": {"score": 0.5}},
        {"structuredContent": {"score": math.nan, "model_version": "4", "decline_threshold": 0.7}},
        {"structuredContent": {"score": 0.5, "decline_threshold": 0.7}},
        {},
    ],
)
def test_analysis_blocks_tool_errors_and_invalid_score_contracts(result):
    with pytest.raises(ValueError):
        validate("score_payment_risk", result)


def test_analysis_rejects_missing_knowledge_data():
    with pytest.raises(ValueError, match="versioned knowledge"):
        validate("get_knowledge_chunk", {"structuredContent": {"chunk_id": "known"}})


def test_analysis_initializes_lists_and_calls_preview_before_passing(monkeypatch):
    methods = []
    original = httpx.AsyncClient

    def upstream(request):
        import json

        body = json.loads(request.content)
        method = body["method"]
        methods.append(method)
        if method == "initialize":
            result = {"protocolVersion": "2025-06-18"}
        elif method == "tools/list":
            result = {"tools": [{"name": "get_knowledge_chunk"}]}
        else:
            assert request.headers["MCP-Protocol-Version"] == "2025-06-18"
            assert body["params"]["arguments"] == {"chunk_id": "known"}
            result = {"structuredContent": {"chunk_id": "known", "text": "passage", "knowledge_version": "v1"}}
        return httpx.Response(200, json={"jsonrpc": "2.0", "id": 1, "result": result})

    monkeypatch.setattr(
        httpx, "AsyncClient", lambda **kwargs: original(transport=httpx.MockTransport(upstream), **kwargs)
    )
    asyncio.run(analyze("http://preview/mcp", "get_knowledge_chunk", {"chunk_id": "known"}))
    assert methods == ["initialize", "tools/list", "tools/call"]
