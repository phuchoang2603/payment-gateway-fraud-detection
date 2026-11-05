import asyncio
import math

import httpx
import pytest

from fraud_rag_common import mcp_analysis as analysis
from fraud_rag_common.mcp_analysis import analyze, validate


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


@pytest.mark.parametrize("status", ["available", "missing", "expired", "future", "error", "timeout"])
def test_analysis_accepts_missing_history_but_rejects_failed_feature_reads(status):
    data = {
        "score": 0.5,
        "model_version": "4",
        "decline_threshold": 0.7,
        "views": {
            view: {"status": "available"}
            for view in (
                "customer_daily",
                "payment_method_daily",
                "customer_stream",
                "payment_method_stream",
                "request_features",
            )
        },
    }
    data["views"]["customer_daily"]["status"] = status
    if status in {"error", "timeout"}:
        with pytest.raises(ValueError, match="feature views"):
            validate("score_payment_risk", {"structuredContent": data})
    else:
        validate("score_payment_risk", {"structuredContent": data})


def test_analysis_rejects_missing_request_features():
    data = {
        "score": 0.5,
        "model_version": "4",
        "decline_threshold": 0.7,
        "views": {
            view: {"status": "missing"}
            for view in (
                "customer_daily",
                "payment_method_daily",
                "customer_stream",
                "payment_method_stream",
                "request_features",
            )
        },
    }
    with pytest.raises(ValueError, match="request features"):
        validate("score_payment_risk", {"structuredContent": data})


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


def serve(monkeypatch, upstream):
    original = httpx.AsyncClient
    monkeypatch.setattr(analysis, "RETRY_SECONDS", 0)
    monkeypatch.setattr(
        httpx, "AsyncClient", lambda **kwargs: original(transport=httpx.MockTransport(upstream), **kwargs)
    )


def test_analysis_waits_for_a_preview_that_is_not_yet_an_endpoint(monkeypatch):
    attempts = []

    def upstream(request):
        import json

        body = json.loads(request.content)
        if body["method"] == "initialize":
            attempts.append(1)
            if len(attempts) < 3:
                raise httpx.ReadError("connection reset", request=request)
            return httpx.Response(200, json={"jsonrpc": "2.0", "id": 1, "result": {"protocolVersion": "2025-06-18"}})
        if body["method"] == "tools/list":
            result = {"tools": [{"name": "get_knowledge_chunk"}]}
        else:
            result = {"structuredContent": {"chunk_id": "known", "text": "passage", "knowledge_version": "v1"}}
        return httpx.Response(200, json={"jsonrpc": "2.0", "id": 1, "result": result})

    serve(monkeypatch, upstream)
    asyncio.run(analyze("http://preview/mcp", "get_knowledge_chunk", {"chunk_id": "known"}))
    assert len(attempts) == 3


def test_analysis_fails_when_the_preview_never_answers(monkeypatch):
    monkeypatch.setattr(analysis, "REACHABLE_SECONDS", 0)

    def upstream(request):
        raise httpx.ReadError("connection reset", request=request)

    serve(monkeypatch, upstream)
    with pytest.raises(httpx.ReadError):
        asyncio.run(analyze("http://preview/mcp", "get_knowledge_chunk", {"chunk_id": "known"}))


def test_analysis_does_not_retry_a_preview_that_answers_with_an_error(monkeypatch):
    calls = []

    def upstream(request):
        calls.append(1)
        return httpx.Response(200, json={"jsonrpc": "2.0", "id": 1, "error": {"code": -32603, "message": "boom"}})

    serve(monkeypatch, upstream)
    with pytest.raises(ValueError, match="JSON-RPC"):
        asyncio.run(analyze("http://preview/mcp", "get_knowledge_chunk", {"chunk_id": "known"}))
    assert len(calls) == 1
