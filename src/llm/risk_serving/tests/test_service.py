import json
from datetime import UTC, datetime, timedelta

import httpx
import pytest
from conftest import feast_response, inference, model_config, reference
from fastapi.testclient import TestClient

from fraud_risk_serving.app import create_app
from fraud_risk_serving.drivers import NOTICE

HEADERS = {"Accept": "application/json, text/event-stream", "MCP-Protocol-Version": "2025-06-18"}


def rpc(client, method, params=None):
    response = client.post(
        "/mcp", headers=HEADERS, json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params or {}}
    )
    assert response.status_code == 200
    return response.json()["result"]


@pytest.mark.parametrize("missing", [False, True])
def test_mcp_reads_and_scores_with_observable_success_and_failure(request_input, missing):
    broken = False

    def upstream(request):
        if request.url.path.endswith("get-online-features"):
            response = feast_response(datetime.now(UTC) - timedelta(minutes=1))
            if missing:
                for name, field in zip(response["metadata"]["feature_names"], response["results"]):
                    if name.startswith("customer_daily__"):
                        field.update(values=[None], statuses=["NOT_FOUND"], event_timestamps=["1970-01-01T00:00:00Z"])
            return httpx.Response(200, json=response)
        if broken:
            return httpx.Response(503)
        if request.url.path.endswith("config"):
            return httpx.Response(200, json=model_config())
        if request.url.path.endswith("infer"):
            return httpx.Response(200, **inference())
        return httpx.Response(200, json=reference())

    with TestClient(create_app(httpx.MockTransport(upstream))) as client:
        assert (
            'risk_mcp_tool_calls_total{outcome="success",tool="score_payment_risk"} 0.0'
            in client.get("/metrics/").text
        )
        rpc(
            client,
            "initialize",
            {"protocolVersion": "2025-06-18", "capabilities": {}, "clientInfo": {"name": "ci", "version": "1"}},
        )
        tools = rpc(client, "tools/list")["tools"]
        assert {tool["name"] for tool in tools} == {"get_risk_features", "score_payment_risk"}
        assert all(tool["annotations"]["readOnlyHint"] for tool in tools)
        arguments = {"request": request_input.model_dump(mode="json")}
        feature_result = rpc(client, "tools/call", {"name": "get_risk_features", "arguments": arguments})
        assert set(feature_result["structuredContent"]["views"]) == {
            "customer_daily",
            "customer_stream",
            "payment_method_daily",
            "payment_method_stream",
            "request_features",
        }
        success = rpc(client, "tools/call", {"name": "score_payment_risk", "arguments": arguments})
        assert not success.get("isError", False)
        assert success["structuredContent"]["drivers_notice"] == NOTICE
        assert success["structuredContent"]["model_version"] == "4"
        assert success["structuredContent"]["score"] == 0.8
        assert success["structuredContent"]["decline_threshold"] == 0.7
        assert success["structuredContent"]["at_or_above_decline_threshold"] is True
        assert success["structuredContent"]["views"]["customer_daily"]["status"] == (
            "missing" if missing else "available"
        )
        broken = True
        failure = rpc(client, "tools/call", {"name": "score_payment_risk", "arguments": arguments})
        assert failure["isError"] is True
        assert "model server" in json.dumps(failure)
        assert "structuredContent" not in failure
        metrics = client.get("/metrics/").text
        assert 'risk_mcp_tool_calls_total{outcome="success",tool="score_payment_risk"} 1.0' in metrics
        assert 'risk_mcp_tool_calls_total{outcome="failure",tool="score_payment_risk"} 1.0' in metrics
        assert 'risk_tool_latency_seconds_count{tool="score_payment_risk"} 2.0' in metrics
        status = "missing" if missing else "available"
        assert f'risk_view_status_total{{status="{status}",view="customer_daily"}} 3.0' in metrics
