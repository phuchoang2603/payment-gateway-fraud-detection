import os
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Any

import httpx
from fastapi import FastAPI
from mcp.server.mcpserver import MCPServer
from mcp.server.mcpserver.exceptions import ToolError
from mcp.server.transport_security import TransportSecuritySettings
from mcp.types import ToolAnnotations
from prometheus_client import CollectorRegistry, Counter, Histogram, make_asgi_app

from fraud_risk_serving.drivers import Reference
from fraud_risk_serving.features import VIEW_FIELDS, RiskRequest, features
from fraud_risk_serving.model import Model


def rounded(value):
    # The gateway masks any run of 13 or more digits as a card number, and a full-precision float has more.
    if isinstance(value, float):
        return round(value, 4)
    if isinstance(value, dict):
        return {key: rounded(item) for key, item in value.items()}
    if isinstance(value, list):
        return [rounded(item) for item in value]
    return value


def create_app(transport=None):
    registry = CollectorRegistry()
    calls = Counter("risk_mcp_tool_calls_total", "Risk MCP calls", ["tool", "outcome"], registry=registry)
    for tool in ("get_risk_features", "score_payment_risk"):
        for outcome in ("success", "failure"):
            calls.labels(tool, outcome)
    latency = Histogram("risk_tool_latency_seconds", "Risk tool call latency", ["tool"], registry=registry)
    statuses = Counter("risk_view_status_total", "Observed view statuses", ["view", "status"], registry=registry)
    mcp = MCPServer(name="risk", instructions="Read-only online features and what-if payment re-scoring.")
    state = {}
    feast_url = os.getenv("FEAST_URL", "http://feast-server.data.svc.cluster.local:6566")

    async def invoke(tool, request, score):
        with latency.labels(tool).time():
            try:
                try:
                    result = await features(state["client"], feast_url, request, datetime.now(UTC))
                except ValueError as error:
                    status = "timeout" if "Timeout" in str(error) else "error"
                    for view in VIEW_FIELDS:
                        statuses.labels(view, status).inc()
                    raise
                for view, observed in result["views"].items():
                    statuses.labels(view, observed["status"]).inc()
                if score:
                    prediction, values = await state["model"].score(result["views"])
                    drivers = await state["reference"].drivers(prediction["model_version"], values)
                    prediction["at_or_above_decline_threshold"] = (
                        prediction["score"] >= prediction["decline_threshold"]
                    )
                    result |= (
                        prediction
                        | drivers
                        | {"score_kind": "what-if re-score of supplied inputs; not the recorded decision"}
                    )
            except ValueError as error:
                calls.labels(tool, "failure").inc()
                raise ToolError(str(error)) from error
            except Exception:
                calls.labels(tool, "failure").inc()
                raise
            calls.labels(tool, "success").inc()
            return rounded(result)

    annotations = ToolAnnotations(
        read_only_hint=True, destructive_hint=False, idempotent_hint=True, open_world_hint=False
    )

    @mcp.tool(
        description="Read-only: retrieve all five payment_risk_v1 online views with statuses. Requires the "
        "analyst's integration_id, customer_id, payment_method_fingerprint and attempt fields; never invent them.",
        annotations=annotations,
    )
    async def get_risk_features(request: RiskRequest) -> dict[str, Any]:
        return await invoke("get_risk_features", request, False)

    @mcp.tool(
        description="Read-only: call the served fraud model using online features and the supplied request fields. "
        "This is a what-if re-score, not the recorded decision. Returns score, model version, decline threshold, "
        "view statuses and a ranking of unusual, influential features, not an attribution. "
        "Ask for missing identifiers and request fields; never invent them. An error means no score.",
        annotations=annotations,
    )
    async def score_payment_risk(request: RiskRequest) -> dict[str, Any]:
        return await invoke("score_payment_risk", request, True)

    # Ambient mesh admits callers by identity; the gateway rewrites Host before forwarding MCP requests.
    mcp_app = mcp.streamable_http_app(
        stateless_http=True,
        json_response=True,
        transport_security=TransportSecuritySettings(enable_dns_rebinding_protection=False),
    )

    @asynccontextmanager
    async def lifespan(_):
        async with httpx.AsyncClient(transport=transport, follow_redirects=False, trust_env=False) as client:
            state["client"] = client
            state["model"] = Model(
                client, os.getenv("MODEL_URL", "http://payment-risk-predictor.ml.svc.cluster.local")
            )
            state["reference"] = Reference(
                client, os.getenv("REFERENCE_URL", "http://payment-risk-drift.ml.svc.cluster.local")
            )
            async with mcp.session_manager.run():
                yield
            state.clear()

    app = FastAPI(title="Payment risk MCP", version="0.1.0", lifespan=lifespan)

    @app.get("/healthz")
    async def healthz():
        return {"status": "ok"}

    @app.get("/readyz")
    async def readyz():
        return {"status": "ready"}

    app.mount("/metrics", make_asgi_app(registry=registry))
    app.mount("/", mcp_app)
    return app


app = create_app()
