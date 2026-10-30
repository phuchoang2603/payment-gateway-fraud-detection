import asyncio
import os
from contextlib import asynccontextmanager
from typing import Literal

import httpx
import psycopg
import uvicorn
from fastapi import FastAPI, HTTPException, Request
from fastapi import Path as PathParam
from fraud_rag_common import store
from fraud_rag_common.gateway import Embedder
from mcp.server.mcpserver import MCPServer
from mcp.server.transport_security import TransportSecuritySettings
from prometheus_client import Counter, make_asgi_app
from pydantic import BaseModel, Field

Audience = Literal["analyst", "merchant"]
CHUNK_ID = r"^[a-z0-9-]+#[a-z0-9-]+$"
REQUESTS = Counter("knowledge_api_requests_total", "Knowledge REST requests", ["endpoint", "status"])
TOOL_CALLS = Counter("knowledge_mcp_tool_calls_total", "Knowledge MCP tool calls", ["tool", "outcome"])


class Chunk(BaseModel):
    chunk_id: str
    doc_id: str
    title: str
    section: str
    audience: str
    synthetic: bool
    source_path: str
    text: str
    knowledge_version: str
    content_sha256: str


class SearchRequest(BaseModel):
    query: str = Field(min_length=1, max_length=2000)
    top_k: int = Field(default=5, ge=1, le=20)
    audience: Audience | None = None


class SearchHit(BaseModel):
    distance: float
    chunk: Chunk


class SearchResponse(BaseModel):
    hits: list[SearchHit]


class Knowledge:
    def __init__(self):
        self.embedder: Embedder | None = None
        self.client: httpx.AsyncClient | None = None
        self.feature_store = None
        self.lock = asyncio.Lock()

    # The registry exists only after the first pipeline run, and the reader role cannot create it.
    async def store(self):
        if self.feature_store is None:
            async with self.lock:
                if self.feature_store is None:
                    try:
                        feature_store = await asyncio.to_thread(store.store)
                        await asyncio.to_thread(feature_store.get_feature_view, store.VIEW)
                    except Exception as error:
                        raise HTTPException(status_code=503, detail="knowledge store not loaded yet") from error
                    self.feature_store = feature_store
        return self.feature_store

    async def chunk(self, chunk_id: str) -> Chunk | None:
        feature_store = await self.store()
        rows = await asyncio.to_thread(store.read, feature_store, [chunk_id])
        return Chunk(**rows[0]) if rows and rows[0] else None

    async def search(self, request: SearchRequest) -> SearchResponse:
        feature_store = await self.store()
        vector = await self.embedder.query(self.client, request.query)
        depth = request.top_k * 3 if request.audience else request.top_k
        candidates = await asyncio.to_thread(store.search, feature_store, vector, depth)
        rows = await asyncio.to_thread(store.read, feature_store, [chunk_id for chunk_id, _ in candidates])
        hits = [
            SearchHit(distance=distance, chunk=Chunk(**row))
            for (_, distance), row in zip(candidates, rows)
            if row and (request.audience is None or row["audience"] in (request.audience, "both"))
        ]
        return SearchResponse(hits=sorted(hits, key=lambda hit: hit.distance)[: request.top_k])


knowledge = Knowledge()
mcp = MCPServer(
    name="knowledge",
    instructions="Read-only payment-gateway knowledge base. Cite the chunk_id of every passage you use.",
)


async def counted(tool: str, call):
    try:
        result = await call
    except Exception:
        TOOL_CALLS.labels(tool, "failure").inc()
        raise
    TOOL_CALLS.labels(tool, "success").inc()
    return result


async def find_chunk(chunk_id: str) -> Chunk:
    found = await knowledge.chunk(chunk_id)
    if found is None:
        raise ValueError(f"unknown chunk_id: {chunk_id}")
    return found


@mcp.tool()
async def search_knowledge(query: str, top_k: int = 5, audience: Audience | None = None) -> SearchResponse:
    """Search the payment-gateway knowledge base: decline codes, risk policy, payment lifecycle,
    checkout API, features and support guidance. Returns passages with their chunk_id."""
    request = SearchRequest(query=query, top_k=top_k, audience=audience)
    return await counted("search_knowledge", knowledge.search(request))


@mcp.tool()
async def get_knowledge_chunk(chunk_id: str) -> Chunk:
    """Return one knowledge-base passage by its chunk_id, for example 'decline-codes#high-amount'."""
    return await counted("get_knowledge_chunk", find_chunk(chunk_id))


# Callers are restricted by mesh authorization to the agent gateway, which rewrites Host, so the
# SDK's DNS-rebinding check (meant for browser-reachable local servers) is disabled.
mcp_app = mcp.streamable_http_app(
    stateless_http=True,
    json_response=True,
    transport_security=TransportSecuritySettings(enable_dns_rebinding_protection=False),
)


@asynccontextmanager
async def lifespan(_: FastAPI):
    knowledge.embedder = Embedder.from_env()
    async with httpx.AsyncClient(timeout=30.0) as client:
        knowledge.client = client
        async with mcp.session_manager.run():
            yield


app = FastAPI(title="Payment-gateway knowledge API", version="0.1.0", lifespan=lifespan)


@app.middleware("http")
async def count_requests(request: Request, call_next):
    response = await call_next(request)
    route = request.scope.get("route")
    if route is not None and request.url.path.startswith("/v1/"):
        REQUESTS.labels(route.path, str(response.status_code)).inc()
    return response


@app.get("/healthz")
async def healthz() -> dict:
    return {"status": "ok"}


def ping() -> None:
    with psycopg.connect(**store.connection(), connect_timeout=3) as conn:
        conn.execute("SELECT 1")


@app.get("/readyz")
async def readyz() -> dict:
    try:
        await asyncio.to_thread(ping)
    except Exception as error:
        raise HTTPException(status_code=503, detail="knowledge store unavailable") from error
    return {"status": "ready"}


@app.get("/v1/chunks/{chunk_id}", response_model=Chunk, responses={404: {"description": "Unknown chunk"}})
async def get_chunk(chunk_id: str = PathParam(pattern=CHUNK_ID, max_length=200)) -> Chunk:
    found = await knowledge.chunk(chunk_id)
    if found is None:
        raise HTTPException(status_code=404, detail="unknown chunk_id")
    return found


app.mount("/metrics", make_asgi_app())
app.mount("/", mcp_app)


def main() -> None:  # pragma: no cover
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", "8080")), access_log=False)
