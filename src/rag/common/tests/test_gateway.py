import httpx
import pytest

from fraud_rag_common import gateway
from fraud_rag_common.gateway import DIMENSION, QUERY_INSTRUCTION, Embedder


def vector(value=1.0):
    return [value] + [0.0] * (DIMENSION - 1)


def embedder_with(handler, monkeypatch):
    monkeypatch.setattr(gateway.time, "sleep", lambda seconds: None)
    real_client = httpx.Client

    def client(**kwargs):
        kwargs.pop("transport", None)
        return real_client(transport=httpx.MockTransport(handler), **kwargs)

    monkeypatch.setattr(gateway.httpx, "Client", client)
    return Embedder("http://gateway", "key", "qwen3-embedding", batch_size=2)


def test_documents_are_batched_ordered_and_normalized(monkeypatch):
    seen = []

    def handler(request):
        body = request.read().decode()
        seen.append(body)
        count = body.count('"t')
        data = [{"index": i, "embedding": vector(2.0)} for i in reversed(range(count))]
        return httpx.Response(200, json={"data": data})

    vectors = embedder_with(handler, monkeypatch).documents(["t1", "t2", "t3"])

    assert len(seen) == 2 and len(vectors) == 3
    assert vectors[0][0] == pytest.approx(1.0)


def test_rate_limited_batches_are_retried(monkeypatch):
    responses = [
        httpx.Response(429, headers={"retry-after": "1"}),
        httpx.Response(200, json={"data": [{"index": 0, "embedding": vector()}]}),
    ]

    vectors = embedder_with(lambda request: responses.pop(0), monkeypatch).documents(["t1"])

    assert len(vectors) == 1 and not responses


def test_wrong_dimension_is_rejected(monkeypatch):
    handler = lambda request: httpx.Response(200, json={"data": [{"index": 0, "embedding": [1.0, 0.0]}]})  # noqa: E731

    with pytest.raises(ValueError, match="dimension"):
        embedder_with(handler, monkeypatch).documents(["t1"])


def test_queries_carry_the_instruction(monkeypatch):
    bodies = []

    def handler(request):
        bodies.append(request.read().decode())
        return httpx.Response(200, json={"data": [{"index": 0, "embedding": vector()}]})

    embedder_with(handler, monkeypatch).queries(["why declined"])

    assert QUERY_INSTRUCTION.split("\n")[0] in bodies[0]
