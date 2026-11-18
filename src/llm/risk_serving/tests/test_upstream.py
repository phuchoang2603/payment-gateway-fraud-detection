import asyncio

import httpx
import pytest

from fraud_risk_serving.upstream import read


async def fetch(handler):
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        return await read(client, "feature store", "GET", "http://upstream/x", 1)


def test_one_connection_reset_is_retried():
    calls = []

    def handler(request):
        calls.append(request)
        if len(calls) == 1:
            raise httpx.ReadError("connection reset", request=request)
        return httpx.Response(200, content=b"ok")

    content, _ = asyncio.run(fetch(handler))
    assert content == b"ok" and len(calls) == 2


def test_a_second_reset_fails():
    def handler(request):
        raise httpx.ReadError("connection reset", request=request)

    with pytest.raises(ValueError, match="feature store read failed"):
        asyncio.run(fetch(handler))


def test_an_error_status_is_not_retried():
    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(500)

    with pytest.raises(ValueError, match="HTTPStatusError"):
        asyncio.run(fetch(handler))
    assert len(calls) == 1


def test_a_retry_shares_the_original_deadline():
    calls = []

    async def handler(request):
        calls.append(request)
        if len(calls) == 1:
            await asyncio.sleep(0.6)
            raise httpx.ReadError("connection reset", request=request)
        await asyncio.sleep(0.6)
        return httpx.Response(200, content=b"ok")

    with pytest.raises(ValueError, match="TimeoutError"):
        asyncio.run(fetch(handler))
    assert len(calls) == 2
