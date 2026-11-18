import asyncio

import httpx

RESET = (httpx.ReadError, httpx.ConnectError, httpx.RemoteProtocolError)


async def _once(client: httpx.AsyncClient, method: str, url: str, timeout: float, **kwargs):
    async with client.stream(method, url, timeout=timeout, **kwargs) as response:
        response.raise_for_status()
        body = bytearray()
        async for chunk in response.aiter_bytes():
            body.extend(chunk)
            if len(body) > 256 * 1024:
                raise ValueError("response too large")
        return bytes(body), response.headers


async def read(client: httpx.AsyncClient, system: str, method: str, url: str, timeout: float, **kwargs):
    try:
        async with asyncio.timeout(timeout):
            try:
                return await _once(client, method, url, timeout, **kwargs)
            except RESET:
                # NOTE: every read here is idempotent, so one retry after a connection reset is safe.
                return await _once(client, method, url, timeout, **kwargs)
    except (httpx.HTTPError, TimeoutError, ValueError) as error:
        raise ValueError(f"{system} read failed ({type(error).__name__})") from error
