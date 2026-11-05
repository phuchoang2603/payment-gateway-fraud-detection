import asyncio

import httpx


async def read(client: httpx.AsyncClient, system: str, method: str, url: str, timeout: float, **kwargs):
    try:
        async with asyncio.timeout(timeout):
            async with client.stream(method, url, timeout=timeout, **kwargs) as response:
                response.raise_for_status()
                body = bytearray()
                async for chunk in response.aiter_bytes():
                    body.extend(chunk)
                    if len(body) > 256 * 1024:
                        raise ValueError("response too large")
                return bytes(body), response.headers
    except (httpx.HTTPError, TimeoutError, ValueError) as error:
        raise ValueError(f"{system} read failed ({type(error).__name__})") from error
