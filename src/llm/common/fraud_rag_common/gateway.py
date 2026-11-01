import math
import os
import time

import httpx

DIMENSION = 1024
RETRYABLE = {429, 502, 503, 504}
ATTEMPTS = 6
# Qwen3-Embedding is asymmetric: queries carry an instruction, documents are embedded as-is.
QUERY_INSTRUCTION = (
    "Instruct: Given a question about payment-gateway fraud operations, retrieve the passages that answer it\nQuery: "
)


def normalize(vector: list[float]) -> list[float]:
    norm = math.sqrt(sum(value * value for value in vector))
    if not norm or not math.isfinite(norm):
        raise ValueError("embedding has no finite, non-zero norm")
    return [value / norm for value in vector]


class Embedder:
    def __init__(self, url: str, api_key: str, model: str, batch_size: int = 32, timeout: float = 120.0):
        self.endpoint = url.rstrip("/") + "/v1/embeddings"
        self.headers = {"Authorization": f"Bearer {api_key}"}
        self.model = model
        self.batch_size = batch_size
        self.timeout = timeout

    @classmethod
    def from_env(cls) -> "Embedder":
        return cls(os.environ["GATEWAY_URL"], os.environ["GATEWAY_API_KEY"], os.environ["EMBEDDING_MODEL"])

    def _vectors(self, payload: dict) -> list[list[float]]:
        data = sorted(payload["data"], key=lambda item: item["index"])
        vectors = [normalize(item["embedding"]) for item in data]
        if any(len(vector) != DIMENSION for vector in vectors):
            raise ValueError(f"embedding dimension is not {DIMENSION}")
        return vectors

    def documents(self, texts: list[str]) -> list[list[float]]:
        transport = httpx.HTTPTransport(retries=3)
        vectors = []
        with httpx.Client(transport=transport, timeout=self.timeout, headers=self.headers) as client:
            for start in range(0, len(texts), self.batch_size):
                batch = texts[start : start + self.batch_size]
                vectors += self._vectors(self._post(client, batch))
        if len(vectors) != len(texts):
            raise ValueError("embedding count does not match input count")
        return vectors

    # The gateway limits tokens per client per minute, so a full corpus can hit 429 and must wait it out.
    def _post(self, client: httpx.Client, batch: list[str]) -> dict:
        for attempt in range(ATTEMPTS):
            response = client.post(self.endpoint, json={"model": self.model, "input": batch})
            if response.status_code not in RETRYABLE or attempt == ATTEMPTS - 1:
                response.raise_for_status()
                return response.json()
            retry_after = response.headers.get("retry-after", "")
            time.sleep(float(retry_after) if retry_after.isdigit() else min(60.0, 2.0 ** (attempt + 2)))
        raise RuntimeError("unreachable")

    def queries(self, texts: list[str]) -> list[list[float]]:
        return self.documents([QUERY_INSTRUCTION + text for text in texts])

    async def query(self, client: httpx.AsyncClient, text: str) -> list[float]:
        response = await client.post(
            self.endpoint, headers=self.headers, json={"model": self.model, "input": [QUERY_INSTRUCTION + text]}
        )
        response.raise_for_status()
        return self._vectors(response.json())[0]
