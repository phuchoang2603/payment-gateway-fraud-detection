import asyncio
import json
import math
import struct

import httpx
import pytest
from conftest import ORDER, inference, model_config

from fraud_risk_serving.model import HEADER, Model


@pytest.mark.parametrize("second_version", ["5", "6"])
def test_version_change_rebuilds_binary_row_or_fails_without_a_score(second_version):
    config_reads, sent = [], []

    def upstream(request):
        if request.url.path.endswith("/config"):
            config_reads.append(True)
            return httpx.Response(
                200,
                json=model_config(
                    ORDER if len(config_reads) == 1 else list(reversed(ORDER)),
                    "v4" if len(config_reads) == 1 else "v5",
                ),
            )
        size = int(request.headers[HEADER])
        header, values = json.loads(request.content[:size]), struct.unpack("<3f", request.content[size:])
        assert header["inputs"][0]["parameters"]["binary_data_size"] == 12
        sent.append(values)
        return httpx.Response(200, **inference(version="5" if len(sent) == 1 else second_version))

    async def call():
        async with httpx.AsyncClient(transport=httpx.MockTransport(upstream)) as client:
            return await Model(client, "http://model").score(
                {
                    "request_features": {"status": "available", "values": {"amount_minor": 12000, "has_device": True}},
                    "customer_daily": {"status": "expired", "values": {"attempt_count_7d": 999}},
                }
            )

    if second_version == "5":
        result, values = asyncio.run(call())
        assert result["model_version"] == "5"
        assert result["score"] == pytest.approx(0.8)
        assert math.isnan(values[ORDER[1]])
    else:
        with pytest.raises(ValueError, match="version changed twice"):
            asyncio.run(call())
    assert len(config_reads) == 2
    assert sent[0][0] == sent[1][2] == 12000
    assert sent[0][2] == sent[1][0] == 1
    assert math.isnan(sent[0][1]) and math.isnan(sent[1][1])


@pytest.mark.parametrize("bad", ["tensor", "probability", "header"])
def test_malformed_inference_never_produces_a_score(bad):
    answer = inference(score=math.nan if bad == "probability" else 0.8)
    if bad == "tensor":
        answer["content"] = answer["content"][:-1]
    if bad == "header":
        answer["headers"][HEADER] = "-1"

    async def call():
        async with httpx.AsyncClient(transport=httpx.MockTransport(lambda _: httpx.Response(200, **answer))) as client:
            return await Model(client, "http://model").infer([1])

    with pytest.raises(ValueError, match="invalid inference response"):
        asyncio.run(call())
