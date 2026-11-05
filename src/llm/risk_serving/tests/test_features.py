import asyncio
import json
from datetime import timedelta

import httpx
import pytest
from conftest import NOW, feast_response

from fraud_risk_serving.features import features


@pytest.mark.parametrize(
    "age,status",
    [(timedelta(days=32), "expired"), (timedelta(seconds=-1), "future"), (timedelta(hours=25), "expired")],
)
def test_unavailable_views_have_no_values_and_clear_derived_ratios(request_input, age, status):
    response = feast_response(NOW - age)

    async def call():
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(lambda _: httpx.Response(200, json=response))
        ) as client:
            return await features(client, "http://feast", request_input, NOW)

    views = asyncio.run(call())["views"]
    assert views["customer_stream"] == {"status": status, "event_timestamp": (NOW - age).isoformat(), "values": {}}
    if age > timedelta(days=31.5) or age < timedelta(0):
        assert views["customer_daily"]["values"] == {}
        assert views["request_features"]["values"]["customer_amount_to_30d_avg"] is None


def test_missing_rows_are_distinct_from_zero_history(request_input):
    response = feast_response()
    for name, field in zip(response["metadata"]["feature_names"], response["results"]):
        if name.startswith("customer_daily__"):
            field.update(values=[None], statuses=["NOT_FOUND"], event_timestamps=["1970-01-01T00:00:00Z"])

    async def call():
        def upstream(request):
            body = json.loads(request.content)
            assert body["entities"]["payment_method_fingerprint"] == [request_input.payment_method_fingerprint]
            assert body["full_feature_names"] is True
            return httpx.Response(200, json=response)

        async with httpx.AsyncClient(transport=httpx.MockTransport(upstream)) as client:
            return await features(client, "http://feast", request_input, NOW)

    views = asyncio.run(call())["views"]
    assert views["customer_daily"]["status"] == "missing"
    assert views["customer_daily"]["values"] == {}
    assert views["payment_method_daily"]["status"] == "available"
    assert views["request_features"]["values"]["customer_amount_to_30d_avg"] is None


def test_malformed_view_is_an_error_not_an_available_partial_row(request_input):
    response = feast_response()
    response["results"][0]["values"] = [-1]

    async def call():
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(lambda _: httpx.Response(200, json=response))
        ) as client:
            return await features(client, "http://feast", request_input, NOW)

    view = asyncio.run(call())["views"]["customer_daily"]
    assert view == {"status": "error", "values": {}}


def test_unreachable_store_returns_no_partial_features(request_input):
    def timeout(request):
        raise httpx.ReadTimeout("private address", request=request)

    async def call():
        async with httpx.AsyncClient(transport=httpx.MockTransport(timeout)) as client:
            return await features(client, "http://feast", request_input, NOW)

    with pytest.raises(ValueError, match="feature store read failed"):
        asyncio.run(call())
