import asyncio
import json
from datetime import timedelta

import httpx
import pytest
from conftest import NOW, feast_response

from fraud_risk_serving.features import VIEW_FIELDS, customer_features, features


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


def customer_response(timestamp=NOW):
    response = feast_response(timestamp)
    keep = [
        i
        for i, n in enumerate(response["metadata"]["feature_names"])
        if n.startswith(("customer_daily__", "customer_stream__"))
    ]
    return {
        "metadata": {"feature_names": [response["metadata"]["feature_names"][i] for i in keep]},
        "results": [response["results"][i] for i in keep],
    }


def call_customer(handler):
    async def call():
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            return await customer_features(client, "http://feast", "merchant", "customer", NOW)

    return asyncio.run(call())


def test_customer_features_ask_feast_for_explicit_refs_keyed_by_integration_and_customer():
    seen = {}

    def upstream(request):
        seen.update(json.loads(request.content))
        return httpx.Response(200, json=customer_response())

    result = call_customer(upstream)
    assert seen["entities"] == {"integration_id": ["merchant"], "customer_id": ["customer"]}
    assert "feature_service" not in seen
    assert set(seen["features"]) == {
        f"{view}:{name}" for view in ("customer_daily", "customer_stream") for name in VIEW_FIELDS[view]
    }
    assert set(result.views) == {"customer_daily", "customer_stream"}
    assert all(view.status == "available" for view in result.views.values())
    assert result.views["customer_stream"].values["customer_snapshot_attempt_count_5m"] == 1


@pytest.mark.parametrize(
    "age,status",
    [
        (timedelta(0), "available"),
        (timedelta(hours=24, minutes=5), "available"),
        (timedelta(hours=24, minutes=5, seconds=1), "expired"),
        (timedelta(seconds=-1), "future"),
    ],
)
def test_customer_stream_staleness_boundary_does_not_expire_the_daily_view(age, status):
    result = call_customer(lambda _: httpx.Response(200, json=customer_response(NOW - age)))
    assert result.views["customer_stream"].status == status
    assert result.views["customer_daily"].status == ("future" if status == "future" else "available")
    if status != "available":
        assert result.views["customer_stream"].values == {}


def test_customer_without_rows_is_missing_not_zero():
    response = customer_response()
    for field in response["results"]:
        field.update(values=[None], statuses=["NOT_FOUND"], event_timestamps=["1970-01-01T00:00:00Z"])
    views = call_customer(lambda _: httpx.Response(200, json=response)).views
    assert {view.status for view in views.values()} == {"missing"}
    assert all(view.values == {} for view in views.values())


@pytest.mark.parametrize("body", [b"not json", b'{"metadata": {"feature_names": ["a"]}, "results": []}'])
def test_customer_malformed_store_response_is_rejected(body):
    with pytest.raises(ValueError, match="malformed response"):
        call_customer(lambda _: httpx.Response(200, content=body))
