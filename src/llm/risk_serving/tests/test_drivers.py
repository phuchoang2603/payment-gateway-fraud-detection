import asyncio
import math

import httpx
from conftest import ORDER, reference
from hypothesis import given
from hypothesis import strategies as st

from fraud_risk_serving.drivers import NOTICE, Reference, rank


@given(st.permutations(ORDER))
def test_ranking_is_order_independent_and_extreme_outweighs_middle(order):
    profile = reference()["reference"]
    for entry in profile.values():
        entry.update(edges=[0, 1], share=[0.1, 0.8, 0.1, 0])
    values = dict(zip(ORDER, [0.5, 2, math.nan]))
    ranked = rank(profile, {name: values[name] for name in order})
    assert ranked == rank(profile, values)
    assert next(item["weight"] for item in ranked if item["feature"] == ORDER[1]) > next(
        item["weight"] for item in ranked if item["feature"] == ORDER[0]
    )
    assert math.isclose(sum(item["share"] for item in ranked), 1)


def test_another_versions_reference_returns_no_drivers_and_is_not_cached():
    requests = []

    def upstream(_):
        requests.append(True)
        return httpx.Response(200, json=reference("v3" if len(requests) == 1 else "v4"))

    async def call():
        async with httpx.AsyncClient(transport=httpx.MockTransport(upstream)) as client:
            source = Reference(client, "http://reference")
            first = await source.drivers("4", dict.fromkeys(ORDER, 1.0))
            second = await source.drivers("4", dict.fromkeys(ORDER, 1.0))
            await source.drivers("4", dict.fromkeys(ORDER, 1.0))
            return first, second

    first, second = asyncio.run(call())
    assert first["drivers"] == [] and "another model version" in first["drivers_unavailable_reason"]
    assert second["drivers"] and second["drivers_notice"] == NOTICE
    assert len(requests) == 2


def test_reference_without_gains_keeps_the_score_without_inventing_drivers():
    profile = reference()
    for entry in profile["reference"].values():
        entry["gain"] = 0

    async def call():
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(lambda _: httpx.Response(200, json=profile))
        ) as client:
            return await Reference(client, "http://reference").drivers("4", dict.fromkeys(ORDER, 1.0))

    result = asyncio.run(call())
    assert result["drivers"] == []
    assert result["drivers_unavailable_reason"] == "reference profile has no positive feature gains"
