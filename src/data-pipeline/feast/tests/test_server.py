from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi.testclient import TestClient

from fraud_feast import server


def response(names, values):
    from feast.protos.feast.serving.ServingService_pb2 import FieldStatus, GetOnlineFeaturesResponse
    from feast.protos.feast.types.Value_pb2 import Value

    proto = GetOnlineFeaturesResponse()
    proto.metadata.feature_names.val.extend(names)
    for value in values:
        result = proto.results.add()
        result.values.append(Value(int64_val=value) if value is not None else Value())
        result.statuses.append(FieldStatus.PRESENT if value is not None else FieldStatus.NOT_FOUND)
    return SimpleNamespace(proto=proto)


@pytest.fixture
def online_store():
    """A Feast store double: the server's lifecycle hooks are async and its reads are synchronous."""
    store = MagicMock()
    store.config.project = "payment_fraud"
    store.initialize = AsyncMock()
    store.close = AsyncMock()
    store._get_provider.return_value.async_supported.online.read = False
    store.get_feature_service.side_effect = lambda name, **_: SimpleNamespace(
        name=name, feature_view_projections=[]
    )
    store.get_online_features.return_value = response(
        ["integration_id", "request_features__amount_minor"], [None, 1200]
    )
    return store


@pytest.fixture
def client(online_store):
    with TestClient(server.get_app(online_store)) as http:
        yield http


def test_health_is_served(client):
    assert client.get("/health").status_code == 200


# Partitions of the request body: a valid service read, an unknown service, and malformed bodies.
@pytest.mark.parametrize(
    ("body", "status"),
    [
        ({"feature_service": "payment_risk_v1", "entities": {"integration_id": ["i"]}}, 200),
        ({"feature_service": "payment_risk_v1", "entities": {"integration_id": []}}, 200),
        (
            {"features": ["request_features:amount_minor"], "entities": {"integration_id": ["i"]}},
            200,
        ),
        ({"feature_service": "payment_risk_v1"}, 422),
        ({"feature_service": "payment_risk_v1", "entities": "i"}, 422),
    ],
)
def test_online_feature_requests(client, body, status):
    assert client.post("/get-online-features", json=body).status_code == status


def test_unknown_service_is_an_error(client, online_store):
    online_store.get_feature_service.side_effect = KeyError("missing")
    with pytest.raises(KeyError):
        client.post(
            "/get-online-features",
            json={"feature_service": "missing", "entities": {"integration_id": ["i"]}},
        )


def test_response_keeps_values_and_statuses(client, online_store):
    body = client.post(
        "/get-online-features",
        json={
            "feature_service": "payment_risk_v1",
            "entities": {"integration_id": ["i"]},
            "full_feature_names": True,
        },
    ).json()
    assert body["metadata"]["feature_names"] == ["integration_id", "request_features__amount_minor"]
    assert [result["statuses"] for result in body["results"]] == [["NOT_FOUND"], ["PRESENT"]]
    assert body["results"][1]["values"] == [1200]
    _, kwargs = online_store.get_online_features.call_args
    assert kwargs["full_feature_names"] is True
    assert kwargs["entity_rows"] == {"integration_id": ["i"]}


def test_transform_dict_adds_projected_full_names():
    view = SimpleNamespace(
        projection=SimpleNamespace(name_to_use=lambda: "request_features"),
        features=[SimpleNamespace(name="amount_minor"), SimpleNamespace(name="absent")],
    )
    original = server._transform_dict
    server._transform_dict = lambda _, inputs: {"amount_minor": inputs["amount_minor"]}
    try:
        result = server._transform_dict_with_full_names(view, {"amount_minor": [5]})
    finally:
        server._transform_dict = original
    assert result == {"amount_minor": [5], "request_features__amount_minor": [5]}
