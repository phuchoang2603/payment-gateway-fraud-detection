from datetime import UTC, datetime, timedelta

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from fraud_feast.repo import definitions
from fraud_feast.request import REQUEST_FEATURES, request_features_view

AT = datetime(2026, 8, 1, 12, tzinfo=UTC)


@pytest.fixture(scope="module")
def transform(monkeypatch_module):
    monkeypatch_module.setenv("FEAST_STREAM_EPOCHS", "history-ci,live-ci")
    view = next(item for item in definitions() if getattr(item, "name", "") == "request_features")
    return view.feature_transformation.udf


@pytest.fixture(scope="module")
def monkeypatch_module():
    with pytest.MonkeyPatch.context() as patch:
        yield patch


def inputs(**overrides):
    values = {
        "scored_at": AT,
        "amount_minor": 1200,
        "customer_account_created_at": AT - timedelta(days=10),
        "merchant_account_created_at": AT - timedelta(days=20),
        "shipping_country": "US",
        "ip_country": "US",
        "item_count": 2,
        "has_device": True,
        "customer_daily__amount_sum_minor_30d": 6000,
        "customer_daily__attempt_count_30d": 10,
        "payment_method_daily__amount_sum_minor_30d": 2400,
        "payment_method_daily__attempt_count_30d": 2,
    }
    values.update(overrides)
    return {name: [value] for name, value in values.items()}


def one(transform, **overrides):
    result = transform(inputs(**overrides))
    assert set(result) == set(REQUEST_FEATURES)
    return {name: values[0] for name, values in result.items()}


# Equivalence partitions of the country pair: both present and equal, both present and different, either absent.
@pytest.mark.parametrize(
    ("ip", "shipping", "expected"),
    [
        ("US", "US", False),
        ("CA", "US", True),
        (None, "US", None),
        ("US", None, None),
        (None, None, None),
    ],
)
def test_country_mismatch_partitions(transform, ip, shipping, expected):
    assert (
        one(transform, ip_country=ip, shipping_country=shipping)["ip_shipping_country_mismatch"]
        is expected
    )


# Boundary values of account age: created at the scoring instant, one microsecond and one day before, and absent.
@pytest.mark.parametrize(
    ("created", "days"),
    [
        (AT, 0),
        (AT - timedelta(microseconds=1), 0),
        (AT - timedelta(days=1) + timedelta(microseconds=1), 0),
        (AT - timedelta(days=1), 1),
        (AT - timedelta(days=365), 365),
        (None, None),
    ],
)
def test_account_age_boundaries(transform, created, days):
    result = one(
        transform, customer_account_created_at=created, merchant_account_created_at=created
    )
    assert result["customer_account_age_days"] == days
    assert result["merchant_account_age_days"] == days


# Partitions of the 30-day history: usable, empty (zero count or zero sum), unknown (null or NaN from offline nulls).
@pytest.mark.parametrize(
    ("total", "count", "expected"),
    [
        (6000, 10, 2.0),
        (1200, 1, 1.0),
        (0, 0, None),
        (0, 5, None),
        (6000, 0, None),
        (None, 10, None),
        (6000, None, None),
        (float("nan"), 10, None),
        (6000, float("nan"), None),
    ],
)
def test_relative_amount_partitions(transform, total, count, expected):
    result = one(
        transform,
        customer_daily__amount_sum_minor_30d=total,
        customer_daily__attempt_count_30d=count,
    )
    assert result["customer_amount_to_30d_avg"] == expected


# Boundary values of the scoring hour, including an offset timestamp and the ISO string Go sends online.
@pytest.mark.parametrize(
    ("scored", "hour"),
    [
        (datetime(2026, 8, 1, 0, tzinfo=UTC), 0),
        (datetime(2026, 8, 1, 23, 59, 59, 999999, tzinfo=UTC), 23),
        (datetime(2026, 8, 1, 1, tzinfo=UTC).astimezone(UTC), 1),
        ("2026-08-01T05:30:00Z", 5),
        ("2026-08-01T05:30:00+02:00", 3),
    ],
)
def test_scoring_hour_boundaries(transform, scored, hour):
    created = datetime(2026, 7, 1, tzinfo=UTC)
    result = one(
        transform,
        scored_at=scored,
        customer_account_created_at=created,
        merchant_account_created_at=created,
    )
    assert result["scored_hour_utc"] == hour


def test_view_reads_only_the_two_daily_aggregates(monkeypatch):
    monkeypatch.setenv("FEAST_STREAM_EPOCHS", "history-ci,live-ci")
    views = {
        item.name: item for item in definitions() if getattr(item, "name", "").endswith("_daily")
    }
    view = request_features_view(views["customer_daily"], views["payment_method_daily"])
    sources = {
        name: [field.name for field in projection.features]
        for name, projection in view.source_feature_view_projections.items()
    }
    assert sources == {
        "customer_daily": ["amount_sum_minor_30d", "attempt_count_30d"],
        "payment_method_daily": ["amount_sum_minor_30d", "attempt_count_30d"],
    }


AMOUNTS = st.integers(min_value=0, max_value=10**10)
COUNTS = st.integers(min_value=0, max_value=10**6)


# Idempotency: the request view is a pure function, so the same request gives the same features on every call,
# and the relative amount is the amount over the 30-day mean whenever that mean exists.
@settings(max_examples=300, deadline=None)
@given(amount=AMOUNTS, total=AMOUNTS, count=COUNTS, offset=st.integers(-48, 48))
def test_request_features_are_deterministic_and_scale_with_the_mean(
    transform, amount, total, count, offset
):
    scored = AT + timedelta(hours=offset)
    request = dict(
        scored_at=scored,
        amount_minor=amount,
        customer_daily__amount_sum_minor_30d=total,
        customer_daily__attempt_count_30d=count,
    )
    first, second = one(transform, **request), one(transform, **request)
    assert first == second
    ratio = first["customer_amount_to_30d_avg"]
    if total and count:
        assert ratio == pytest.approx(amount / (total / count))
        assert ratio >= 0
    else:
        assert ratio is None
    assert 0 <= first["scored_hour_utc"] <= 23
