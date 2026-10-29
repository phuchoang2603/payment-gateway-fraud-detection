from datetime import UTC, datetime, timedelta

from simulator.lifecycle import risk_reasons


def test_geo_mismatch_declines_only_new_accounts_with_material_amounts():
    created = datetime(2026, 7, 1, tzinfo=UTC)

    assert risk_reasons(25_000, created, created + timedelta(days=2), "US", "CA") == ["NEW_ACCOUNT_GEO_MISMATCH"]
    assert risk_reasons(25_000, created, created + timedelta(days=30), "US", "CA") == []
    assert risk_reasons(19_999, created, created + timedelta(days=2), "US", "CA") == []
