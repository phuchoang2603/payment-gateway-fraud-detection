import pytest

from risk_model.registry import flatten


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (4507818529279769299, "4507818529279769299"),
        (999_999_999, 999_999_999),
        (-1_000_000_000, "-1000000000"),
        (True, True),
        (0.5, 0.5),
        ("v1", "v1"),
        ({"a": 1}, '{"a": 1}'),
    ],
)
def test_flatten_fits_the_registry_property_types(value, expected):
    assert flatten(value) == expected
