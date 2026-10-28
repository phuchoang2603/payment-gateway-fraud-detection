from datetime import UTC, datetime, timedelta, timezone

import pytest
from pydantic import ValidationError

from simulator.arrow import ASSESSMENTS
from simulator.config import SchemaSchedule

BOUNDARY = datetime(2026, 7, 3, tzinfo=UTC)


@pytest.mark.parametrize(
    ("offset_us", "expected"),
    [(-1, 3), (0, 4), (1, 4)],
    ids=["before-cutover", "at-cutover", "after-cutover"],
)
def test_schema_cutover_boundary_values(offset_us, expected):
    schedule = SchemaSchedule(changes=[{"effective_at": BOUNDARY, "version": 4}])
    instant = BOUNDARY + timedelta(microseconds=offset_us)
    assert schedule.version_at(instant) == expected
    assert schedule.version_at(instant.astimezone(timezone(timedelta(hours=-5)))) == expected


@pytest.mark.parametrize(
    "values",
    [
        {"initial_version": 99},
        {"changes": [{"effective_at": BOUNDARY, "version": 99}]},
        {"changes": [{"effective_at": BOUNDARY.replace(tzinfo=None), "version": 4}]},
        {
            "changes": [
                {"effective_at": BOUNDARY, "version": 4},
                {"effective_at": BOUNDARY, "version": 3},
            ]
        },
        {
            "changes": [
                {"effective_at": BOUNDARY, "version": 4},
                {"effective_at": BOUNDARY - timedelta(microseconds=1), "version": 3},
            ]
        },
    ],
    ids=["unknown-initial", "unknown-target", "naive-cutover", "equal-cutovers", "reversed-cutovers"],
)
def test_invalid_schedule_partitions(values):
    with pytest.raises(ValidationError):
        SchemaSchedule(**values)


def test_declared_nonconsecutive_versions_and_multiple_cutovers(monkeypatch):
    monkeypatch.setitem(ASSESSMENTS, 5, ASSESSMENTS[4])
    schedule = SchemaSchedule(
        initial_version=4,
        changes=[
            {"effective_at": BOUNDARY, "version": 3},
            {"effective_at": BOUNDARY + timedelta(days=1), "version": 5},
        ],
    )
    assert schedule.version_at(BOUNDARY - timedelta(microseconds=1)) == 4
    assert schedule.version_at(BOUNDARY) == 3
    assert schedule.version_at(BOUNDARY + timedelta(days=1)) == 5
    assert SchemaSchedule(initial_version=4).version_at(BOUNDARY) == 4
    with pytest.raises(ValueError, match="timezone"):
        schedule.version_at(BOUNDARY.replace(tzinfo=None))
