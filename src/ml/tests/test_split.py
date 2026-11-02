"""Tests for the time windows and the splitter that classifies rows by them."""

from datetime import UTC, datetime

import pyarrow as pa
import pytest

from risk_model.split import WindowSplitter

DEFAULT = ("2026-07-29..2026-09-30", "2026-10-01..2026-10-14", "2026-10-15..2026-10-28")


def stamps(*values):
    """Returns an Arrow timestamp column from ISO-formatted strings."""
    return pa.array([datetime.fromisoformat(v) for v in values], pa.timestamp("us", tz="UTC"))


def test_default_windows_cover_every_row_exactly_once():
    """Default windows cover every row exactly once."""
    splitter = WindowSplitter(*DEFAULT)
    counts = splitter.counts(
        stamps(
            "2026-07-29T00:00:00+00:00",
            "2026-09-30T23:59:59+00:00",
            "2026-10-01T00:00:00+00:00",
            "2026-10-28T23:59:59+00:00",
        )
    )
    assert counts == {"train": 2, "validation": 1, "test": 1}
    assert splitter.start.isoformat() == "2026-07-29"
    assert splitter.end.isoformat() == "2026-10-29"


@pytest.mark.parametrize(
    "windows",
    [
        ("2026-07-29..2026-10-01", "2026-10-01..2026-10-14", "2026-10-15..2026-10-28"),
        ("2026-07-29..2026-09-30", "2026-10-02..2026-10-14", "2026-10-15..2026-10-28"),
        ("2026-10-01..2026-10-14", "2026-07-29..2026-09-30", "2026-10-15..2026-10-28"),
    ],
)
def test_overlapping_gapped_or_misordered_windows_are_rejected(windows):
    """Overlapping gapped or misordered windows are rejected."""
    with pytest.raises(ValueError):
        WindowSplitter(*windows)


def test_row_outside_every_window_is_rejected():
    """Row outside every window is rejected."""
    splitter = WindowSplitter(*DEFAULT)
    with pytest.raises(ValueError, match="outside every window"):
        splitter.counts(stamps("2026-07-28T23:59:59+00:00", "2026-08-01T00:00:00+00:00"))
    assert datetime(2026, 7, 29, tzinfo=UTC) == splitter["train"].start
