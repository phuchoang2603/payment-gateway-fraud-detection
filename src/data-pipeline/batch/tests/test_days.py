from datetime import date, timedelta

import pytest
from hypothesis import given
from hypothesis import strategies as st

from batch.days import DAY, day_range, day_ranges, every_day

DAYS = st.dates(date(2024, 1, 1), date(2027, 12, 31))


@given(st.lists(DAYS, max_size=40))
def test_day_ranges_cover_exactly_the_given_days_with_disjoint_non_adjacent_ranges(days):
    ranges = day_ranges(days)

    assert {day for first, after in ranges for day in every_day(first, after - DAY)} == set(days)
    assert all(first < after for first, after in ranges)
    assert all(left[1] < right[0] for left, right in zip(ranges, ranges[1:]))


@given(today=DAYS, lookback=st.integers(0, 60))
def test_the_default_range_ends_yesterday_and_spans_the_lookback(today, lookback):
    start, end = day_range({"DP_LOOKBACK_DAYS": str(lookback)}, today)

    assert end == today - DAY
    assert (end - start).days == lookback


@given(start=DAYS, end=DAYS)
def test_an_explicit_range_is_kept_or_rejected_when_reversed(start, end):
    environ = {"DP_START": start.isoformat(), "DP_END": end.isoformat()}
    if start > end:
        with pytest.raises(ValueError):
            day_range(environ, date(2030, 1, 1))
    else:
        assert day_range(environ, date(2030, 1, 1)) == (start, end)


@given(first=DAYS, length=st.integers(0, 400))
def test_every_day_lists_each_day_of_the_range_once_in_order(first, length):
    days = every_day(first, first + timedelta(days=length))

    assert len(days) == length + 1
    assert all(later - earlier == DAY for earlier, later in zip(days, days[1:]))
