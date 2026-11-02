from datetime import date, timedelta

import pyarrow as pa
import pytest
from hypothesis import given
from hypothesis import strategies as st

from risk_model.split import WindowSplitter
from risk_model.windows import (
    instant,
    is_newer,
    latest_covered,
    partition_days,
    reference_day,
    rolling,
)

DAYS = st.dates(min_value=date(2020, 1, 1), max_value=date(2035, 1, 1))
SIZES = st.integers(min_value=1, max_value=120)


@given(DAYS, SIZES, SIZES, SIZES)
def test_rolling_windows_are_contiguous_sized_and_end_on_the_end_day(end, train, validation, test):
    plan = rolling(end, train, validation, test)
    assert plan.test.last == end
    assert (plan.train.last - plan.train.first).days + 1 == train
    assert (plan.validation.last - plan.validation.first).days + 1 == validation
    assert (plan.test.last - plan.test.first).days + 1 == test
    assert plan.validation.first == plan.train.last + timedelta(days=1)
    assert plan.test.first == plan.validation.last + timedelta(days=1)
    WindowSplitter(plan.train, plan.validation, plan.test)


@given(DAYS, SIZES, SIZES, SIZES)
def test_a_later_end_day_shifts_every_window_by_the_same_days(end, train, validation, test):
    earlier, later = (
        rolling(end, train, validation, test),
        rolling(end + timedelta(days=3), train, validation, test),
    )
    for before, after in (
        (earlier.train, later.train),
        (earlier.validation, later.validation),
        (earlier.test, later.test),
    ):
        assert after.first - before.first == timedelta(days=3)
        assert after.last - before.last == timedelta(days=3)


def test_the_cutoff_is_midnight_after_the_test_window():
    plan = rolling(date(2026, 10, 20), 4, 1, 1)
    assert [str(plan.train), str(plan.validation), str(plan.test)] == [
        "2026-10-15..2026-10-18",
        "2026-10-19..2026-10-19",
        "2026-10-20..2026-10-20",
    ]
    assert plan.cutoff == "2026-10-21T00:00:00Z"


@pytest.mark.parametrize("sizes", [(0, 1, 1), (1, 0, 1), (1, 1, 0), (-1, 1, 1)])
def test_every_window_needs_a_day(sizes):
    with pytest.raises(ValueError, match="at least one day"):
        rolling(date(2026, 10, 20), *sizes)


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("2026-11-02T03:17:00Z", date(2026, 11, 2)),
        ("2026-11-02T03:17:00+00:00", date(2026, 11, 2)),
        ("2026-11-02T23:30:00-05:00", date(2026, 11, 3)),
        ("2026-11-02T03:17:00", date(2026, 11, 2)),
    ],
)
def test_instants_are_read_as_utc(text, expected):
    assert instant(text).date() == expected


@pytest.mark.parametrize("text", ["", "garbage", "1970-01-01T00:00:00Z", None])
def test_placeholders_that_are_not_real_times_are_ignored(text):
    assert instant(text) is None


def test_the_reference_day_prefers_the_schedule_time():
    assert reference_day("2026-11-02T03:17:00Z", "2026-11-05T00:00:00Z") == date(2026, 11, 2)
    assert reference_day("1970-01-01T00:00:00Z", "2026-11-05T09:00:00Z") == date(2026, 11, 5)
    assert reference_day("", "2026-11-05T09:00:00Z") == date(2026, 11, 5)
    with pytest.raises(ValueError, match="neither"):
        reference_day("", "")


def test_a_cutoff_is_new_only_when_it_is_later_than_the_registered_one():
    assert is_newer("2026-10-21T00:00:00Z", None)
    assert is_newer("2026-10-22T00:00:00Z", "2026-10-21T00:00:00Z")
    assert not is_newer("2026-10-21T00:00:00Z", "2026-10-21T00:00:00Z")
    assert not is_newer("2026-10-20T00:00:00Z", "2026-10-21T00:00:00Z")


class Partitioned:
    def __init__(self, days):
        self.days = days

    @property
    def inspect(self):
        return self

    def partitions(self):
        return pa.table({"partition": [{"event_day": day} for day in self.days]})


def test_partition_days_reads_the_day_of_each_partition():
    days = [date(2026, 10, 18), date(2026, 10, 20), date(2026, 10, 18)]
    assert partition_days(Partitioned(days)) == {date(2026, 10, 18), date(2026, 10, 20)}
    assert partition_days(Partitioned([])) == set()


def run(first, last):
    return {first + timedelta(days=offset) for offset in range((last - first).days + 1)}


def test_the_latest_covered_day_skips_a_gap_between_replay_and_live_data():
    replayed = run(date(2026, 10, 14), date(2026, 10, 20))
    days = replayed | {date(2026, 11, 2)}
    assert latest_covered(days, 6, date(2026, 11, 1), lambda _: True) == date(2026, 10, 20)
    assert latest_covered(days, 6, date(2026, 11, 2), lambda _: True) == date(2026, 10, 20)
    assert latest_covered(days, 1, date(2026, 11, 2), lambda _: True) == date(2026, 11, 2)


def test_the_latest_covered_day_stops_at_the_cap_and_needs_the_whole_span_before_it():
    days = run(date(2026, 10, 14), date(2026, 10, 20))
    assert latest_covered(days, 3, date(2026, 10, 18), lambda _: True) == date(2026, 10, 18)
    assert latest_covered(days, 3, date(2026, 10, 15), lambda _: True) is None
    assert latest_covered(days, 3, date(2026, 10, 16), lambda _: True) == date(2026, 10, 16)
    assert latest_covered(days, 8, date(2026, 10, 20), lambda _: True) is None
    assert latest_covered(days, 7, date(2026, 10, 20), lambda _: True) == date(2026, 10, 20)
    assert latest_covered(days, 3, date(2026, 10, 1), lambda _: True) is None
    assert latest_covered(set(), 1, date(2026, 10, 20), lambda _: True) is None


def test_a_day_without_rows_in_the_epochs_rules_out_every_span_that_contains_it():
    days = run(date(2026, 10, 1), date(2026, 10, 5))
    last_missing, middle_missing = {date(2026, 10, 5)}, {date(2026, 10, 3)}
    cap = date(2026, 10, 5)
    assert latest_covered(days, 2, cap, lambda day: day not in last_missing) == date(2026, 10, 4)
    assert latest_covered(days, 2, cap, lambda day: day not in middle_missing) == cap
    assert latest_covered(days, 3, cap, lambda day: day not in middle_missing) is None


def test_each_day_is_checked_for_rows_at_most_once():
    days = run(date(2026, 10, 1), date(2026, 10, 9))
    calls = []

    def has_rows(day):
        calls.append(day)
        return False

    assert latest_covered(days, 3, date(2026, 10, 9), has_rows) is None
    assert len(calls) == len(set(calls))


@given(
    st.sets(st.integers(0, 40).map(lambda n: date(2026, 1, 1) + timedelta(days=n)), max_size=41),
    st.integers(min_value=1, max_value=8),
    st.integers(min_value=0, max_value=45),
    st.sets(st.integers(min_value=0, max_value=40), max_size=5),
)
def test_the_chosen_end_day_is_covered_and_no_later_covered_day_exists(days, span, cap, bad):
    base = date(2026, 1, 1)
    not_after = base + timedelta(days=cap)
    ruled_out = {base + timedelta(days=offset) for offset in bad}

    def covers(end):
        needed = [end - timedelta(days=offset) for offset in range(span)]
        return all(day in days and day not in ruled_out for day in needed)

    chosen = latest_covered(days, span, not_after, lambda day: day not in ruled_out)
    expected = [end for end in days if end <= not_after and covers(end)]
    assert chosen == (max(expected) if expected else None)
