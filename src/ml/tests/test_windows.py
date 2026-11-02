from datetime import date, timedelta

import pyarrow as pa
import pytest
from hypothesis import given
from hypothesis import strategies as st

from risk_model import windows
from risk_model.split import WindowSplitter
from risk_model.windows import end_day, instant, is_newer, newest_day, reference_day, rolling

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


def test_the_end_day_is_capped_by_lag_and_by_replayed_history():
    assert end_day(date(2026, 11, 2), 1, date(2026, 12, 1)) == date(2026, 11, 1)
    assert end_day(date(2026, 11, 2), 1, date(2026, 10, 20)) == date(2026, 10, 20)
    assert end_day(date(2026, 11, 2), 0, date(2026, 11, 2)) == date(2026, 11, 2)
    with pytest.raises(ValueError, match="no rows"):
        end_day(date(2026, 11, 2), 1, None)


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


def test_the_newest_day_is_the_latest_partition_with_rows_in_the_epochs(monkeypatch):
    table = Partitioned([date(2026, 10, 18), date(2026, 10, 20), date(2026, 10, 19)])
    seen = []

    def has_rows(_, day, epochs):
        seen.append((day, list(epochs)))
        return day <= date(2026, 10, 19)

    monkeypatch.setattr(windows, "has_rows", has_rows)
    assert newest_day(table, ["history-v18"]) == date(2026, 10, 19)
    assert seen == [
        (date(2026, 10, 20), ["history-v18"]),
        (date(2026, 10, 19), ["history-v18"]),
    ]


def test_there_is_no_newest_day_without_rows_in_the_epochs(monkeypatch):
    monkeypatch.setattr(windows, "has_rows", lambda *_: False)
    assert newest_day(Partitioned([date(2026, 10, 20)]), ["live-v18"]) is None
    assert newest_day(Partitioned([]), ["live-v18"]) is None
