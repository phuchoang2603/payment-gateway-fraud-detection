"""Tests for rolling windows, timestamps, the skip guard and data coverage."""

from datetime import date, timedelta
from types import SimpleNamespace

import pyarrow as pa
import pytest
from hypothesis import given
from hypothesis import strategies as st

from risk_model import windows
from risk_model.split import WindowSplitter
from risk_model.windows import (
    covered_end,
    has_rows,
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
    """Rolling windows are contiguous sized and end on the end day."""
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
    """A later end day shifts every window by the same days."""
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
    """The cutoff is midnight after the test window."""
    plan = rolling(date(2026, 10, 20), 4, 1, 1)
    assert [str(plan.train), str(plan.validation), str(plan.test)] == [
        "2026-10-15..2026-10-18",
        "2026-10-19..2026-10-19",
        "2026-10-20..2026-10-20",
    ]
    assert plan.cutoff == "2026-10-21T00:00:00Z"


@pytest.mark.parametrize("sizes", [(0, 1, 1), (1, 0, 1), (1, 1, 0), (-1, 1, 1)])
def test_every_window_needs_a_day(sizes):
    """Every window needs a day."""
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
    """Instants are read as utc."""
    assert instant(text).date() == expected


@pytest.mark.parametrize("text", ["", "garbage", None])
def test_text_that_is_not_a_time_is_not_an_instant(text):
    """Empty, unparseable and missing values are not instants."""
    assert instant(text) is None


def test_the_reference_day_is_the_utc_day_the_run_was_created():
    """The reference day is the UTC day of the creation time, and a non-time is refused."""
    assert reference_day("2026-11-05T09:00:00Z") == date(2026, 11, 5)
    assert reference_day("2026-11-05T23:30:00-05:00") == date(2026, 11, 6)
    with pytest.raises(ValueError, match="creation time"):
        reference_day("")


def test_a_cutoff_is_new_only_when_it_is_later_than_the_registered_one():
    """A cutoff is new only when it is later than the registered one."""
    assert is_newer("2026-10-21T00:00:00Z", None)
    assert is_newer("2026-10-22T00:00:00Z", "2026-10-21T00:00:00Z")
    assert not is_newer("2026-10-21T00:00:00Z", "2026-10-21T00:00:00Z")
    assert not is_newer("2026-10-20T00:00:00Z", "2026-10-21T00:00:00Z")


class Partitioned:
    """A stream table that reports a partition for each given day."""

    def __init__(self, days):
        self.days = days

    @property
    def inspect(self):
        """Returns the table itself, so `inspect.partitions()` works."""
        return self

    def partitions(self):
        """Returns an Arrow table with one `event_day` partition per day."""
        return pa.table({"partition": [{"event_day": day} for day in self.days]})


def test_partition_days_reads_the_day_of_each_partition():
    """Partition days reads the day of each partition."""
    days = [date(2026, 10, 18), date(2026, 10, 20), date(2026, 10, 18)]
    assert partition_days(Partitioned(days)) == {date(2026, 10, 18), date(2026, 10, 20)}
    assert partition_days(Partitioned([])) == set()


def run(first, last):
    """Returns every day from `first` to `last` inclusive."""
    return {first + timedelta(days=offset) for offset in range((last - first).days + 1)}


def test_the_latest_covered_day_skips_a_gap_between_replay_and_live_data():
    """The latest covered day skips a gap between replay and live data."""
    replayed = run(date(2026, 10, 14), date(2026, 10, 20))
    days = replayed | {date(2026, 11, 2)}
    assert latest_covered(days, 6, date(2026, 11, 1), lambda _: True) == date(2026, 10, 20)
    assert latest_covered(days, 6, date(2026, 11, 2), lambda _: True) == date(2026, 10, 20)
    assert latest_covered(days, 1, date(2026, 11, 2), lambda _: True) == date(2026, 11, 2)


def test_each_day_is_checked_for_rows_only_once_across_overlapping_spans():
    """Candidate spans overlap, so a day that was already looked up is not looked up again."""
    asked = []

    def covers(day):
        asked.append(day)
        return day != date(2026, 10, 20)

    days = run(date(2026, 10, 14), date(2026, 10, 20))
    assert latest_covered(days, 3, date(2026, 10, 20), covers) == date(2026, 10, 19)
    assert len(asked) == len(set(asked))


def test_the_latest_covered_day_stops_at_the_cap_and_needs_the_whole_span_before_it():
    """The latest covered day stops at the cap and needs the whole span before it."""
    days = run(date(2026, 10, 14), date(2026, 10, 20))
    assert latest_covered(days, 3, date(2026, 10, 18), lambda _: True) == date(2026, 10, 18)
    assert latest_covered(days, 3, date(2026, 10, 15), lambda _: True) is None
    assert latest_covered(days, 3, date(2026, 10, 16), lambda _: True) == date(2026, 10, 16)
    assert latest_covered(days, 8, date(2026, 10, 20), lambda _: True) is None
    assert latest_covered(days, 7, date(2026, 10, 20), lambda _: True) == date(2026, 10, 20)
    assert latest_covered(days, 3, date(2026, 10, 1), lambda _: True) is None
    assert latest_covered(set(), 1, date(2026, 10, 20), lambda _: True) is None


def test_a_day_without_rows_in_the_epochs_rules_out_every_span_that_contains_it():
    """A day without rows in the epochs rules out every span that contains it."""
    days = run(date(2026, 10, 1), date(2026, 10, 5))
    last_missing, middle_missing = {date(2026, 10, 5)}, {date(2026, 10, 3)}
    cap = date(2026, 10, 5)
    assert latest_covered(days, 2, cap, lambda day: day not in last_missing) == date(2026, 10, 4)
    assert latest_covered(days, 2, cap, lambda day: day not in middle_missing) == cap
    assert latest_covered(days, 3, cap, lambda day: day not in middle_missing) is None


def test_each_day_is_checked_for_rows_at_most_once():
    """Each day is checked for rows at most once."""
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
    """The chosen end day is covered and no later covered day exists."""
    base = date(2026, 1, 1)
    not_after = base + timedelta(days=cap)
    ruled_out = {base + timedelta(days=offset) for offset in bad}

    def covers(end):
        needed = [end - timedelta(days=offset) for offset in range(span)]
        return all(day in days and day not in ruled_out for day in needed)

    chosen = latest_covered(days, span, not_after, lambda day: day not in ruled_out)
    expected = [end for end in days if end <= not_after and covers(end)]
    assert chosen == (max(expected) if expected else None)


class Scan:
    """A stream table whose scan finds a fixed number of rows and records its options."""

    def __init__(self, rows):
        self.rows, self.options = rows, []

    def scan(self, **options):
        """Records the options and returns a result with the fixed number of rows."""
        self.options.append(options)
        return SimpleNamespace(
            to_arrow=lambda: pa.table({"event_timestamp": pa.array(range(self.rows))})
        )


def test_a_day_has_rows_when_a_one_row_scan_of_its_bounds_finds_one():
    """has_rows scans one row between the day's midnights and reports whether any came back."""
    table = Scan(1)
    assert has_rows(table, date(2026, 10, 20), ["history-v18", "live-v18"])
    assert not has_rows(Scan(0), date(2026, 10, 20), ["history-v18"])
    (options,) = table.options
    assert options["limit"] == 1 and options["selected_fields"] == ("event_timestamp",)
    bounds = repr(options["row_filter"])
    assert "2026-10-20T00:00:00+00:00" in bounds and "2026-10-21T00:00:00+00:00" in bounds


class Rows(Partitioned):
    """A stream table with partitions for some days and rows on a subset of them."""

    def __init__(self, days, with_rows=None):
        super().__init__(days)
        self.with_rows = set(days if with_rows is None else with_rows)


def test_the_covered_end_needs_every_stream_table_to_have_rows_on_every_day_of_the_span(
    monkeypatch,
):
    """A day missing rows in one table ends the covered range before it."""
    from fraud_feast.repo import TABLES

    monkeypatch.setenv("FEAST_STREAM_EPOCHS", "history-ci,live-ci")
    monkeypatch.setattr(windows, "has_rows", lambda table, day, epochs: day in table.with_rows)
    days = run(date(2026, 10, 14), date(2026, 10, 20))
    customer = Rows(days)
    method = Rows(days, with_rows=days - {date(2026, 10, 20)})
    catalog = SimpleNamespace(
        load_table=lambda name: {TABLES["customer"]: customer, TABLES["payment_method"]: method}[
            name
        ]
    )
    assert covered_end(catalog, 3, date(2026, 11, 1)) == date(2026, 10, 19)
    assert covered_end(catalog, 3, date(2026, 10, 18)) == date(2026, 10, 18)
    assert covered_end(catalog, 7, date(2026, 11, 1)) is None
    catalog = SimpleNamespace(
        load_table=lambda name: Rows(days) if name == TABLES["customer"] else Rows([])
    )
    assert covered_end(catalog, 1, date(2026, 11, 1)) is None
