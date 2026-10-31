"""UTC day ranges, the unit every Silver and Gold run reads, overwrites and checks."""

from datetime import date, datetime, time, timedelta, timezone
from functools import reduce

from pyspark.sql import functions as F

DAY = timedelta(days=1)
START_ENV = "DP_START"
END_ENV = "DP_END"
LOOKBACK_ENV = "DP_LOOKBACK_DAYS"
LOOKBACK_DEFAULT_DAYS = 2


def midnight(day: date) -> datetime:
    return datetime.combine(day, time(), tzinfo=timezone.utc)


def utc(micros) -> datetime:
    return datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(microseconds=int(micros))


def every_day(first: date, last: date) -> list[date]:
    return [first + n * DAY for n in range((last - first).days + 1)]


def day_range(environ, today: date) -> tuple[date, date]:
    """DP_START and DP_END, or yesterday and the lookback days before it, which catches late events."""
    end = date.fromisoformat(environ[END_ENV]) if environ.get(END_ENV) else today - DAY
    if environ.get(START_ENV):
        start = date.fromisoformat(environ[START_ENV])
    else:
        start = end - timedelta(days=int(environ.get(LOOKBACK_ENV, LOOKBACK_DEFAULT_DAYS)))
    if start > end:
        raise ValueError(f"start {start} is after end {end}")
    return start, end


def between(column: str, first: datetime, after: datetime):
    return (F.col(column) >= F.lit(first)) & (F.col(column) < F.lit(after))


def day_ranges(days: list[date]) -> list[tuple[date, date]]:
    """Consecutive days merged into [first, after) ranges."""
    ranges = []
    for day in sorted(set(days)):
        if ranges and ranges[-1][1] == day:
            ranges[-1] = (ranges[-1][0], day + DAY)
        else:
            ranges.append((day, day + DAY))
    return ranges


def on_days(column: str, days: list[date]):
    """A filter Iceberg can push down: the timestamp column falls on one of the days."""
    return reduce(
        lambda left, right: left | right,
        [between(column, midnight(first), midnight(after)) for first, after in day_ranges(days)],
        F.lit(False),
    )
