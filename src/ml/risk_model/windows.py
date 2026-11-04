"""Rolling train, validation and test windows for retraining, and the data coverage they need."""

import functools
from dataclasses import dataclass
from datetime import UTC, datetime, time, timedelta

from pyiceberg.expressions import And, GreaterThanOrEqual, In, LessThan

from risk_model.split import Window


@dataclass(frozen=True)
class Plan:
    """Three contiguous windows, train then validation then test."""

    train: Window
    validation: Window
    test: Window

    @property
    def cutoff(self):
        """Returns midnight after the test window as a UTC timestamp."""
        return self.test.end.strftime("%Y-%m-%dT%H:%M:%SZ")


def rolling(end_day, train_days, validation_days, test_days):
    """Returns the Plan whose test window ends on `end_day`, with the given window lengths in days."""
    if min(train_days, validation_days, test_days) < 1:
        raise ValueError("every window needs at least one day")
    test = Window(end_day - timedelta(days=test_days - 1), end_day)
    validation_last = test.first - timedelta(days=1)
    validation = Window(validation_last - timedelta(days=validation_days - 1), validation_last)
    train_last = validation.first - timedelta(days=1)
    return Plan(Window(train_last - timedelta(days=train_days - 1), train_last), validation, test)


def instant(text):
    """Returns a timestamp as an aware UTC datetime, or None if it is not a real time."""
    try:
        value = datetime.fromisoformat(str(text).strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def reference_day(create_time):
    """Returns the UTC day a run was created."""
    moment = instant(create_time)
    if moment is None:
        raise ValueError("the creation time is not a timestamp")
    return moment.date()


def has_rows(table, day, epochs):
    """Returns whether a stream table has a row in one of the epochs on a UTC day."""
    start = datetime.combine(day, time.min, UTC)
    found = table.scan(
        row_filter=And(
            In("epoch", list(epochs)),
            GreaterThanOrEqual("event_timestamp", start.isoformat()),
            LessThan("event_timestamp", (start + timedelta(days=1)).isoformat()),
        ),
        selected_fields=("event_timestamp",),
        limit=1,
    ).to_arrow()
    return found.num_rows > 0


def partition_days(table):
    """Returns the set of days a stream table has partitions for."""
    return {
        next(iter(row.values()))
        for row in table.inspect.partitions().column("partition").to_pylist()
    }


def latest_covered(days, span, not_after, covers):
    """The latest end day, at or before `not_after`, where each of the `span` days up to it has rows.

    `covers(day)` says whether a day has rows, and is asked at most once per day.
    """
    covers = functools.cache(covers)
    for end in sorted((day for day in days if day <= not_after), reverse=True):
        needed = [end - timedelta(days=offset) for offset in range(span)]
        if all(day in days for day in needed) and all(covers(day) for day in needed):
            return end
    return None


def covered_end(catalog, span, not_after):
    """Returns the latest covered end day for the stream tables of the configured epochs, or None."""
    from fraud_feast.repo import TABLES, epochs

    configured = epochs()
    tables = [catalog.load_table(name) for name in TABLES.values()]
    days = set.intersection(*(partition_days(table) for table in tables))
    return latest_covered(
        days,
        span,
        not_after,
        lambda day: all(has_rows(table, day, configured) for table in tables),
    )


def is_newer(cutoff, registered):
    """Returns whether a cutoff is later than the registered one, which counts as older when missing."""
    return registered is None or instant(cutoff) > instant(registered)
