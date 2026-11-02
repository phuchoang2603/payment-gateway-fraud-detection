from dataclasses import dataclass
from datetime import UTC, datetime, time, timedelta

from pyiceberg.expressions import And, GreaterThanOrEqual, In, LessThan

from risk_model.split import Window


@dataclass(frozen=True)
class Plan:
    train: Window
    validation: Window
    test: Window

    @property
    def cutoff(self):
        return self.test.end.strftime("%Y-%m-%dT%H:%M:%SZ")


def rolling(end_day, train_days, validation_days, test_days):
    if min(train_days, validation_days, test_days) < 1:
        raise ValueError("every window needs at least one day")
    test = Window(end_day - timedelta(days=test_days - 1), end_day)
    validation_last = test.first - timedelta(days=1)
    validation = Window(validation_last - timedelta(days=validation_days - 1), validation_last)
    train_last = validation.first - timedelta(days=1)
    return Plan(Window(train_last - timedelta(days=train_days - 1), train_last), validation, test)


def instant(text):
    try:
        value = datetime.fromisoformat(str(text).strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC) if value.year > 2000 else None


def reference_day(schedule_time, create_time):
    moment = instant(schedule_time) or instant(create_time)
    if moment is None:
        raise ValueError("neither the schedule time nor the creation time is a timestamp")
    return moment.date()


def has_rows(table, day, epochs):
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


def newest_day(table, epochs):
    days = {
        next(iter(row.values()))
        for row in table.inspect.partitions().column("partition").to_pylist()
    }
    return next((day for day in sorted(days, reverse=True) if has_rows(table, day, epochs)), None)


def newest_stream_day(catalog):
    from fraud_feast.repo import TABLES, epochs

    days = [newest_day(catalog.load_table(name), epochs()) for name in TABLES.values()]
    return None if None in days else min(days)


def end_day(reference, lag_days, newest):
    if newest is None:
        raise ValueError("the stream history tables have no rows for the configured epochs")
    return min(reference - timedelta(days=lag_days), newest)


def is_newer(cutoff, registered):
    return registered is None or instant(cutoff) > instant(registered)
