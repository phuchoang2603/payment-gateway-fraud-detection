"""Time-ordered train, validation and test windows, and the masks that select their rows."""

from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta

import pyarrow as pa
import pyarrow.compute as pc

NAMES = ("train", "validation", "test")


@dataclass(frozen=True)
class Window:
    """A range of UTC days, first and last inclusive."""

    first: date
    last: date

    @classmethod
    def parse(cls, text):
        """Returns the Window for text like `2026-07-29..2026-09-30`, or raises ValueError."""
        first, separator, last = str(text).partition("..")
        if not separator:
            raise ValueError(f"window {text!r} must look like 2026-07-29..2026-09-30")
        window = cls(date.fromisoformat(first), date.fromisoformat(last))
        if window.last < window.first:
            raise ValueError(f"window {text!r} ends before it starts")
        return window

    @property
    def start(self):
        """Returns the first instant of the window."""
        return datetime.combine(self.first, time.min, UTC)

    @property
    def end(self):
        """Returns the first instant after the window."""
        return datetime.combine(self.last + timedelta(days=1), time.min, UTC)

    def __str__(self):
        """Returns the window as text like `2026-07-29..2026-09-30`."""
        return f"{self.first}..{self.last}"


class WindowSplitter:
    """Holds the train, validation and test windows and selects rows by time.

    Each window is a Window or text like `2026-07-29..2026-09-30`. The windows must follow one another
    with no overlap and no gap, or ValueError is raised. `mask` and `counts` classify a timestamp
    column, and `counts` fails on any row outside every window.
    """

    def __init__(self, train, validation, test):
        self.windows = {
            name: value if isinstance(value, Window) else Window.parse(value)
            for name, value in zip(NAMES, (train, validation, test))
        }
        ordered = list(self.windows.items())
        for (earlier, before), (later, after) in zip(ordered, ordered[1:]):
            if after.first <= before.last:
                raise ValueError(f"{later} window overlaps or precedes the {earlier} window")
            if after.first != before.last + timedelta(days=1):
                raise ValueError(f"{later} window must start the day after the {earlier} window")

    def __getitem__(self, name):
        """Returns the window called `train`, `validation` or `test`."""
        return self.windows[name]

    @property
    def start(self):
        """Returns the first day of the train window."""
        return self.windows["train"].first

    @property
    def end(self):
        """Returns the day after the test window."""
        return self.windows["test"].last + timedelta(days=1)

    def mask(self, column, name):
        """Returns a boolean Arrow mask of the rows of a timestamp column inside a window."""
        window = self.windows[name]
        return pc.and_(
            pc.greater_equal(column, pa.scalar(window.start, column.type)),
            pc.less(column, pa.scalar(window.end, column.type)),
        )

    def counts(self, started_at):
        """Returns the row count of each window, and raises ValueError for rows outside all of them."""
        column = pa.chunked_array([started_at]) if isinstance(started_at, pa.Array) else started_at
        counts = {}
        for name in self.windows:
            inside = self.mask(column, name)
            counts[name] = pc.sum(inside.cast(pa.int64())).as_py() or 0
        outside = len(column) - column.null_count - sum(counts.values())
        if column.null_count or outside:
            raise ValueError(
                f"{outside + column.null_count} rows fall outside every window "
                f"{', '.join(f'{name} {window}' for name, window in self.windows.items())}"
            )
        return counts
