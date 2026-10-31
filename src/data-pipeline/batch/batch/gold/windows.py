"""Exact sliding-window features as change-point rows.

An attempt counts at anchor s for window w when s - w <= attempt_time < s and started_at < s (outcome-gated measures
also need outcome_at < s). So each attempt contributes over an anchor interval [enter, exit): it enters at the first
anchor after its attempt time and start (and outcome) and leaves at the first anchor whose window begins after its
attempt time. A value at an anchor is the running sum of +/- deltas at those interval ends; a distinct count first
merges each value's intervals. The anchor rule decides which instants are anchors: UTC midnights for the daily
tables, or every microsecond for exact change instants. Values at a run's anchors are exact when the run reads every
attempt still in a window at its first anchor, so a run over any date range needs only that much history.
"""

from dataclasses import dataclass
from datetime import datetime, timedelta
from functools import reduce

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

ANCHOR = "event_timestamp"
SINCE = "since"


class DailyAnchors:
    """Anchors at each UTC midnight; windows must be whole days."""

    name = "daily"

    def after(self, instant: Column) -> Column:
        return F.date_add(F.to_date(instant), 1).cast("timestamp")

    def expiry(self, attempt_time: Column, window: timedelta) -> Column:
        if window % timedelta(days=1):
            raise ValueError(f"daily anchors need whole-day windows, not {window}")
        return F.date_add(F.to_date(attempt_time), window.days + 1).cast("timestamp")


class ExactAnchors:
    """Every microsecond is an anchor, so rows fall at the exact instants a value changes."""

    name = "exact"

    def after(self, instant: Column) -> Column:
        return F.timestamp_micros(F.unix_micros(instant) + F.lit(1))

    def expiry(self, attempt_time: Column, window: timedelta) -> Column:
        micros = window // timedelta(microseconds=1)
        return F.timestamp_micros(F.unix_micros(attempt_time) + F.lit(micros + 1))


@dataclass(frozen=True)
class Count:
    """Attempts where the SQL condition `when` holds."""

    name: str
    when: str | None = None
    outcome: bool = False


@dataclass(frozen=True)
class Total:
    """The sum of the SQL expression `value`."""

    name: str
    value: str
    outcome: bool = False


@dataclass(frozen=True)
class Distinct:
    name: str
    column: str


@dataclass(frozen=True)
class Ratio:
    name: str
    numerator: str
    denominator: str


def merge_intervals(frame: DataFrame, partition: list[str], start: str = "start", end: str = "end") -> DataFrame:
    """Union overlapping or touching [start, end) intervals within each partition."""
    ordered = Window.partitionBy(*partition).orderBy(start, end)
    reach = F.max(end).over(ordered.rowsBetween(Window.unboundedPreceding, -1))
    island = F.sum(F.when(reach.isNull() | (F.col(start) > reach), 1).otherwise(0)).over(
        ordered.rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    return (
        frame.withColumn("_island", island)
        .groupBy(*partition, "_island")
        .agg(F.min(start).alias(start), F.max(end).alias(end))
        .drop("_island")
    )


def columns(measures, windows: dict) -> list[str]:
    return [f"{measure.name}_{window}" for window in windows for measure in measures]


def window_states(
    attempts: DataFrame,
    keys: list[str],
    measures: list,
    windows: dict[str, timedelta],
    rule,
    history_start: datetime,
    floor: datetime,
    cutoff: datetime,
) -> DataFrame:
    """Each entity's served values at `floor` and at every later anchor up to `cutoff` where a value may change.

    `attempts` has the keys, attempt_time, started_at, outcome_at and every column the measures read. The values are
    exact when it holds every attempt still in a window at `floor`. A window's columns are null at anchors before
    history_start + window, where coverage is incomplete. `_begun` marks rows at or after the entity's first non-empty
    anchor; it is exact whenever coverage is incomplete, because such a floor lies within one window of the history
    start, so the run read all history.
    """
    additive = [m for m in measures if isinstance(m, (Count, Total))]
    distinct = [m for m in measures if isinstance(m, Distinct)]
    raw = [f"{m.name}_{w}" for w in windows for m in additive + distinct]

    enter = F.greatest(rule.after(F.col("attempt_time")), rule.after(F.col("started_at")))
    gated = F.greatest(enter, rule.after(F.col("outcome_at")))
    base = attempts.withColumns(
        {
            "_enter": enter,
            "_gated": gated,
            **{f"_exit_{w}": rule.expiry(F.col("attempt_time"), length) for w, length in windows.items()},
        }
    )

    def contributes(measure, window):
        start = F.col("_gated" if measure.outcome else "_enter")
        active = start < F.col(f"_exit_{window}")
        if isinstance(measure, Count):
            value = F.lit(1)
            if measure.when is not None:
                active = active & F.coalesce(F.expr(measure.when), F.lit(False))
        else:
            value = F.coalesce(F.expr(measure.value), F.lit(0))
        return F.when(active, value.cast("long")).otherwise(F.lit(0).cast("long"))

    events = []
    for anchor in ["_enter", "_gated", *[f"_exit_{w}" for w in windows]]:
        deltas = {}
        for w in windows:
            for measure in additive:
                value = contributes(measure, w)
                if anchor == f"_exit_{w}":
                    deltas[f"{measure.name}_{w}"] = -value
                elif anchor == ("_gated" if measure.outcome else "_enter"):
                    deltas[f"{measure.name}_{w}"] = value
        events.append(
            F.struct(
                F.col(anchor).alias(ANCHOR),
                *[(deltas.get(name, F.lit(0).cast("long"))).alias(name) for name in raw],
            )
        )
    additive_deltas = base.select(*keys, F.explode(F.array(*events)).alias("_event")).select(*keys, "_event.*")

    distinct_deltas = []
    for measure in distinct:
        for w in windows:
            intervals = base.where(F.col(measure.column).isNotNull()).select(
                *keys,
                F.col(measure.column).alias("_value"),
                F.col("_enter").alias("start"),
                F.col(f"_exit_{w}").alias("end"),
            )
            merged = merge_intervals(intervals.where(F.col("start") < F.col("end")), [*keys, "_value"])
            name = f"{measure.name}_{w}"
            ends = F.array(
                F.struct(F.col("start").alias(ANCHOR), F.lit(1).cast("long").alias(name)),
                F.struct(F.col("end").alias(ANCHOR), F.lit(-1).cast("long").alias(name)),
            )
            distinct_deltas.append(merged.select(*keys, F.explode(ends).alias("_event")).select(*keys, "_event.*"))

    marks = [F.lit(floor), *[F.lit(history_start + length) for length in windows.values()]]
    anchor_rows = base.select(*keys).distinct().select(*keys, F.explode(F.array(*marks)).alias(ANCHOR))

    deltas = reduce(
        lambda left, right: left.unionByName(right, allowMissingColumns=True),
        [additive_deltas, *distinct_deltas, anchor_rows],
    ).fillna(0, subset=raw)
    summed = (
        deltas.where(F.col(ANCHOR).isNotNull()).groupBy(*keys, ANCHOR).agg(*[F.sum(name).alias(name) for name in raw])
    )
    running = Window.partitionBy(*keys).orderBy(ANCHOR).rowsBetween(Window.unboundedPreceding, Window.currentRow)
    state = summed.select(*keys, ANCHOR, *[F.sum(name).over(running).alias(name) for name in raw])
    empty = reduce(lambda left, right: left & right, [F.col(name) == 0 for name in raw])
    state = state.withColumn("_begun", F.max((~empty).cast("int")).over(running) == 1)
    state = state.where((F.col(ANCHOR) >= F.lit(floor)) & (F.col(ANCHOR) <= F.lit(cutoff)))

    served = {}
    for w, length in windows.items():
        covered = F.col(ANCHOR) >= F.lit(history_start + length)
        for measure in measures:
            name = f"{measure.name}_{w}"
            if isinstance(measure, Ratio):
                denominator = F.col(f"{measure.denominator}_{w}")
                value = F.when(denominator > 0, F.try_divide(F.col(f"{measure.numerator}_{w}"), denominator))
            else:
                value = F.col(name)
            served[name] = F.when(covered, value)
    return state.select(*keys, ANCHOR, *[served[name].alias(name) for name in columns(measures, windows)], "_begun")


def changes(states: DataFrame, keys: list[str], output: list[str], floor: datetime) -> DataFrame:
    """The rows after `floor` whose values differ from the entity's previous row; `floor` rows are the baseline. An
    entity has no row until it is first non-empty, and then always a row there."""
    ordered = Window.partitionBy(*keys).orderBy(ANCHOR)
    # to_json compares rows null-safely; a hash would let (null, 1) and (1, null) collide.
    rows = states.withColumns(
        {"_values": F.to_json(F.struct(*output)), "_previous_begun": F.lag("_begun").over(ordered)}
    ).withColumn("_previous", F.lag("_values").over(ordered))
    first = ~F.coalesce(F.col("_previous_begun"), F.lit(False))
    changed = F.col("_begun") & (first | (F.col("_values") != F.col("_previous")))
    return rows.where((F.col(ANCHOR) > F.lit(floor)) & changed).select(*keys, ANCHOR, *output)


def as_of(points: DataFrame, states: DataFrame, keys: list[str], output: list[str], at: str) -> DataFrame:
    """Each row of `points` with the values of its entity's latest state at or before its `at` time."""
    marked = points.withColumns({"_at": F.col(at), "_order": F.lit(1)})
    known = states.select(
        *keys, F.col(ANCHOR).alias("_at"), F.struct(*output).alias("_state"), F.lit(0).alias("_order")
    )
    ordered = (
        Window.partitionBy(*keys).orderBy("_at", "_order").rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    filled = marked.unionByName(known, allowMissingColumns=True).withColumn(
        "_state", F.last("_state", ignorenulls=True).over(ordered)
    )
    return filled.where(F.col("_order") == 1).select(
        *points.columns, *[F.col(f"_state.{name}").alias(name) for name in output]
    )
