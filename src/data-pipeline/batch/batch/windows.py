"""Exact sliding-window features as change-point rows.

An attempt counts at anchor s for window w when s - w <= attempt_time < s and started_at < s (outcome-gated measures
also need outcome_at < s). So each attempt contributes over an anchor interval [enter, exit): it enters at the first
anchor after its attempt time and start (and outcome) and leaves at the first anchor whose window begins after its
attempt time. A value at an anchor is the running sum of +/- deltas at those interval ends; a distinct count first
merges each value's intervals. The anchor rule decides which instants are anchors: UTC midnights for the daily
tables, or every microsecond for exact change instants. Rows are kept only where the served value changes, so the
latest row at or before any anchor is its value.
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
    name: str
    when: Column | None = None
    outcome: bool = False


@dataclass(frozen=True)
class Total:
    name: str
    value: Column
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


def change_points(
    attempts: DataFrame,
    keys: list[str],
    measures: list,
    windows: dict[str, timedelta],
    rule,
    history_start: datetime,
    cutoff: datetime,
    scope: DataFrame | None = None,
    seeds: DataFrame | None = None,
) -> DataFrame:
    """Change-point rows of `measures` per entity `keys` at the anchors of `rule`, up to `cutoff`.

    `attempts` has the keys, attempt_time, started_at, outcome_at and every column the measures read. A window's
    columns are null at anchors before history_start + window, where coverage is incomplete.

    With `scope` (keys and `since`), only anchors at or after each entity's since are computed, from every attempt
    still in a window there; `seeds` (keys, event_timestamp and the output columns) holds each entity's last row
    before since, so a recomputed row that repeats it is dropped.
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
    if scope is not None:
        base = base.join(scope, keys).withColumns(
            {"_enter": F.greatest("_enter", SINCE), "_gated": F.greatest("_gated", SINCE)}
        )

    def contributes(measure, window):
        start = F.col("_gated" if measure.outcome else "_enter")
        active = start < F.col(f"_exit_{window}")
        if isinstance(measure, Count):
            value = F.lit(1)
            if measure.when is not None:
                active = active & F.coalesce(measure.when, F.lit(False))
        else:
            value = F.coalesce(measure.value, F.lit(0))
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

    entities = base.select(*keys).distinct() if scope is None else scope.select(*keys, SINCE)
    marks = [F.lit(history_start + length) for length in windows.values()]
    if scope is not None:
        marks.append(F.col(SINCE))
    anchor_rows = entities.select(*keys, F.explode(F.array(*marks)).alias(ANCHOR))

    deltas = reduce(
        lambda left, right: left.unionByName(right, allowMissingColumns=True),
        [additive_deltas, *distinct_deltas, anchor_rows],
    ).fillna(0, subset=raw)
    summed = (
        deltas.where(F.col(ANCHOR).isNotNull()).groupBy(*keys, ANCHOR).agg(*[F.sum(name).alias(name) for name in raw])
    )
    running = Window.partitionBy(*keys).orderBy(ANCHOR).rowsBetween(Window.unboundedPreceding, Window.currentRow)
    state = summed.select(*keys, ANCHOR, *[F.sum(name).over(running).alias(name) for name in raw])
    state = state.where(F.col(ANCHOR) <= F.lit(cutoff))
    if scope is not None:
        state = state.join(scope, keys).where(F.col(ANCHOR) >= F.col(SINCE)).drop(SINCE)

    output = columns(measures, windows)
    served = {}
    for w, length in windows.items():
        covered = F.col(ANCHOR) >= F.lit(history_start + length)
        for measure in measures:
            name = f"{measure.name}_{w}"
            if isinstance(measure, Ratio):
                denominator = F.col(f"{measure.denominator}_{w}")
                value = F.when(denominator > 0, F.col(f"{measure.numerator}_{w}") / denominator)
            else:
                value = F.col(name)
            served[name] = F.when(covered, value)
    empty = reduce(lambda left, right: left & right, [F.col(name) == 0 for name in raw])
    rows = state.select(*keys, ANCHOR, *[served[name].alias(name) for name in output], empty.alias("_empty"))

    rows = rows.withColumn("_seed", F.lit(False))
    if seeds is not None:
        rows = rows.unionByName(
            seeds.select(*keys, ANCHOR, *output).withColumns({"_empty": F.lit(False), "_seed": F.lit(True)})
        )
    ordered = Window.partitionBy(*keys).orderBy(ANCHOR)
    # An entity has no row until its state is first non-empty, so earlier anchors are not a previous row.
    begun = F.max((~F.col("_empty") | F.col("_seed")).cast("int")).over(
        ordered.rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    rows = rows.withColumn("_begun", begun).where(F.col("_begun") == 1)
    # to_json compares rows null-safely; a hash would let (null, 1) and (1, null) collide.
    kept = rows.withColumn("_values", F.to_json(F.struct(*output))).withColumn(
        "_previous", F.lag("_values").over(ordered)
    )
    kept = kept.where(~F.col("_seed") & (F.col("_previous").isNull() | (F.col("_values") != F.col("_previous"))))
    return kept.select(*keys, ANCHOR, *output)
