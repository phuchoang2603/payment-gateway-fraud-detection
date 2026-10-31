from pyspark.sql import functions as F

from batch.bronze.schema import EVENT_COLUMNS, business_schema
from batch.silver import curate as silver
from tests.events import at, created


def hashed_events(spark, rows):
    events = spark.createDataFrame(rows, business_schema(spark, EVENT_COLUMNS))
    return silver.with_event_hash(
        events.withColumns({"_source": F.lit("historical"), "_ingested_at": F.current_timestamp()})
    )


def event_ids(frame):
    return sorted(r.event_id for r in frame.select("event_id").collect())


def as_json_rows(frame):
    return sorted(r[0] for r in frame.select(F.to_json(F.struct(*frame.columns))).collect())


def test_unique_events_pass_through_unchanged(spark):
    hashed = hashed_events(spark, [created("pay-1", "evt-1"), created("pay-2", "evt-2", minutes=1)])

    plan = silver.plan_dedup(hashed, 0)
    distinct, collapsed = silver.narrow_dedup(hashed, plan)

    assert plan["duplicated_event_ids"] == 0
    assert event_ids(distinct) == ["evt-1", "evt-2"]
    assert collapsed.count() == 0


def test_identical_copies_collapse_to_the_earliest_copy(spark):
    hashed = hashed_events(spark, [created("pay-1", "evt-1", recorded=5), created("pay-1", "evt-1", recorded=0)])

    plan = silver.plan_dedup(hashed, silver.dedup_broadcast_threshold())
    distinct, collapsed = silver.narrow_dedup(hashed, plan)

    assert plan["duplicated_event_ids"] == 1
    assert collapsed.first()["hashes"] == 1
    kept = distinct.collect()
    assert [r.event_id for r in kept] == ["evt-1"]
    assert kept[0]["recorded_at"] == at(0).replace(tzinfo=None)


def test_conflicting_copies_leave_the_clean_events_of_other_payments(spark):
    hashed = hashed_events(
        spark,
        [
            created("pay-2", "evt-2", amount=100),
            created("pay-2", "evt-2", amount=999),
            created("pay-2", "evt-3", minutes=1),
        ],
    )

    distinct, collapsed = silver.narrow_dedup(hashed, silver.plan_dedup(hashed, 0))

    assert [r.event_id for r in collapsed.where(F.col("hashes") > 1).collect()] == ["evt-2"]
    assert event_ids(distinct) == ["evt-2", "evt-3"]


def test_both_join_strategies_match_the_full_width_pass(spark):
    hashed = hashed_events(
        spark,
        [
            created("pay-1", "evt-1", recorded=0),
            created("pay-1", "evt-1", recorded=5),
            created("pay-2", "evt-2", amount=100),
            created("pay-2", "evt-2", amount=999),
            created("pay-3", "evt-3", minutes=2),
        ],
    )
    expected = as_json_rows(silver.full_width_dedup(hashed))

    for threshold, strategy in ((0, "shuffle"), (1 << 40, "broadcast")):
        plan = silver.plan_dedup(hashed, threshold)
        distinct, _ = silver.narrow_dedup(hashed, plan)
        assert plan["strategy"] == strategy
        assert as_json_rows(distinct) == expected
