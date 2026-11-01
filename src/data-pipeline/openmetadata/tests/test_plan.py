from openmetadata import plan

RANGE = "2026-06-01/2026-06-30"
CODE = "spark:main-abc"


class FakeCatalog:
    def __init__(self, tables):
        self.tables = tables

    def metadata(self, table):
        return self.tables[table]


def table(snapshot, summary=None):
    return {"refs": {"main": {"snapshot-id": snapshot}}, "snapshots": [{"snapshot-id": snapshot, "summary": summary}]}


def gold_catalog(day_range=RANGE, code=CODE, silver_attempts=11):
    summary = {
        "dp3.range": day_range,
        plan.CODE_PROPERTY: code,
        "silver.attempts.snapshot_id": "11",
        "silver.labels.snapshot_id": "12",
        "silver.payment_events.snapshot_id": "13",
    }
    return FakeCatalog(
        {
            "silver.attempts": table(silver_attempts),
            "silver.labels": table(12),
            "silver.payment_events": table(13),
            **{name: table(100 + n, summary) for n, name in enumerate(plan.GOLD_TABLES)},
        }
    )


def test_a_layer_whose_main_heads_record_the_same_range_inputs_and_code_is_skipped():
    assert plan.unchanged(gold_catalog(), "gold", RANGE, CODE)


def test_new_code_rebuilds_the_layer():
    assert not plan.unchanged(gold_catalog(), "gold", RANGE, "spark:main-def")


def test_a_new_input_snapshot_rebuilds_the_layer():
    assert not plan.unchanged(gold_catalog(silver_attempts=21), "gold", RANGE, CODE)


def test_a_head_from_another_range_rebuilds_the_layer():
    assert not plan.unchanged(gold_catalog(day_range="2026-07-01/2026-07-31"), "gold", RANGE, CODE)


def test_output_without_a_code_version_and_daily_runs_are_rebuilt():
    assert not plan.unchanged(gold_catalog(code=None), "gold", RANGE, CODE)
    assert not plan.unchanged(gold_catalog(), "gold", RANGE, "")
    assert not plan.unchanged(gold_catalog(), "gold", "", CODE)
