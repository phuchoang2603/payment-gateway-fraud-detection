import pytest

from openmetadata_jobs import checks, wap

BRANCH = "audit-dp1-bronze-abcde"


def metadata(refs, snapshots):
    return {
        "refs": {name: {"snapshot-id": snapshot, "type": "branch"} for name, snapshot in refs.items()},
        "snapshots": [{"snapshot-id": snapshot, "parent-snapshot-id": parent} for snapshot, parent in snapshots],
    }


class FakeCatalog:
    def __init__(self, tables):
        self.tables = tables

    def metadata(self, table):
        return self.tables[table]


def test_rule_is_pinned_to_the_audited_snapshot():
    sql = "SELECT count(*) FROM lakehouse.`bronze.fraud_labels`\nWHERE source = ''\n"

    assert checks.audited(sql, 42).endswith("WHERE source = ''\nSETTINGS iceberg_snapshot_id = 42")
    assert checks.audited(sql, None) == sql


def test_audit_reads_the_branch_head_or_main_for_tables_the_run_did_not_write():
    catalog = FakeCatalog(
        {
            "bronze.payment_events": metadata({"main": 1, BRANCH: 3}, [(1, None), (2, 1), (3, 2)]),
            "bronze.rejected_records": metadata({"main": 7}, [(7, None)]),
        }
    )

    assert wap.audited_snapshots(catalog, ["bronze.payment_events", "bronze.rejected_records"], BRANCH) == {
        "bronze.payment_events": 3,
        "bronze.rejected_records": 7,
    }


def test_publish_fast_forwards_main_and_drops_the_branch():
    catalog = FakeCatalog(
        {
            "bronze.payment_events": metadata({"main": 1, BRANCH: 3}, [(1, None), (2, 1), (3, 2)]),
            "bronze.rejected_records": metadata({"main": 7}, [(7, None)]),
        }
    )

    [change] = wap.publish_changes(catalog, ["bronze.payment_events", "bronze.rejected_records"], BRANCH)

    assert change["identifier"] == {"namespace": ["bronze"], "name": "payment_events"}
    assert change["requirements"] == [{"type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": 1}]
    assert change["updates"] == [
        {"action": "set-snapshot-ref", "ref-name": "main", "type": "branch", "snapshot-id": 3},
        {"action": "remove-snapshot-ref", "ref-name": BRANCH},
    ]


def test_publish_creates_main_on_a_table_that_had_none():
    catalog = FakeCatalog({"silver.attempts": metadata({BRANCH: 5}, [(5, None)])})

    [change] = wap.publish_changes(catalog, ["silver.attempts"], BRANCH)

    assert change["requirements"] == [{"type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": None}]


def test_publish_refuses_when_main_moved_off_the_branch_base():
    catalog = FakeCatalog({"bronze.payment_events": metadata({"main": 4, BRANCH: 3}, [(1, None), (3, 1), (4, 1)])})

    with pytest.raises(RuntimeError, match="not a fast-forward"):
        wap.publish_changes(catalog, ["bronze.payment_events"], BRANCH)
