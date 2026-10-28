from fraud_batch.common.products import SILVER_PORTS
from fraud_batch.silver.silver_contracts import hashes, product_hash, quality_hash
from fraud_batch.silver.silver_native import cancel_owned_queries, cases, expected
from fraud_batch.silver.silver_runs import IDENTITY_FIELDS, digest


def test_native_rules_pin_all_candidate_and_bronze_snapshots():
    candidate = {
        "candidate_id": "a" * 64,
        "completeness": "complete",
        "contracts": hashes(),
        "product_sha256": product_hash(),
        "quality_sha256": quality_hash(),
        "snapshots": {table: index + 10 for index, table in enumerate(SILVER_PORTS)},
        "transformation_revision": "image@sha256:" + "b" * 64,
        "inputs": [
            {"dataset_version": "old", "snapshots": {"payment_events": 100, "fraud_labels": 101}},
            {"dataset_version": "late", "snapshots": {"payment_events": 200, "fraud_labels": 201}},
        ],
    }
    candidate["candidate_id"] = digest({field: candidate[field] for field in IDENTITY_FIELDS})
    rules = cases(candidate)
    assert {rule.table for rule in rules} == set(SILVER_PORTS)
    accounting = next(rule.sql for rule in rules if rule.name == "source_event_conservation")
    assert "FOR VERSION AS OF 100" in accounting and "FOR VERSION AS OF 200" in accounting
    assert "_source_dataset_version = 'old'" in accounting and "_source_dataset_version = 'late'" in accounting
    assert "_candidate_id =" not in accounting
    prepared = expected(candidate, rules)
    assert prepared["snapshots"] == candidate["snapshots"]
    assert len(prepared["cases"]) == len(rules)
    candidate["completeness"] = "recurring"
    candidate["candidate_id"] = digest({field: candidate[field] for field in IDENTITY_FIELDS})
    recurring = {rule.name: rule.sql for rule in cases(candidate)}
    assert "previous_version + 1" in recurring["payment_lifecycle"]
    assert "WHERE NOT pending_context" in recurring["label_attempt_linkage"]
    assert "WHERE NOT pending_context" not in recurring["label_availability"]


def test_cleanup_only_cancels_queries_owned_by_the_candidate():
    class Cursor:
        def __init__(self):
            self.commands = []

        def execute(self, command):
            self.commands.append(command)

        def fetchall(self):
            return [
                {"User": "metadata", "Id": 1, "Command": "Query", "Info": "/* dp2 candidate abc */ SELECT 1"},
                {"User": "metadata", "Id": 2, "Command": "Query", "Info": "/* dp2 candidate other */ SELECT 1"},
                {"User": "other", "Id": 3, "Command": "Query", "Info": "/* dp2 candidate abc */ SELECT 1"},
                {"User": "metadata", "Id": 4, "Command": "Query", "Info": "/* dp1 run abc */ SELECT 1"},
            ]

    cursor = Cursor()
    cancel_owned_queries(cursor, "metadata", 99, "abc")
    assert cursor.commands == ["SHOW FULL PROCESSLIST", "KILL QUERY 1"]
