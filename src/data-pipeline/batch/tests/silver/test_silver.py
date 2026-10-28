from datetime import UTC, datetime

from hypothesis import HealthCheck, example, given, settings
from hypothesis import strategies as st
from pyspark.sql import functions as F

from fraud_batch.bronze.ingest import source_frames
from fraud_batch.bronze.runs import open_run
from fraud_batch.common.products import AUDIT, hashes
from fraud_batch.common.storage import Store
from fraud_batch.silver.silver import attempt_histories, canonical_events, label_observations, mark_pending_events
from fraud_batch.silver.silver_quality import invalid_recurring_lifecycles


@settings(max_examples=8, deadline=None, suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(st.lists(st.tuples(st.integers(0, 2), st.integers(0, 2), st.integers(0, 2)), min_size=1, max_size=6))
@example([(0, 0, 0), (0, 0, 1), (1, 0, 0)])
@example([(0, 0, 0), (0, 0, 0)])
def test_canonical_events_are_idempotent_under_duplicate_replay(spark, bounded_shuffle, records):
    audit = {name: "source" for name in AUDIT if name != "_source_schema_version"}
    deliveries = spark.createDataFrame(
        [
            {
                "integration_id": str(integration),
                "event_id": str(event),
                "content": content,
                "_source_schema_version": 4,
                **audit,
            }
            for integration, event, content in records
        ]
    )
    first = canonical_events(deliveries).canonical
    replay = canonical_events(deliveries.unionByName(deliveries).orderBy(F.col("content").desc()))
    assert sorted(first.collect()) == sorted(replay.canonical.collect())
    assert replay.provenance.agg(F.sum("deliveries")).first()[0] == 2 * len(records)


def test_canonical_events_preserve_delivery_counts_and_reject_conflicts(spark, bounded_shuffle):
    def delivery(integration, event, content, source, schema=3):
        audit = {name: f"{name}-{source}" for name in AUDIT if name != "_source_schema_version"}
        return {
            "integration_id": integration,
            "event_id": event,
            "payment_id": "payment",
            "content": content,
            "_source_schema_version": schema,
            **audit,
        }

    rows = [
        delivery("a", "same", "original", "file-1"),
        delivery("a", "same", "original", "file-1"),
        delivery("a", "same", "original", "file-2"),
        delivery("a", "conflict", "first", "file-1"),
        delivery("a", "conflict", "second", "file-2"),
        delivery("b", "same", "other-scope", "file-3"),
        delivery("a", "versions", "same", "file-1", 3),
        delivery("a", "versions", "same", "file-2", 4),
    ]
    result = canonical_events(spark.createDataFrame(rows))
    assert {(row.integration_id, row.event_id) for row in result.canonical.collect()} == {("a", "same"), ("b", "same")}
    assert {(row.integration_id, row.event_id) for row in result.conflicts.collect()} == {
        ("a", "conflict"),
        ("a", "versions"),
    }
    totals = result.provenance.groupBy("integration_id", "event_id").sum(
        "deliveries", "representative_deliveries", "identical_duplicate_deliveries", "conflicting_deliveries"
    )
    observed = {
        (row.integration_id, row.event_id): tuple(row[index] for index in range(2, 6)) for row in totals.collect()
    }
    assert observed == {
        ("a", "same"): (3, 1, 2, 0),
        ("a", "conflict"): (2, 0, 0, 2),
        ("a", "versions"): (2, 0, 0, 2),
        ("b", "same"): (1, 1, 0, 0),
    }


def test_distinct_label_reports_remain_independent_and_exact_copies_are_counted(spark, bounded_shuffle):
    audit = {name: f"{name}-source" for name in AUDIT if name != "_source_schema_version"}
    label = {
        "integration_id": "a",
        "payment_id": "payment",
        "attempt_id": "attempt",
        "source": "LABEL_SOURCE_SIMULATION_TRUTH",
        "is_fraud": True,
        "occurred_at": datetime(2025, 7, 1, tzinfo=UTC),
        "label_available_at": datetime(2025, 7, 2, tzinfo=UTC),
        **audit,
    }
    later = {**label, "label_available_at": datetime(2025, 7, 3, tzinfo=UTC)}
    result = label_observations(spark.createDataFrame([label, label, later]))
    assert result.observations.count() == 2
    assert sorted(row.deliveries for row in result.provenance.collect()) == [1, 2]


def test_labels_are_pending_until_their_attempt_context_is_available(spark, dataset, tmp_path, bounded_shuffle):
    root, manifest = dataset
    run = open_run(Store(str(tmp_path / "evidence")), "smoke", "a" * 64, hashes(), "image", "workflow")
    raw = source_frames(spark, str(root), manifest, run)
    events = canonical_events(raw["payment_events"])
    labels = label_observations(raw["fraud_labels"]).observations
    assert not events.conflicts.take(1)
    pending = attempt_histories(events.canonical.limit(0), labels)
    assert pending.linked_labels.where("pending_context = false").count() == 0
    resolved = attempt_histories(events.canonical, labels)
    assert not resolved.invalid.take(1)
    assert not resolved.pending.take(1)
    assert not resolved.linked_labels.where("pending_context = true").take(1)
    assert resolved.linked_labels.count() == labels.count()
    partial = mark_pending_events(canonical_events(raw["payment_events"].where("aggregate_version <= 2")))
    assert not invalid_recurring_lifecycles(partial.canonical).take(1)
    assert partial.provenance.where("pending_context_deliveries > 0").take(1)
    partial_attempts = attempt_histories(partial.canonical, labels)
    assert not partial_attempts.linked_labels.where("pending_context = false").take(1)
