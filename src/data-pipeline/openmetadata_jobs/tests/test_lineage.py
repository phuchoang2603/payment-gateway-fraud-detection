from openmetadata_jobs import lineage

VERSION = "v18"


def by_pipeline(name):
    return [edge for edge in lineage.edges(VERSION) if edge.pipeline == name]


def test_every_edge_belongs_to_a_registered_pipeline():
    assert {edge.pipeline for edge in lineage.edges(VERSION)} <= set(lineage.PIPELINES)


def test_historical_dataset_feeds_bronze_from_its_container():
    sources = {(edge.source.kind, edge.source.fqn, edge.target.fqn) for edge in by_pipeline("dp1-bronze-historical")}

    assert sources == {
        ("container", "platform-object-storage.historical-payments-v18", lineage.BRONZE_EVENTS.fqn),
        ("container", "platform-object-storage.historical-payments-v18", lineage.BRONZE_LABELS.fqn),
    }


def test_kafka_topics_feed_bronze_and_quarantine_through_their_own_pipeline():
    edges = by_pipeline("dp1-bronze-kafka")
    topics = {(edge.source.fqn, edge.target.fqn) for edge in edges}

    assert ('payment-gateway-kafka."payments.events.v1"', lineage.BRONZE_EVENTS.fqn) in topics
    assert ('payment-gateway-kafka."fraud.labels.v1"', lineage.BRONZE_LABELS.fqn) in topics
    assert all(edge.source.kind == "topic" for edge in edges)
    assert {edge.target.fqn for edge in edges} == {
        lineage.BRONZE_EVENTS.fqn,
        lineage.BRONZE_LABELS.fqn,
        lineage.BRONZE_REJECTED.fqn,
    }


def test_silver_is_built_only_from_bronze_tables():
    edges = by_pipeline("dp2-silver")

    assert edges
    assert {edge.source.fqn for edge in edges} == {lineage.BRONZE_EVENTS.fqn, lineage.BRONZE_LABELS.fqn}
    assert {edge.target.fqn for edge in edges} == {
        lineage.SILVER_EVENTS.fqn,
        lineage.SILVER_ATTEMPTS.fqn,
        lineage.SILVER_LABELS.fqn,
        lineage.SILVER_REJECTED.fqn,
    }


def test_each_silver_table_has_exactly_one_producing_pipeline():
    producers = {}
    for edge in lineage.edges(VERSION):
        if edge.target.kind == "table" and '"silver.' in edge.target.fqn:
            producers.setdefault(edge.target.fqn, set()).add(edge.pipeline)

    assert producers
    assert all(pipelines == {"dp2-silver"} for pipelines in producers.values())


def test_no_duplicate_or_self_referencing_edges():
    edges = lineage.edges(VERSION)

    assert len(edges) == len(set(edges))
    assert all(edge.source != edge.target for edge in edges)


def test_maintenance_publishes_status_without_lineage():
    assert by_pipeline("maintain-lakehouse") == []
    assert "maintain-lakehouse" in lineage.PIPELINES
