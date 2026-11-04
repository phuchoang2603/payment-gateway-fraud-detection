"""Describe dataset, topic and pipeline identities and their data-flow relationships."""

from dataclasses import dataclass

from openmetadata.warehouse import LAKEHOUSE, MARTS, table_fqn

SERVICE = "argo"
KAFKA_SERVICE = "payment-gateway-kafka"
STORAGE_SERVICE = "platform-object-storage"
HISTORICAL_PREFIX = "datasets/payment-gateway/historical"
PAYMENTS_TOPIC = "payments.events.v1"
LABELS_TOPIC = "fraud.labels.v1"


@dataclass(frozen=True)
class Node:
    """Identify a catalog entity by kind and fully qualified name."""

    kind: str
    fqn: str


@dataclass(frozen=True)
class Edge:
    """Assign an upstream/downstream relationship to its publishing pipeline."""

    source: Node
    target: Node
    pipeline: str


def table(name, database=LAKEHOUSE):
    """Identify an Iceberg or warehouse table in the shared catalog."""
    return Node("table", table_fqn(name, database))


def topic(name):
    """Quote a Kafka topic identity without treating dots as hierarchy separators."""
    return Node("topic", f'{KAFKA_SERVICE}."{name}"')


def dataset_container(version):
    """Name the immutable historical version container."""
    return f"historical-payments-{version}"


def dataset(version):
    """Identify the historical source version as a storage entity."""
    return Node("container", f"{STORAGE_SERVICE}.{dataset_container(version)}")


def pipeline_fqn(name):
    """Scope a pipeline identity to the configured orchestration service."""
    return f"{SERVICE}.{name}"


BRONZE_EVENTS = table("bronze.payment_events")
BRONZE_LABELS = table("bronze.fraud_labels")
BRONZE_REJECTED = table("bronze.rejected_records")
SILVER_EVENTS = table("silver.payment_events")
SILVER_ATTEMPTS = table("silver.attempts")
SILVER_LABELS = table("silver.labels")
SILVER_REJECTED = table("silver.rejected_events")
GOLD_DIM_CUSTOMER = table("gold.dim_customer")
GOLD_FEAT_CUSTOMER = table("gold.feat_customer_daily")
GOLD_FEAT_METHOD = table("gold.feat_payment_method_daily")
GOLD_LABEL_ATTEMPT = table("gold.label_attempt")
MART_FACT = table("fact_payment_attempt", MARTS)
MART_MERCHANT_DAY = table("fact_merchant_day", MARTS)

PIPELINES = {
    "dp1-bronze-historical": "Bronze from the historical source",
    "dp1-bronze-kafka": "Bronze from the payments and labels topics",
    "dp2-silver": "Silver from pinned Bronze snapshots",
    "maintain-lakehouse": "Lakehouse compaction, layout and snapshot expiry",
    "dp3-features": "Gold features, dimension and labels from pinned Silver",
    "dp3-marts": "ClickHouse marts from Silver",
    "fraud-streaming": "Flink short-window snapshots and rejected events",
    "feature-writer": "Stream features in Redis and Iceberg",
    "feast-materialize": "Daily features in Redis with overlap",
    "stream-history": "Deterministic Bronze replay into stream history",
}
DP3 = ("dp3-features", "dp3-marts")


def edges(dataset_version: str) -> list[Edge]:
    """Build the owned pipeline relationships for a historical dataset version."""
    historical = dataset(dataset_version)
    historical_edges = [
        Edge(historical, BRONZE_EVENTS, "dp1-bronze-historical"),
        Edge(historical, BRONZE_LABELS, "dp1-bronze-historical"),
    ]
    kafka_edges = [
        Edge(topic(PAYMENTS_TOPIC), BRONZE_EVENTS, "dp1-bronze-kafka"),
        Edge(topic(PAYMENTS_TOPIC), BRONZE_REJECTED, "dp1-bronze-kafka"),
        Edge(topic(LABELS_TOPIC), BRONZE_LABELS, "dp1-bronze-kafka"),
        Edge(topic(LABELS_TOPIC), BRONZE_REJECTED, "dp1-bronze-kafka"),
    ]
    silver_edges = [
        Edge(BRONZE_EVENTS, SILVER_EVENTS, "dp2-silver"),
        Edge(BRONZE_EVENTS, SILVER_ATTEMPTS, "dp2-silver"),
        Edge(BRONZE_EVENTS, SILVER_REJECTED, "dp2-silver"),
        Edge(BRONZE_LABELS, SILVER_LABELS, "dp2-silver"),
        Edge(BRONZE_LABELS, SILVER_REJECTED, "dp2-silver"),
    ]
    gold_edges = [
        Edge(SILVER_ATTEMPTS, GOLD_DIM_CUSTOMER, "dp3-features"),
        Edge(SILVER_EVENTS, GOLD_DIM_CUSTOMER, "dp3-features"),
        Edge(SILVER_ATTEMPTS, GOLD_FEAT_CUSTOMER, "dp3-features"),
        Edge(SILVER_ATTEMPTS, GOLD_FEAT_METHOD, "dp3-features"),
        Edge(SILVER_ATTEMPTS, GOLD_LABEL_ATTEMPT, "dp3-features"),
        Edge(SILVER_LABELS, GOLD_LABEL_ATTEMPT, "dp3-features"),
        Edge(SILVER_ATTEMPTS, MART_FACT, "dp3-marts"),
        Edge(SILVER_LABELS, MART_FACT, "dp3-marts"),
        Edge(MART_FACT, MART_MERCHANT_DAY, "dp3-marts"),
    ]
    epoch = f"live-{dataset_version}"
    journal = topic(f"fraud.feature-updates.v2.{epoch}")
    streaming = Node("pipeline", pipeline_fqn("fraud-streaming"))
    writer = Node("pipeline", pipeline_fqn("feature-writer"))
    materialize = Node("pipeline", pipeline_fqn("feast-materialize"))
    feature_edges = [
        Edge(topic(PAYMENTS_TOPIC), streaming, "fraud-streaming"),
        Edge(streaming, journal, "fraud-streaming"),
        Edge(streaming, topic(f"fraud.feature-rejects.v1.{epoch}"), "fraud-streaming"),
        Edge(journal, writer, "feature-writer"),
        Edge(GOLD_FEAT_CUSTOMER, materialize, "feast-materialize"),
        Edge(GOLD_FEAT_METHOD, materialize, "feast-materialize"),
    ]
    for scope in ("customer", "payment_method"):
        target = table(f"gold.feat_stream_{scope}")
        feature_edges.extend(
            [
                Edge(writer, target, "feature-writer"),
                Edge(BRONZE_EVENTS, target, "stream-history"),
            ]
        )
    return historical_edges + kafka_edges + silver_edges + gold_edges + feature_edges
