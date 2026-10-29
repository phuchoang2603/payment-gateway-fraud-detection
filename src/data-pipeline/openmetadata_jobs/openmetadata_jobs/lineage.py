from dataclasses import dataclass

SERVICE = "argo"
KAFKA_SERVICE = "payment-gateway-kafka"
STORAGE_SERVICE = "platform-object-storage"
STARROCKS_SERVICE = "starrocks"
CATALOG = "fraud_lakehouse"
HISTORICAL_PREFIX = "datasets/payment-gateway/historical"
PAYMENTS_TOPIC = "payments.events.v1"
LABELS_TOPIC = "fraud.labels.v1"


@dataclass(frozen=True)
class Node:
    kind: str
    fqn: str


@dataclass(frozen=True)
class Edge:
    source: Node
    target: Node
    pipeline: str


def table(schema, name):
    return Node("table", f"{STARROCKS_SERVICE}.{CATALOG}.{schema}.{name}")


def topic(name):
    return Node("topic", f'{KAFKA_SERVICE}."{name}"')


def dataset(version):
    return Node("container", f'{STORAGE_SERVICE}."{HISTORICAL_PREFIX}/{version}"')


def pipeline_fqn(name):
    return f"{SERVICE}.{name}"


BRONZE_EVENTS = table("bronze", "payment_events")
BRONZE_LABELS = table("bronze", "fraud_labels")
BRONZE_REJECTED = table("bronze", "rejected_records")
SILVER_EVENTS = table("silver", "payment_events")
SILVER_ATTEMPTS = table("silver", "attempts")
SILVER_LABELS = table("silver", "labels")
SILVER_REJECTED = table("silver", "rejected_events")

PIPELINES = {
    "dp1-bronze-historical": "Bronze from the v18 historical source",
    "dp1-bronze-kafka": "Bronze from the payments and labels topics",
    "dp2-silver": "Silver from pinned Bronze snapshots",
    "maintain-lakehouse": "Lakehouse compaction, layout and snapshot expiry",
}


def edges(dataset_version: str) -> list[Edge]:
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
    return historical_edges + kafka_edges + silver_edges
