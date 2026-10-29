import os

from batch.catalog import client

SERVICE = "starrocks"
EDGES = [
    ("bronze.payment_events", "silver.payment_events"),
    ("bronze.payment_events", "silver.attempts"),
    ("bronze.payment_events", "silver.rejected_events"),
    ("bronze.fraud_labels", "silver.labels"),
    ("bronze.fraud_labels", "silver.rejected_events"),
]


def fqn(table):
    return f"{SERVICE}.{os.environ.get('STARROCKS_CATALOG', 'fraud_lakehouse')}.{table}"


def add_lineage(source, target):
    from metadata.generated.schema.api.lineage.addLineage import AddLineageRequest
    from metadata.generated.schema.entity.data.table import Table
    from metadata.generated.schema.type.entityLineage import EntitiesEdge
    from metadata.generated.schema.type.entityReference import EntityReference

    sdk = client()
    origin = sdk.get_by_name(entity=Table, fqn=fqn(source))
    destination = sdk.get_by_name(entity=Table, fqn=fqn(target))
    sdk.add_lineage(
        AddLineageRequest(
            edge=EntitiesEdge(
                fromEntity=EntityReference(id=origin.id, type="table"),
                toEntity=EntityReference(id=destination.id, type="table"),
            )
        )
    )


def main():
    for source, target in EDGES:
        add_lineage(source, target)
    print(f"published {len(EDGES)} lineage edges")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
