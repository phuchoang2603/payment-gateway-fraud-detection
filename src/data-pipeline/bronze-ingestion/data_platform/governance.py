from hashlib import sha256

import yaml

from data_platform.products import PORTS, PRODUCTS, contract_path
from data_platform.readback import PRODUCT

DOMAIN = "fraud-detection"
SOURCE_PRODUCT = "historical-payments"


def product_document(product):
    return yaml.safe_load((PRODUCTS / product / "product.odps.yaml").read_text())


def contract_body(table, entity_id):
    artifact = contract_path(table)
    original = artifact.read_bytes()
    contract = yaml.safe_load(original)
    provenance = (
        f"ODCS id={contract['id']} version={contract['version']} sha256={sha256(original).hexdigest()} "
        f"artifact=contracts/data-products/{PRODUCT}/{artifact.name}"
    )
    return {
        "name": contract["id"],
        "displayName": contract["name"],
        "description": provenance + "\n\n```yaml\n" + original.decode() + "\n```",
        "entity": {"id": entity_id, "type": "table"},
        "termsOfUse": contract["description"]["usage"],
    }


def requests(run, ids, report_uri):
    """Named OpenMetadata writes for the ordered Argo HTTP steps."""
    bronze = product_document(PRODUCT)
    source = product_document(SOURCE_PRODUCT)
    source_name = "payment-event-landing" if run.get("input_mode") == "landing" else SOURCE_PRODUCT
    manifest = (
        f"{run['source_root']}/streaming/_batches/epoch={run['source_epoch']}/"
        if run.get("input_mode") == "landing"
        else f"{run['source_root'].rstrip('/')}/manifest.json"
    )
    description = (
        f"workflow={run['workflow']} run_id={run['run_id']} manifest_sha256={run['manifest_sha256']} "
        f"snapshots={run['snapshots']} evidence={report_uri}"
    )
    table = lambda name: {"id": ids[name], "type": "table"}  # noqa: E731
    out = {
        "domain": {
            "method": "PUT",
            "path": "/v1/domains",
            "body": {"name": DOMAIN, "domainType": "Aggregate", "description": "Fraud detection data platform"},
        },
        "domain_assets": {
            "method": "PUT",
            "path": f"/v1/domains/{DOMAIN}/assets/add",
            "body": {"assets": [table(name) for name in PORTS]},
        },
        "source_product": {
            "method": "PUT",
            "path": "/v1/dataProducts",
            "body": {
                "name": source_name,
                "displayName": "Payment event landing" if run.get("input_mode") == "landing" else source["name"],
                "description": f"Immutable source completion manifests {manifest} (sha256 {run['manifest_sha256']}).",
                "domains": [DOMAIN],
            },
        },
        "bronze_product": {
            "method": "PUT",
            "path": "/v1/dataProducts",
            "body": {
                "name": PRODUCT,
                "displayName": bronze["name"],
                "description": f"{bronze['description']['purpose']} ODPS {bronze['id']} version {bronze['version']}.",
                "domains": [DOMAIN],
            },
        },
        "product_assets": {
            "method": "PUT",
            "path": f"/v1/dataProducts/{PRODUCT}/assets/add",
            "body": {"assets": [table(name) for name in PORTS]},
        },
    }
    for port, name in (("payment_events", "events_contract"), ("fraud_labels", "labels_contract")):
        body = contract_body(port, ids[port])
        out[name] = {
            "method": "PUT",
            "path": "/v1/dataContracts",
            "body": body,
            "provenance": [{"op": "replace", "path": "/description", "value": body["description"]}],
        }
    out["lineage"] = {
        "method": "PUT",
        "path": "/v1/lineage",
        "body": {
            "edge": {
                "fromEntity": table("payment_events"),
                "toEntity": table("fraud_labels"),
                "lineageDetails": {
                    "description": "Raw payment and label ports; business linkage belongs to silver; " + description,
                    "pipeline": {"id": ids["pipeline"], "type": "pipeline"},
                },
            }
        },
    }
    return out
