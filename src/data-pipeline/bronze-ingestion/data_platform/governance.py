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
    """Explicit OpenMetadata writes for Argo HTTP templates: [{method, path, body}]."""
    bronze = product_document(PRODUCT)
    source = product_document(SOURCE_PRODUCT)
    manifest = f"{run['source_root'].rstrip('/')}/manifest.json"
    description = (
        f"workflow={run['workflow']} run_id={run['run_id']} manifest_sha256={run['manifest_sha256']} "
        f"snapshots={run['snapshots']} evidence={report_uri}"
    )
    table = lambda name: {"id": ids[name], "type": "table"}  # noqa: E731
    out = [
        {
            "method": "PUT",
            "path": "/v1/domains",
            "body": {"name": DOMAIN, "domainType": "Aggregate", "description": "Fraud detection data platform"},
        },
        {
            "method": "PUT",
            "path": f"/v1/domains/{DOMAIN}/assets/add",
            "body": {"assets": [table(name) for name in PORTS]},
        },
        {
            "method": "PUT",
            "path": "/v1/dataProducts",
            "body": {
                "name": SOURCE_PRODUCT,
                "displayName": source["name"],
                "description": f"{source['description']['purpose']} Immutable source manifest {manifest} "
                f"(sha256 {run['manifest_sha256']}).",
                "domains": [DOMAIN],
            },
        },
        {
            "method": "PUT",
            "path": "/v1/dataProducts",
            "body": {
                "name": PRODUCT,
                "displayName": bronze["name"],
                "description": f"{bronze['description']['purpose']} ODPS {bronze['id']} version {bronze['version']}.",
                "domains": [DOMAIN],
            },
        },
        {
            "method": "PUT",
            "path": f"/v1/dataProducts/{PRODUCT}/assets/add",
            "body": {"assets": [table(name) for name in PORTS]},
        },
    ]
    out += [{"method": "PUT", "path": "/v1/dataContracts", "body": contract_body(name, ids[name])} for name in PORTS]
    out.append(
        {
            "method": "PUT",
            "path": "/v1/lineage",
            "body": {
                "edge": {
                    "fromEntity": table("payment_events"),
                    "toEntity": table("fraud_labels"),
                    "lineageDetails": {
                        "description": "label validation depends on payment events; " + description,
                        "pipeline": {"id": ids["pipeline"], "type": "pipeline"},
                    },
                }
            },
        }
    )
    return out
