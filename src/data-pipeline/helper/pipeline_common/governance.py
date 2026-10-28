import json
import os

import yaml

from pipeline_common.catalog import ref
from pipeline_common.products import PRODUCTS

DOMAIN = "fraud-detection"
PRODUCT = "bronze-payments"
REPOSITORY = "https://github.com/phuchoang2603/payment-gateway-fraud-detection"
TYPES = {
    "string": "STRING",
    "binary": "BINARY",
    "long": "BIGINT",
    "int": "INT",
    "double": "DOUBLE",
    "boolean": "BOOLEAN",
    "timestamp": "TIMESTAMP",
}


def product_document(product=PRODUCT):
    return yaml.safe_load((PRODUCTS / product / "product.odps.yaml").read_text())


def columns(properties):
    result = []
    for prop in properties:
        logical = prop["logicalType"]
        dtype = "STRUCT" if logical == "object" else "ARRAY" if logical == "array" else TYPES[prop["physicalType"]]
        col = {"name": prop["name"], "dataType": dtype, "description": prop.get("description", prop["name"])}
        if prop.get("required"):
            col["constraint"] = "NOT_NULL"
        if logical == "object":
            col["children"] = columns(prop["properties"])
        elif logical == "array":
            item = prop["items"]
            if item["logicalType"] == "array":
                raise ValueError("nested array contract mapping is unsupported")
            col["arrayDataType"] = "STRUCT" if item["logicalType"] == "object" else TYPES[item["physicalType"]]
            if item["logicalType"] == "object":
                col["children"] = columns(item["properties"])
        result.append(col)
    return result


def resolve_owners(sdk):
    from metadata.generated.schema.api.teams.createTeam import CreateTeamRequest
    from metadata.generated.schema.entity.teams.user import User

    team = product_document()["team"]
    mapping = json.loads(os.environ.get("OPENMETADATA_OWNER_MAP", "{}"))
    users = []
    for member in team["members"]:
        if member.get("role") == "owner":
            user = sdk.get_by_name(User, mapping.get(member["username"], member["username"]), nullable=False)
            users.append(user.id)
    if not users:
        raise ValueError("product has no resolvable declared owners")
    owner = sdk.create_or_update(CreateTeamRequest(name=team["name"], teamType="Group", users=users))
    return [ref(owner, "team")]
