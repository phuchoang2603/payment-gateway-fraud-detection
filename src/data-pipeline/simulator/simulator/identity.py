import hmac
import ipaddress
from hashlib import sha256
from uuid import UUID, uuid5

IDENTITY_NAMESPACE = UUID("71088b85-53a3-5ed5-bf27-8ea372bf796d")


def entity_id(integration_id: UUID, kind: str, external_id: str) -> str:
    if kind not in ("merchant", "customer"):
        raise ValueError(f"unsupported entity kind: {kind}")
    return str(uuid5(IDENTITY_NAMESPACE, f"{integration_id}:{kind}:{external_id}"))


def fingerprint(synthetic_key: str, kind: str, value: str) -> str:
    if kind == "ip_network":
        address = ipaddress.ip_address(value)
        prefix = 24 if address.version == 4 else 48
        normalized = str(ipaddress.ip_network(f"{address}/{prefix}", strict=False))
    elif kind == "payment_method":
        if not value.startswith("tok_") or value.rsplit("_", maxsplit=1)[-1] not in ("ok", "nsf", "issuer", "invalid"):
            raise ValueError("payment method must be a synthetic token")
        normalized = value[4:].rsplit("_", maxsplit=1)[0]
    elif kind in ("address", "device"):
        normalized = " ".join(value.casefold().split())
    else:
        raise ValueError(f"unsupported fingerprint kind: {kind}")
    return hmac.new(synthetic_key.encode(), normalized.encode(), sha256).hexdigest()
