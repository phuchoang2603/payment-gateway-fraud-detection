"""Build authenticated catalog clients and resolve canonical entity references."""

import json
import os
import re
import subprocess
import tempfile
from functools import lru_cache
from pathlib import Path


def server():
    """Build the native connection template without embedding credential values."""
    return {
        "hostPort": "${OPENMETADATA_URL}",
        "authProvider": "openmetadata",
        "securityConfig": {"jwtToken": "${OPENMETADATA_TOKEN}"},
    }


@lru_cache(maxsize=1)
def client():
    """Reuse one authenticated OpenMetadata SDK client per process."""
    from metadata.generated.schema.entity.services.connections.metadata.openMetadataConnection import (
        OpenMetadataConnection,
    )
    from metadata.ingestion.ometa.ometa_api import OpenMetadata

    return OpenMetadata(OpenMetadataConnection.model_validate(expand(server())))


def expand(value):
    """Resolve required environment placeholders recursively, failing on missing values."""
    if isinstance(value, dict):
        return {k: expand(v) for k, v in value.items()}
    if isinstance(value, list):
        return [expand(v) for v in value]
    if isinstance(value, str):
        return re.sub(r"\$\{([A-Z_][A-Z0-9_]*)\}", lambda match: os.environ[match[1]], value)
    return value


def native(command, config):
    """Run the native metadata CLI with a private, short-lived configuration file."""
    with tempfile.TemporaryDirectory(prefix=f"metadata-{command}-") as directory:
        path = Path(directory) / "workflow.json"
        path.write_text(json.dumps(expand(config)))
        path.chmod(0o600)
        subprocess.run(
            ["metadata", command, "-c", str(path)],
            cwd=directory,
            check=True,
            timeout=int(os.environ.get("NATIVE_TIMEOUT_SECONDS", "1500")),
        )


def ref(value, kind):
    """Encode an entity identity for a typed OpenMetadata relationship."""
    return {"id": str(value.id.root), "type": kind}
