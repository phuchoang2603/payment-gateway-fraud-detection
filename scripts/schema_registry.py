import json
import os
import re
import time
from hashlib import sha256
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen


def imports(schema):
    tokens = re.sub(
        r'"(?:\\.|[^"\\])*"|//[^\n]*|/\*.*?\*/',
        lambda match: match[0] if match[0].startswith('"') else " ",
        schema,
        flags=re.DOTALL,
    )
    return re.findall(r'\bimport\s+(?:(?:public|weak)\s+)?"([^"]+)"\s*;', tokens)


def bundle(root, subjects):
    root = Path(root).resolve()
    schemas = {}
    visiting = set()

    def visit(name, subject):
        if subject in schemas:
            if schemas[subject]["name"] != name:
                raise ValueError(f"conflicting files for subject {subject}")
            return
        if name in visiting:
            raise ValueError(f"cyclic protobuf import: {name}")
        path = (root / name).resolve()
        if not path.is_relative_to(root):
            raise ValueError("protobuf file escapes contract bundle")
        schema = path.read_text()
        dependencies = imports(schema)
        visiting.add(name)
        for dependency in dependencies:
            visit(dependency, dependency)
        visiting.remove(name)
        schemas[subject] = {"name": name, "schema": schema, "imports": dependencies}

    if not subjects:
        raise ValueError("at least one schema subject is required")
    for subject, name in sorted(subjects.items()):
        visit(name, subject)
    return schemas


class Registry:
    def __init__(self, url, *, attempts=6, delay=5):
        self.url = url.rstrip("/")
        self.attempts = attempts
        self.delay = delay

    def request(self, method, path, body=None):
        request = Request(
            self.url + path,
            data=json.dumps(body).encode() if body is not None else None,
            headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
            method=method,
        )
        for attempt in range(self.attempts):
            try:
                with urlopen(request, timeout=10) as response:
                    return json.load(response)
            except HTTPError as error:
                retry = error.code in (408, 429) or error.code >= 500
                code = error.code
                error.close()
                if not retry:
                    raise RuntimeError(
                        f"registry rejected {method} {path}: HTTP {code}"
                    ) from None
            except (URLError, TimeoutError, ConnectionError):
                pass
            if attempt + 1 < self.attempts:
                time.sleep(self.delay)
        raise RuntimeError(f"registry unavailable for {method} {path}")


def register(registry, schemas, compatibility):
    versions = {}
    for subject, source in schemas.items():
        encoded = quote(subject, safe="")
        body = {
            "schemaType": "PROTOBUF",
            "schema": source["schema"],
            "references": [
                {"name": name, "subject": name, "version": versions[name]}
                for name in source["imports"]
            ],
        }
        registry.request("PUT", f"/config/{encoded}", {"compatibility": compatibility})
        registered = registry.request("POST", f"/subjects/{encoded}/versions", body)
        found = registry.request("POST", f"/subjects/{encoded}", body)
        latest = registry.request("GET", f"/subjects/{encoded}/versions/latest")
        config = registry.request("GET", f"/config/{encoded}")
        if (
            registered["id"] != found["id"]
            or found["id"] != latest["id"]
            or found["version"] != latest["version"]
            or latest["schemaType"] != "PROTOBUF"
            or sorted(latest.get("references", []), key=lambda ref: ref["name"])
            != sorted(body["references"], key=lambda ref: ref["name"])
            or config["compatibilityLevel"] != compatibility
        ):
            raise RuntimeError(
                f"registry readback differs from approved schema: {subject}"
            )
        versions[subject] = found["version"]
        print(
            json.dumps(
                {
                    "subject": subject,
                    "id": found["id"],
                    "version": found["version"],
                    "sha256": sha256(source["schema"].encode()).hexdigest(),
                }
            ),
            flush=True,
        )
    return versions


def main():
    schemas = bundle(
        os.environ.get("SCHEMA_CONTRACTS_DIR", "/app/contracts"),
        json.loads(os.environ["SCHEMA_SUBJECTS"]),
    )
    register(
        Registry(os.environ["SCHEMA_REGISTRY_URL"]),
        schemas,
        os.environ["SCHEMA_COMPATIBILITY"],
    )


if __name__ == "__main__":
    main()
