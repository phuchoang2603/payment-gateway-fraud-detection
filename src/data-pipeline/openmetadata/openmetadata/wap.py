"""Write-audit-publish refs on the Iceberg REST catalog.

A pipeline run writes to the branch named by WAP_BRANCH. The checks audit each table at that branch's head, and this
step then fast-forwards main to the heads in one catalog transaction and drops the branch. A failed run leaves main
unchanged and keeps its branch for diagnosis.
"""

import json
import os
import urllib.parse
import urllib.request


class Catalog:
    def __init__(self):
        self.uri = os.environ["ICEBERG_REST_URI"].rstrip("/")
        form = urllib.parse.urlencode(
            {
                "grant_type": "client_credentials",
                "client_id": os.environ["ICEBERG_CLIENT_ID"],
                "client_secret": os.environ["ICEBERG_CLIENT_SECRET"],
                "scope": "PRINCIPAL_ROLE:ALL",
            }
        ).encode()
        self.token = self._send(urllib.request.Request(os.environ["ICEBERG_OAUTH2_URI"], data=form))["access_token"]
        query = urllib.parse.urlencode({"warehouse": os.environ["ICEBERG_WAREHOUSE"]})
        config = self.request("GET", f"{self.uri}/v1/config?{query}")
        prefix = {**config.get("defaults", {}), **config.get("overrides", {})}.get("prefix")
        self.base = f"{self.uri}/v1/{prefix}" if prefix else f"{self.uri}/v1"

    @staticmethod
    def _send(request):
        with urllib.request.urlopen(request, timeout=60) as response:
            body = response.read()
        return json.loads(body) if body else {}

    def request(self, method, url, body=None):
        data = json.dumps(body).encode() if body is not None else None
        request = urllib.request.Request(url, data=data, method=method)
        if getattr(self, "token", None):
            request.add_header("Authorization", f"Bearer {self.token}")
        if data is not None:
            request.add_header("Content-Type", "application/json")
        return self._send(request)

    def metadata(self, table):
        namespace, name = table.split(".")
        return self.request("GET", f"{self.base}/namespaces/{namespace}/tables/{name}")["metadata"]

    def commit(self, changes):
        self.request("POST", f"{self.base}/transactions/commit", {"table-changes": changes})


def ref_snapshot(metadata, ref):
    return ((metadata.get("refs") or {}).get(ref) or {}).get("snapshot-id")


def is_ancestor(metadata, ancestor, head):
    parents = {
        snapshot["snapshot-id"]: snapshot.get("parent-snapshot-id") for snapshot in metadata.get("snapshots", [])
    }
    current = head
    while current is not None:
        if current == ancestor:
            return True
        current = parents.get(current)
    return False


def audited_snapshots(catalog, tables, branch):
    """Each table's snapshot to audit: the run's branch head, or main when this run did not write the table."""
    found = {}
    for table in tables:
        metadata = catalog.metadata(table)
        found[table] = ref_snapshot(metadata, branch) or ref_snapshot(metadata, "main")
    return found


def publish_changes(catalog, tables, branch):
    """The transaction that fast-forwards main to the branch heads, refusing if main moved off the branch's base."""
    changes = []
    for table in tables:
        metadata = catalog.metadata(table)
        head = ref_snapshot(metadata, branch)
        if head is None:
            continue
        main = ref_snapshot(metadata, "main")
        if main is not None and not is_ancestor(metadata, main, head):
            raise RuntimeError(f"{table}: main {main} is not an ancestor of {branch} head {head}; not a fast-forward")
        namespace, name = table.split(".")
        changes.append(
            {
                "identifier": {"namespace": [namespace], "name": name},
                "requirements": [{"type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": main}],
                "updates": [
                    {"action": "set-snapshot-ref", "ref-name": "main", "type": "branch", "snapshot-id": head},
                    {"action": "remove-snapshot-ref", "ref-name": branch},
                ],
            }
        )
    return changes


def main():
    from openmetadata.checks import checks

    branch = os.environ["WAP_BRANCH"]
    tables = sorted({check.table for check in checks(products=[os.environ["CHECK_PRODUCT"]])})
    catalog = Catalog()
    changes = publish_changes(catalog, tables, branch)
    if changes:
        catalog.commit(changes)
    for change in changes:
        table = ".".join(change["identifier"]["namespace"] + [change["identifier"]["name"]])
        print(f"published {table} main -> {change['updates'][0]['snapshot-id']} from {branch}")
    if not changes:
        print(f"nothing to publish from {branch}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
