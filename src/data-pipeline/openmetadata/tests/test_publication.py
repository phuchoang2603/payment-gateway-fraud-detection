import json
from types import SimpleNamespace

import pytest

from openmetadata import checks, wap


class Catalog:
    def __init__(self, metadata):
        self.tables = metadata
        self.commits = []

    def metadata(self, table):
        return self.tables[table]

    def commit(self, changes):
        self.commits.append(changes)


@pytest.fixture
def publication(monkeypatch, tmp_path):
    branch = "audit-dp2-silver-existing"
    metadata = {
        "refs": {"main": {"snapshot-id": 1}, branch: {"snapshot-id": 2}},
        "snapshots": [{"snapshot-id": 1}, {"snapshot-id": 2, "parent-snapshot-id": 1}],
    }
    catalog = Catalog({"silver.attempts": metadata})
    monkeypatch.setattr(wap, "Catalog", lambda: catalog)
    monkeypatch.setattr(checks, "checks", lambda **kwargs: [SimpleNamespace(table=table) for table in catalog.tables])
    for name, value in {
        "CHECK_PRODUCT": "silver-payments",
        "WAP_BRANCH": branch,
        "WAP_REQUIRE_FORCE": "true",
        "WAP_FORCE_PUBLISH": "true",
        "WAP_FORCE_REASON": "Explicit publication request; validation incomplete",
        "WAP_PUBLICATION_REPORT": str(tmp_path / "publication.json"),
    }.items():
        monkeypatch.setenv(name, value)
    return catalog, branch, tmp_path / "publication.json"


def test_unconfirmed_publication_does_not_commit(publication, monkeypatch):
    catalog, _, report = publication
    monkeypatch.setenv("WAP_FORCE_PUBLISH", "false")
    with pytest.raises(ValueError, match="force=true"):
        wap.main()
    assert catalog.commits == []
    assert not report.exists()


def test_missing_audit_table_prevents_partial_publication(publication):
    catalog, _, report = publication
    catalog.tables["silver.labels"] = {"refs": {"main": {"snapshot-id": 1}}}
    with pytest.raises(ValueError, match="silver.labels"):
        wap.main()
    assert catalog.commits == []
    assert not report.exists()


def test_override_cannot_replace_diverged_main(publication):
    catalog, _, report = publication
    metadata = catalog.tables["silver.attempts"]
    metadata["refs"]["main"]["snapshot-id"] = 3
    metadata["snapshots"].append({"snapshot-id": 3, "parent-snapshot-id": 1})
    with pytest.raises(RuntimeError, match="not a fast-forward"):
        wap.main()
    assert catalog.commits == []
    assert not report.exists()


def test_explicit_publication_keeps_compare_and_swap_and_incomplete_validation(publication):
    catalog, branch, report = publication
    assert wap.main() == 0
    assert len(catalog.commits) == 1
    change = catalog.commits[0][0]
    assert change["requirements"] == [{"type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": 1}]
    assert change["updates"] == [
        {"action": "set-snapshot-ref", "ref-name": "main", "type": "branch", "snapshot-id": 2},
        {"action": "remove-snapshot-ref", "ref-name": branch},
    ]
    result = json.loads(report.read_text())
    assert "incomplete" in result["validation"]
    assert result["tables"]["silver.attempts"] == {"previous_main_snapshot": 1, "published_snapshot": 2}
