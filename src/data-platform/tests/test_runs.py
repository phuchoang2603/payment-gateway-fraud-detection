import pytest

from data_platform.runs import accept, open_run, report_key, save_run
from data_platform.storage import Store


def test_stable_context_manifest_guard_and_acceptance_requires_both_stages(tmp_path):
    store = Store(str(tmp_path))
    contracts = {"payment_events": "one", "fraud_labels": "two"}
    run = open_run(store, "v7", "digest", contracts, "image", "workflow")
    assert open_run(store, "v7", "digest", contracts, "other-image", "retry") == run
    with pytest.raises(ValueError, match="different manifest"):
        open_run(store, "v7", "changed", contracts, "image", "retry")
    with pytest.raises(ValueError, match="both committed"):
        accept(store, run, contracts)
    run["snapshots"] = {"payment_events": 1, "fraud_labels": 2}
    save_run(store, run)
    with pytest.raises(ValueError, match="persisted passing"):
        accept(store, run, contracts)
    store.put(
        report_key(run, contracts),
        {"data_outcome": "passed", "snapshots": run["snapshots"], "manifest_sha256": "digest"},
    )
    with pytest.raises(ValueError, match="governance"):
        accept(store, run, contracts)
    store.put(
        report_key(run, contracts).replace("validation.json", "governance.json"),
        {"outcome": "passed", "readback_verified": True},
    )
    accepted = accept(store, run, contracts)
    store.put(report_key(run, contracts), {"data_outcome": "failed"})
    with pytest.raises(ValueError):
        accept(store, run, contracts)
    assert store.get("accepted/v7.json") == accepted
