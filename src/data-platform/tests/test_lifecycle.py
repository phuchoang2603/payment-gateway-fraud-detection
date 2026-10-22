from data_platform.lifecycle import reap
from data_platform.storage import Store


def test_termination_without_exit_handler_cleans_only_its_running_applications(tmp_path):
    calls = []

    def api(path, method="GET"):
        calls.append((path, method))
        if "?labelSelector=" in path:
            return {
                "items": [
                    {
                        "metadata": {"name": name, "labels": {"dp1-workflow": workflow}},
                        "status": {"applicationState": {"state": state}},
                    }
                    for name, workflow, state in [
                        ("cancelled", "stop", "RUNNING"),
                        ("active", "live", "RUNNING"),
                        ("done", "stop", "COMPLETED"),
                    ]
                ]
            }
        if path.endswith("/workflows/stop"):
            return {"spec": {"shutdown": "Terminate"}, "status": {"phase": "Running"}}
        if path.endswith("/workflows/live"):
            return {"status": {"phase": "Running"}}
        return {}

    store = Store(str(tmp_path))
    reap(api=api, store=store)
    assert [path.rsplit("/", 1)[1] for path, method in calls if method == "DELETE"] == ["cancelled"]
    assert store.get("diagnostics/stop/cancelled.json")["cleanup_requested"]
