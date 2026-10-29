from fraud_streaming.benchmark import run


def test_handled_replay_matches_reference_while_naive_variants_drift():
    report = run(
        seed=7,
        attempts=300,
        hours=26,
        late_share=0.1,
        too_late_share=0.03,
        duplicate_share=0.05,
        conflict_share=0.03,
    )
    injected, variants = report["injected"], report["variants"]
    handled = variants["handled"]
    assert handled["mismatched_cells"] == 0
    assert handled["counters"]["too_late"] == injected["too_late"]
    assert handled["counters"]["duplicates_dropped_event"] == injected["duplicate"]
    assert handled["counters"]["rejected_conflicting_duplicate"] == injected["conflict"]
    assert handled["counters"]["lateness_5m_15m"] == injected["late"]
    assert variants["no_dedup"]["over_count"] > 0
    assert variants["arrival_time"]["mismatched_cells"] > 0


def test_default_benchmark_matches_documented_table():
    report = run()
    assert report["injected"] == {
        "on_time": 1134,
        "late": 58,
        "too_late": 8,
        "duplicate": 17,
        "conflict": 4,
    }
    assert report["deliveries"] == 1221
    assert {
        name: (result["mismatched_cells"], result["over_count"], result["under_count"])
        for name, result in report["variants"].items()
    } == {
        "handled": (0, 0, 0),
        "no_dedup": (541, 850, 0),
        "arrival_time": (274, 349, 0),
        "baseline": (683, 1199, 0),
    }
    assert all(result["cells"] == 3234 for result in report["variants"].values())
    assert report["variants"]["handled"]["counters"] == {
        "lateness_lt_1m": 1134,
        "lateness_5m_15m": 58,
        "lateness_too_late": 8,
        "too_late": 8,
        "duplicates_dropped_event": 17,
        "rejected_conflicting_duplicate": 4,
    }
