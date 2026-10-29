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
