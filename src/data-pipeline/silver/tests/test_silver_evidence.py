import pytest
from pipeline_common.storage import Store

from silver_pipeline.silver_evidence import validate_evidence


def test_missing_real_capture_index_cannot_be_accepted(tmp_path):
    candidate = {
        "candidate_id": "a" * 64,
        "inputs": [{"manifest_sha256": "b" * 64}],
        "snapshots": {"payment_events": 12},
        "transformation_revision": "image@sha256:" + "c" * 64,
        "transform": {"workflow_id": "workflow", "spark_application_id": "application"},
    }
    with pytest.raises(ValueError, match="evidence index is missing"):
        validate_evidence(Store(str(tmp_path)), candidate, "d" * 40)
