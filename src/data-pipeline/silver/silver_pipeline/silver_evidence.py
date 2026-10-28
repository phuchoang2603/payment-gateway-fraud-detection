import json
import re
from datetime import datetime
from hashlib import sha256

from pipeline_common.products import SILVER_PORTS

from silver_pipeline.silver_runs import digest

REQUIRED_CAPTURES = (
    "generator-configuration",
    "source-diagnostics",
    "skew",
    "cardinality",
    "schema-evolution",
    "duplicates",
    "spark-baseline",
    "spark-optimized",
    "code",
    "argo",
    "metadata-lineage",
    "metadata-validation",
    "metadata-contracts",
)
IMAGE_SIGNATURES = {"image/png": b"\x89PNG\r\n\x1a\n", "image/jpeg": b"\xff\xd8\xff"}


def valid_image(body, media_type):
    if not body.startswith(IMAGE_SIGNATURES[media_type]):
        return False
    if media_type == "image/png":
        return (
            len(body) >= 45
            and body[12:16] == b"IHDR"
            and int.from_bytes(body[16:20], "big") > 0
            and int.from_bytes(body[20:24], "big") > 0
            and body.endswith(b"\x00\x00\x00\x00IEND\xaeB\x60\x82")
        )
    return len(body) >= 128 and body.endswith(b"\xff\xd9")


def validate_evidence(store, candidate, revision):
    candidate_id = candidate["candidate_id"]
    key = f"dp2/evidence/{candidate_id}.json"
    index = store.get(key)
    transform = candidate.get("transform", {})
    benchmark_ref = candidate.get("benchmark", {})
    if (
        not index
        or index.get("candidate_id") != candidate_id
        or index.get("input_sha256") != digest(candidate["inputs"])
        or index.get("snapshots") != candidate["snapshots"]
        or index.get("image_digest") != candidate["transformation_revision"]
        or index.get("git_revision") != revision
        or index.get("workflow_id") != transform.get("workflow_id")
        or not index.get("workflow_id")
        or index.get("transform_application_id") != transform.get("spark_application_id")
        or not index.get("transform_application_id")
        or index.get("benchmark") != benchmark_ref
    ):
        raise ValueError("coursework evidence index is missing or does not match the candidate")
    benchmark_key = benchmark_ref.get("key", "")
    if not benchmark_key.startswith(f"dp2/benchmarks/{candidate_id}/"):
        raise ValueError("candidate benchmark evidence is missing")
    benchmark = store.get(benchmark_key)
    if (
        not benchmark
        or digest(benchmark) != benchmark_ref.get("sha256")
        or benchmark.get("candidate_id") != candidate_id
        or benchmark.get("inputs") != candidate["inputs"]
        or benchmark.get("transformation_revision") != candidate["transformation_revision"]
        or benchmark.get("spark_application_id") != benchmark_ref.get("spark_application_id")
        or benchmark.get("outcome") != "passed"
        or set(benchmark.get("bidirectional_content_equal", {})) != set(SILVER_PORTS)
        or any(value is not True for value in benchmark["bidirectional_content_equal"].values())
    ):
        raise ValueError("benchmark evidence is missing, stale or unequal")
    external_ref = index.get("external_metrics")
    external = None
    if external_ref:
        external_key, external_hash = external_ref.get("key", ""), external_ref.get("sha256", "")
        if not external_key.startswith(f"dp2/evidence/assets/{candidate_id}/") or not re.fullmatch(
            r"[a-f0-9]{64}", external_hash
        ):
            raise ValueError("external Spark metrics identity is invalid")
        body = store.read(external_key, max_bytes=20 * 1024 * 1024)
        if sha256(body).hexdigest() != external_hash:
            raise ValueError("external Spark metrics bytes differ from their identity")
        external = json.loads(body)
        if external.get("spark_application_id") != benchmark_ref.get("spark_application_id"):
            raise ValueError("external Spark metrics do not match the benchmark application")
    for variant in ("baseline", "partitioned"):
        result = benchmark.get(variant, {})
        ui = result.get("ui_metrics", {})
        stages = (
            ui.get("stages", [])
            if ui.get("available") is True
            else (external or {}).get(variant, {}).get("stages", [])
        )
        selected_stages = set(result.get("stage_ids", []))
        if (
            type(result.get("runtime_seconds")) not in (int, float)
            or result["runtime_seconds"] <= 0
            or not result.get("job_ids")
            or set(result.get("plans", {})) != set(SILVER_PORTS)
            or not selected_stages
            or not any(stage.get("stageId") in selected_stages and stage.get("task_quantiles") for stage in stages)
        ):
            raise ValueError(f"benchmark lacks actual runtime, plan or Spark UI metrics: {variant}")
    captures = index.get("captures")
    if not isinstance(captures, dict) or set(captures) != set(REQUIRED_CAPTURES):
        raise ValueError("coursework evidence requires every declared capture")
    manifests = {item["manifest_sha256"] for item in candidate["inputs"]}
    for name, capture in captures.items():
        if not isinstance(capture, dict):
            raise ValueError(f"invalid coursework capture: {name}")
        path, checksum, media_type = capture.get("key", ""), capture.get("sha256", ""), capture.get("media_type")
        try:
            captured_at = datetime.fromisoformat(capture["captured_at"].replace("Z", "+00:00"))
            if captured_at.utcoffset() is None:
                raise ValueError
        except (KeyError, AttributeError, TypeError, ValueError):
            raise ValueError(f"capture requires a timezone-aware timestamp: {name}") from None
        if (
            not path.startswith(f"dp2/evidence/assets/{candidate_id}/")
            or not re.fullmatch(r"[a-f0-9]{64}", checksum)
            or media_type not in IMAGE_SIGNATURES
            or capture.get("manifest_sha256") not in manifests
            or not isinstance(capture.get("run_id"), str)
            or not capture["run_id"].strip()
            or not isinstance(capture.get("description"), str)
            or not capture["description"].strip()
        ):
            raise ValueError(f"capture is incomplete or not correlated: {name}")
        if name in ("spark-baseline", "spark-optimized") and capture.get("application_id") != benchmark_ref.get(
            "spark_application_id"
        ):
            raise ValueError(f"Spark capture does not match the benchmark application: {name}")
        if name == "argo" and capture["run_id"] != transform["workflow_id"]:
            raise ValueError("Argo capture does not match the transform workflow")
        body = store.read(path, max_bytes=20 * 1024 * 1024)
        if not valid_image(body, media_type) or sha256(body).hexdigest() != checksum:
            raise ValueError(f"capture bytes do not match declared image identity: {name}")
    return {"key": key, "sha256": digest(index), "benchmark": benchmark_ref, "external_metrics": external_ref}
