"""Tests for the Triton model repository and the v2 client, with object storage and HTTP replaced by fakes."""

import io
import json
import math
import re
import struct
from types import SimpleNamespace

import pytest

from risk_model import triton

ORDER = ["customer_daily__attempt_count_7d", "request_features__has_device", "customer_stream__x"]


def parameter(text, key):
    """Returns the string value of a config.pbtxt parameter, decoded from its quoted JSON form."""
    pattern = rf'key: "{key}" value: \{{ string_value: ("(?:[^"\\]|\\.)*") \}}'
    return json.loads(re.search(pattern, text).group(1))


def test_a_version_names_its_directory_by_its_number():
    """number turns v7 into 7, with or without the prefix."""
    assert triton.number("v7") == 7 and triton.number("12") == 12


def test_the_config_runs_the_fil_backend_on_the_gpu_and_carries_the_order_and_version():
    """The instance group is KIND_GPU only, the tensors match the feature count and the order is recoverable."""
    text = triton.config_text(ORDER, "v7")
    assert 'name: "payment-risk"' in text and 'backend: "fil"' in text
    assert "KIND_GPU" in text and "KIND_AUTO" not in text and "KIND_CPU" not in text
    assert f"dims: [ {len(ORDER)} ]" in text and "dims: [ 2 ]" in text
    assert f"max_batch_size: {triton.MAX_BATCH}" in text
    assert parameter(text, "model_type") == "xgboost_json"
    assert parameter(text, "is_classifier") == "true" and parameter(text, "predict_proba") == "true"
    assert parameter(text, "threshold") == "0.5"
    assert json.loads(parameter(text, "feature_order")) == ORDER
    assert parameter(text, "registry_version") == "v7"


def test_the_bin_edges_of_the_reference_profile_ride_in_the_config_when_given():
    """The edges are a compact JSON parameter, and absent without a profile."""
    edges = {ORDER[0]: [0.5, 1.5]}
    assert json.loads(parameter(triton.config_text(ORDER, "v7", edges), "bin_edges")) == edges
    assert "bin_edges" not in triton.config_text(ORDER, "v7")
    assert triton.repository_files("v7", ORDER, b"m", edges)[
        "payment-risk/config.pbtxt"
    ].decode() == (triton.config_text(ORDER, "v7", edges))


def test_the_decline_threshold_of_the_version_rides_in_the_config_when_given():
    """The threshold is a parameter the fraud service reads per version, and absent without one."""
    assert (
        float(parameter(triton.config_text(ORDER, "v7", None, 0.512), "decline_threshold")) == 0.512
    )
    assert "decline_threshold" not in triton.config_text(ORDER, "v7")
    files = triton.repository_files("v7", ORDER, b"m", None, 0.25)
    assert (
        float(parameter(files["payment-risk/config.pbtxt"].decode(), "decline_threshold")) == 0.25
    )


def test_the_repository_entry_has_the_config_and_the_model_in_a_numbered_directory():
    """The model sits at payment-risk/<number>/xgboost.json beside payment-risk/config.pbtxt."""
    files = triton.repository_files("v7", ORDER, b'{"model": 1}')
    assert set(files) == {"payment-risk/config.pbtxt", "payment-risk/7/xgboost.json"}
    assert files["payment-risk/7/xgboost.json"] == b'{"model": 1}'
    assert files["payment-risk/config.pbtxt"].decode() == triton.config_text(ORDER, "v7")


def test_packaging_copies_the_model_into_the_repository_layout_of_its_directory(monkeypatch):
    """package reads model.json from the model directory and writes the entry beneath it."""
    puts, reads = [], []

    def get_object(**kwargs):
        reads.append(kwargs)
        return {"Body": io.BytesIO(b"booster")}

    client = SimpleNamespace(get_object=get_object, put_object=lambda **kw: puts.append(kw))
    monkeypatch.setattr(triton, "s3", lambda: client)
    triton.package("s3://bucket/runs/r1/model/", "v7", ORDER)
    assert reads == [{"Bucket": "bucket", "Key": "runs/r1/model/model.json"}]
    assert {p["Key"]: p["Body"] for p in puts if p["Key"].endswith("xgboost.json")} == {
        "runs/r1/model/payment-risk/7/xgboost.json": b"booster"
    }
    assert sorted(p["Key"] for p in puts) == [
        "runs/r1/model/payment-risk/7/xgboost.json",
        "runs/r1/model/payment-risk/config.pbtxt",
    ] and all(p["Bucket"] == "bucket" for p in puts)


class Response(io.BytesIO):
    """A urlopen response with headers, usable as a context manager."""

    def __init__(self, body, headers=None):
        super().__init__(body)
        self.headers = headers or {}


def answer(version="7", probabilities=(0.75, 0.25)):
    """Returns the body and headers of a Triton binary infer answer."""
    header = json.dumps({"model_version": version, "outputs": [{"name": "output__0"}]}).encode()
    return header + struct.pack("<2f", *probabilities), {
        "Inference-Header-Content-Length": str(len(header))
    }


def test_the_feature_order_is_read_from_the_served_models_config(monkeypatch):
    """feature_order asks the model's config endpoint and decodes the order parameter."""
    asked = []
    config = {"parameters": {"feature_order": {"string_value": json.dumps(ORDER)}}}

    def urlopen(url, timeout):
        asked.append((url, timeout))
        return Response(json.dumps(config).encode())

    monkeypatch.setattr(triton.urllib.request, "urlopen", urlopen)
    assert triton.feature_order("http://svc/", timeout=5) == ORDER
    assert asked == [("http://svc/v2/models/payment-risk/config", 5)]


def test_an_inference_sends_one_binary_row_with_nan_for_missing_and_reads_the_fraud_class(
    monkeypatch,
):
    """infer frames the row in the binary extension, keeps NaN intact and returns the second class."""
    sent = []

    def urlopen(request, timeout):
        sent.append((request, timeout))
        return Response(*answer("7", (0.75, 0.25)))

    monkeypatch.setattr(triton.urllib.request, "urlopen", urlopen)
    version, probability = triton.infer("http://svc/", [1.0, math.nan, 3.0], timeout=9)
    assert version == "7" and probability == pytest.approx(0.25)
    ((request, timeout),) = sent
    assert timeout == 9 and request.full_url == "http://svc/v2/models/payment-risk/infer"
    assert request.get_header("Content-type") == "application/octet-stream"
    size = int(request.get_header("Inference-header-content-length"))
    header, body = json.loads(request.data[:size]), request.data[size:]
    assert header["inputs"] == [
        {
            "name": "input__0",
            "shape": [1, 3],
            "datatype": "FP32",
            "parameters": {"binary_data_size": 12},
        }
    ]
    assert header["outputs"] == [{"name": "output__0", "parameters": {"binary_data": True}}]
    first, missing, third = struct.unpack("<3f", body)
    assert (first, third) == (1.0, 3.0) and math.isnan(missing)


def test_a_probe_scores_an_all_missing_row_and_accepts_the_expected_version(monkeypatch):
    """probe reads the order, sends only NaN and passes when version and score are right."""
    rows = []
    monkeypatch.setattr(triton, "feature_order", lambda address: ORDER)
    monkeypatch.setattr(triton, "infer", lambda address, values: rows.append(values) or ("7", 0.5))
    triton.probe("http://svc", "v7")
    assert len(rows[0]) == len(ORDER) and all(math.isnan(v) for v in rows[0])


@pytest.mark.parametrize(
    "served, probability", [("6", 0.5), ("7", 1.01), ("7", -0.01), ("17", 0.5)]
)
def test_a_probe_fails_for_another_version_or_an_impossible_score(monkeypatch, served, probability):
    """A different version, or a probability outside [0, 1], raises RuntimeError naming the version wanted."""
    monkeypatch.setattr(triton, "feature_order", lambda address: ORDER)
    monkeypatch.setattr(triton, "infer", lambda address, values: (served, probability))
    with pytest.raises(RuntimeError, match="instead of a v7 score"):
        triton.probe("http://svc", "v7")


@pytest.mark.parametrize("probability", [0.0, 1.0])
def test_a_probe_accepts_the_score_boundaries(monkeypatch, probability):
    """Zero and one are valid probabilities."""
    monkeypatch.setattr(triton, "feature_order", lambda address: ORDER)
    monkeypatch.setattr(triton, "infer", lambda address, values: ("7", probability))
    triton.probe("http://svc", "v7")
