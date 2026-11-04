"""The Triton model repository of a registered version, and the v2 client that checks a served model."""

import json
import math
import struct
import urllib.request
from urllib.parse import urlsplit

from fraud_feast.lake import s3

from risk_model.cluster import MODEL_NAME

INPUT = "input__0"
OUTPUT = "output__0"
MAX_BATCH = 4096


def number(version):
    """Returns the integer of a version name like `v7`, which names its directory in the repository."""
    return int(version.removeprefix("v"))


def config_text(order, version, edges=None, threshold=None):
    """Returns the `config.pbtxt` of the FIL model for an ordered feature list.

    The instance group is `KIND_GPU`, never `KIND_AUTO`, because auto falls back to the CPU. The
    feature order, the registry version, the bin edges of the reference profile and the version's
    decline threshold ride along as parameters, so a caller can read them back from the served model.
    """
    parameters = {
        "model_type": "xgboost_json",
        "is_classifier": "true",
        "predict_proba": "true",
        "threshold": "0.5",
        "xgboost_allow_unknown_field": "true",
        "feature_order": json.dumps(order),
        "registry_version": version,
    }
    if edges is not None:
        parameters["bin_edges"] = json.dumps(edges, separators=(",", ":"))
    if threshold is not None:
        parameters["decline_threshold"] = repr(float(threshold))
    entries = ",\n".join(
        f"  {{ key: {json.dumps(key)} value: {{ string_value: {json.dumps(value)} }} }}"
        for key, value in parameters.items()
    )
    return (
        f'name: "{MODEL_NAME}"\n'
        'backend: "fil"\n'
        f"max_batch_size: {MAX_BATCH}\n"
        f'input [ {{ name: "{INPUT}" data_type: TYPE_FP32 dims: [ {len(order)} ] }} ]\n'
        f'output [ {{ name: "{OUTPUT}" data_type: TYPE_FP32 dims: [ 2 ] }} ]\n'
        "instance_group [ { kind: KIND_GPU count: 1 } ]\n"
        f"parameters [\n{entries}\n]\n"
    )


def repository_files(version, order, model, edges=None, threshold=None):
    """Returns the files of the repository entry, keyed by path below the model directory.

    `model` is the booster's JSON. The version directory is the version's integer, so every answer
    from Triton names the registry version that made it.
    """
    return {
        f"{MODEL_NAME}/config.pbtxt": config_text(order, version, edges, threshold).encode(),
        f"{MODEL_NAME}/{number(version)}/xgboost.json": model,
    }


def package(model_uri, version, order, edges=None, threshold=None):
    """Writes the Triton repository entry of a version into its model directory."""
    target = urlsplit(model_uri)
    prefix = target.path.strip("/")
    source = s3().get_object(Bucket=target.netloc, Key=f"{prefix}/model.json")
    for path, body in repository_files(
        version, order, source["Body"].read(), edges, threshold
    ).items():
        s3().put_object(Bucket=target.netloc, Key=f"{prefix}/{path}", Body=body)


def feature_order(address, timeout=30):
    """Returns the ordered feature list a served model was packaged with."""
    url = f"{address.rstrip('/')}/v2/models/{MODEL_NAME}/config"
    with urllib.request.urlopen(url, timeout=timeout) as response:
        config = json.load(response)
    return json.loads(config["parameters"]["feature_order"]["string_value"])


def infer(address, values, timeout=30):
    """Scores one row and returns the model version and the fraud probability.

    NaN marks a missing feature. It travels in the binary tensor extension, because JSON cannot carry it.
    """
    body = struct.pack(f"<{len(values)}f", *values)
    header = json.dumps(
        {
            "inputs": [
                {
                    "name": INPUT,
                    "shape": [1, len(values)],
                    "datatype": "FP32",
                    "parameters": {"binary_data_size": len(body)},
                }
            ],
            "outputs": [{"name": OUTPUT, "parameters": {"binary_data": True}}],
        }
    ).encode()
    request = urllib.request.Request(
        f"{address.rstrip('/')}/v2/models/{MODEL_NAME}/infer",
        data=header + body,
        headers={
            "Content-Type": "application/octet-stream",
            "Inference-Header-Content-Length": str(len(header)),
        },
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        size = int(response.headers["Inference-Header-Content-Length"])
        content = response.read()
    answer = json.loads(content[:size])
    _, positive = struct.unpack("<2f", content[size : size + 8])
    return answer["model_version"], positive


def probe(address, version):
    """Scores an all-missing row and raises RuntimeError unless the served model is `version` and answers."""
    served, probability = infer(address, [math.nan] * len(feature_order(address)))
    if served != str(number(version)) or not 0.0 <= probability <= 1.0:
        raise RuntimeError(
            f"{MODEL_NAME} answered version {served} with {probability} instead of a {version} score"
        )
