import asyncio
import json
import math
import re
import struct
from dataclasses import dataclass

from fraud_risk_serving.upstream import read

HEADER = "Inference-Header-Content-Length"


@dataclass(frozen=True)
class Layout:
    version: str
    order: tuple[str, ...]
    threshold: float


def row(views, order):
    result = []
    for name in order:
        view, field = name.split("__", 1)
        observed = views.get(view, {})
        value = observed.get("values", {}).get(field) if observed.get("status") == "available" else None
        result.append(float(value) if type(value) in (int, float, bool) and math.isfinite(value) else math.nan)
    return result


class Model:
    def __init__(self, client, url):
        self.client, self.url = client, url.rstrip("/") + "/v2/models/payment-risk"
        self.layout = None
        self.lock = asyncio.Lock()

    async def config(self):
        content, _ = await read(self.client, "model server", "GET", self.url + "/config", 3)
        try:
            parameters = json.loads(content)["parameters"]
            order = json.loads(parameters["feature_order"]["string_value"])
            version = parameters["registry_version"]["string_value"]
            threshold = float(parameters["decline_threshold"]["string_value"])
            if (
                not isinstance(order, list)
                or not order
                or len(order) != len(set(order))
                or any(
                    not isinstance(name, str) or not re.fullmatch(r"[a-z0-9_]+__[a-z0-9_]+", name) for name in order
                )
                or not re.fullmatch(r"v[0-9]+", version)
                or not math.isfinite(threshold)
                or not 0 < threshold <= 1
            ):
                raise ValueError("invalid configuration")
            return Layout(version, tuple(order), threshold)
        except (KeyError, TypeError, ValueError) as error:
            raise ValueError("model server returned an invalid configuration") from error

    async def infer(self, values):
        tensor = struct.pack(f"<{len(values)}f", *values)
        header = json.dumps(
            {
                "inputs": [
                    {
                        "name": "input__0",
                        "shape": [1, len(values)],
                        "datatype": "FP32",
                        "parameters": {"binary_data_size": len(tensor)},
                    }
                ],
                "outputs": [{"name": "output__0", "parameters": {"binary_data": True}}],
            }
        ).encode()
        content, headers = await read(
            self.client,
            "model server",
            "POST",
            self.url + "/infer",
            3,
            content=header + tensor,
            headers={HEADER: str(len(header)), "Content-Type": "application/octet-stream"},
        )
        try:
            size = int(headers[HEADER])
            if not 0 < size < len(content):
                raise ValueError("invalid header size")
            answer, data = json.loads(content[:size]), content[size:]
            outputs = answer["outputs"]
            if len(outputs) != 1 or not re.fullmatch(r"[0-9]+", answer["model_version"]):
                raise ValueError("invalid version or outputs")
            output = outputs[0]
            if (
                output["name"] != "output__0"
                or output["datatype"] != "FP32"
                or output["shape"] != [1, 2]
                or output["parameters"]["binary_data_size"] != 8
                or len(data) != 8
            ):
                raise ValueError("invalid tensor")
            score = struct.unpack("<2f", data)[1]
            if not math.isfinite(score) or not 0 <= score <= 1:
                raise ValueError("invalid probability")
            return score, answer["model_version"]
        except (KeyError, TypeError, ValueError, struct.error) as error:
            raise ValueError("model server returned an invalid inference response") from error

    async def score(self, views):
        async with self.lock:
            if self.layout is None:
                self.layout = await self.config()
            layout = self.layout
        for attempt in range(2):
            values = row(views, layout.order)
            score, version = await self.infer(values)
            if "v" + version == layout.version:
                return {"score": score, "model_version": version, "decline_threshold": layout.threshold}, dict(
                    zip(layout.order, values, strict=True)
                )
            if attempt == 0:
                layout = await self.config()
                self.layout = layout
        raise ValueError("model server version changed twice; no consistent score")
