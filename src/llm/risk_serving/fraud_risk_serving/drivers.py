import bisect
import json
import math

from fraud_risk_serving.upstream import read

NOTICE = "Ranking of unusual, influential features, not an attribution."


def rank(reference, values):
    ranked = []
    total_gain = sum(entry["gain"] for entry in reference.values())
    for name in sorted(values):
        entry, value = reference[name], values[name]
        edges, shares = entry["edges"], entry["share"]
        if math.isnan(value):
            position, slot, distance = None, "missing", 1 - shares[-1]
        else:
            slot = bisect.bisect_right(edges, value)
            position = sum(shares[:slot]) + shares[slot] / 2
            distance = abs(position - 0.5) * 2
        weight = entry["gain"] / total_gain * distance if total_gain else 0
        ranked.append(
            {
                "feature": name,
                "value": value if math.isfinite(value) else None,
                "bin": slot,
                "position": position,
                "weight": weight,
            }
        )
    ranked.sort(key=lambda driver: (-driver["weight"], driver["feature"]))
    total = sum(driver["weight"] for driver in ranked)
    return [{**driver, "share": driver["weight"] / total if total else 0} for driver in ranked[:5]]


class Reference:
    def __init__(self, client, url):
        self.client, self.url, self.cached = client, url.rstrip("/") + "/v1/reference", None

    async def drivers(self, version, values):
        if self.cached is None or self.cached["version"] != "v" + version:
            content, _ = await read(self.client, "reference profile", "GET", self.url, 3)
            try:
                result = json.loads(content)
                if result["version"] != "v" + version:
                    return {
                        "drivers": [],
                        "drivers_notice": NOTICE,
                        "drivers_unavailable_reason": "reference profile belongs to another model version",
                    }
                reference = result["reference"]
                for name in values:
                    entry = reference[name]
                    edges, shares, gain = entry["edges"], entry["share"], entry["gain"]
                    if (
                        len(shares) != len(edges) + 2
                        or edges != sorted(set(edges))
                        or any(not math.isfinite(edge) for edge in edges)
                        or any(not math.isfinite(share) or share < 0 for share in shares)
                        or not math.isclose(sum(shares), 1, abs_tol=1e-5)
                        or not math.isfinite(gain)
                        or gain < 0
                    ):
                        raise ValueError("invalid reference feature")
                self.cached = {"version": result["version"], "reference": {name: reference[name] for name in values}}
            except (KeyError, TypeError, ValueError) as error:
                raise ValueError("reference profile returned malformed data") from error
        reference = self.cached["reference"]
        has_gains = any(entry["gain"] > 0 for entry in reference.values())
        return {
            "drivers": rank(reference, values) if has_gains else [],
            "drivers_notice": NOTICE,
            "drivers_unavailable_reason": None if has_gains else "reference profile has no positive feature gains",
        }
