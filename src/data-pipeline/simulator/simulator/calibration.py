import json
from datetime import UTC, datetime, time, timedelta
from functools import cache
from hashlib import sha256
from importlib.resources import files
from zoneinfo import ZoneInfo

import numpy as np
import pytz

_BODY = files("simulator").joinpath("us-calibration.json").read_bytes()
CALIBRATION = json.loads(_BODY)
REGIONS = tuple(CALIBRATION["regions"])


@cache
def zone(index: int) -> ZoneInfo:
    name = REGIONS[index][3]
    with files("pytz").joinpath("zoneinfo", name).open("rb") as stream:
        return ZoneInfo.from_file(stream, key=name)


@cache
def provenance() -> dict:
    digest = sha256(_BODY)
    digest.update(pytz.__version__.encode())
    for index in range(len(REGIONS)):
        digest.update(files("pytz").joinpath("zoneinfo", REGIONS[index][3]).read_bytes())
    return {
        "id": CALIBRATION["id"],
        "sha256": digest.hexdigest(),
        "tzdb": pytz.OLSON_VERSION,
        "sources": CALIBRATION["sources"],
        "assumptions": CALIBRATION["assumptions"],
    }


@cache
def hourly_weights(day, region):
    midnight = datetime.combine(day, time.min, UTC)
    local = [(midnight + timedelta(hours=hour, minutes=30)).astimezone(zone(region)) for hour in range(24)]
    weights = np.array([CALIBRATION["local_hour_weights"][value.hour] for value in local])
    intensity = np.array(
        [
            CALIBRATION["weekday_multipliers"][value.weekday()] * CALIBRATION["month_multipliers"][value.month - 1]
            for value in local
        ]
    )
    return weights * intensity / (weights * intensity).sum(), float(np.average(intensity, weights=weights))


def purchase_seconds(day, regions, random):
    result = np.empty(len(regions), dtype=np.int64)
    for region in np.unique(regions):
        selected = np.flatnonzero(regions == region)
        weights, _ = hourly_weights(day, int(region))
        result[selected] = random.choice(24, len(selected), p=weights) * 3600 + random.integers(0, 3600, len(selected))
    return result
