"""The drift API's response models."""

from typing import Literal

from pydantic import BaseModel, Field


class FeatureProfile(BaseModel):
    """One feature's reference bins: ascending edges, the share of training rows in each bin (missing last) and its gain."""

    edges: list[float]
    share: list[float] = Field(min_length=2)
    gain: float = 0.0


class Serving(BaseModel):
    """The serving model version, when it was registered and the reference profile its requests are compared with."""

    version: str = Field(min_length=1)
    created: str = Field(min_length=1)
    reference: dict[str, FeatureProfile]


class Assessment(BaseModel):
    """The drift of one window: the state, the requests counted, each feature's index and missing-share shift, and those over."""

    window: str
    samples: float = Field(ge=0)
    state: Literal["insufficient", "ok", "drifting"]
    psi: dict[str, float]
    missing: dict[str, float]
    over: list[str]


class DriftReport(BaseModel):
    """The drift of the serving version's scored features over a window."""

    version: str = Field(min_length=1)
    created: str = Field(min_length=1)
    assessment: Assessment


class Status(BaseModel):
    """A probe's answer."""

    status: str
