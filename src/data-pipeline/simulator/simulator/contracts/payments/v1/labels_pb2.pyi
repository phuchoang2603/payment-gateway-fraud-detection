import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class LabelSource(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    LABEL_SOURCE_UNSPECIFIED: _ClassVar[LabelSource]
    LABEL_SOURCE_CHARGEBACK: _ClassVar[LabelSource]
    LABEL_SOURCE_SIMULATION_TRUTH: _ClassVar[LabelSource]
LABEL_SOURCE_UNSPECIFIED: LabelSource
LABEL_SOURCE_CHARGEBACK: LabelSource
LABEL_SOURCE_SIMULATION_TRUTH: LabelSource

class FraudLabel(_message.Message):
    __slots__ = ("integration_id", "payment_id", "attempt_id", "is_fraud", "occurred_at", "label_available_at", "source", "scenario")
    INTEGRATION_ID_FIELD_NUMBER: _ClassVar[int]
    PAYMENT_ID_FIELD_NUMBER: _ClassVar[int]
    ATTEMPT_ID_FIELD_NUMBER: _ClassVar[int]
    IS_FRAUD_FIELD_NUMBER: _ClassVar[int]
    OCCURRED_AT_FIELD_NUMBER: _ClassVar[int]
    LABEL_AVAILABLE_AT_FIELD_NUMBER: _ClassVar[int]
    SOURCE_FIELD_NUMBER: _ClassVar[int]
    SCENARIO_FIELD_NUMBER: _ClassVar[int]
    integration_id: str
    payment_id: str
    attempt_id: str
    is_fraud: bool
    occurred_at: _timestamp_pb2.Timestamp
    label_available_at: _timestamp_pb2.Timestamp
    source: LabelSource
    scenario: str
    def __init__(self, integration_id: _Optional[str] = ..., payment_id: _Optional[str] = ..., attempt_id: _Optional[str] = ..., is_fraud: _Optional[bool] = ..., occurred_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., label_available_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., source: _Optional[_Union[LabelSource, str]] = ..., scenario: _Optional[str] = ...) -> None: ...
