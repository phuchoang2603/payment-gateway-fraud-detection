import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from simulator.contracts.payments.v1 import types_pb2 as _types_pb2
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class DeclineSource(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    DECLINE_SOURCE_UNSPECIFIED: _ClassVar[DeclineSource]
    DECLINE_SOURCE_RISK: _ClassVar[DeclineSource]
    DECLINE_SOURCE_PROCESSOR: _ClassVar[DeclineSource]
DECLINE_SOURCE_UNSPECIFIED: DeclineSource
DECLINE_SOURCE_RISK: DeclineSource
DECLINE_SOURCE_PROCESSOR: DeclineSource

class PaymentEvent(_message.Message):
    __slots__ = ("event_id", "payment_id", "aggregate_version", "integration_id", "occurred_at", "recorded_at", "schema_version", "correlation_id", "payment_created", "attempt_started", "risk_decision_recorded", "processor_operation_requested", "processor_outcome_unknown", "attempt_declined", "payment_succeeded", "payment_expired")
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    PAYMENT_ID_FIELD_NUMBER: _ClassVar[int]
    AGGREGATE_VERSION_FIELD_NUMBER: _ClassVar[int]
    INTEGRATION_ID_FIELD_NUMBER: _ClassVar[int]
    OCCURRED_AT_FIELD_NUMBER: _ClassVar[int]
    RECORDED_AT_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_VERSION_FIELD_NUMBER: _ClassVar[int]
    CORRELATION_ID_FIELD_NUMBER: _ClassVar[int]
    PAYMENT_CREATED_FIELD_NUMBER: _ClassVar[int]
    ATTEMPT_STARTED_FIELD_NUMBER: _ClassVar[int]
    RISK_DECISION_RECORDED_FIELD_NUMBER: _ClassVar[int]
    PROCESSOR_OPERATION_REQUESTED_FIELD_NUMBER: _ClassVar[int]
    PROCESSOR_OUTCOME_UNKNOWN_FIELD_NUMBER: _ClassVar[int]
    ATTEMPT_DECLINED_FIELD_NUMBER: _ClassVar[int]
    PAYMENT_SUCCEEDED_FIELD_NUMBER: _ClassVar[int]
    PAYMENT_EXPIRED_FIELD_NUMBER: _ClassVar[int]
    event_id: str
    payment_id: str
    aggregate_version: int
    integration_id: str
    occurred_at: _timestamp_pb2.Timestamp
    recorded_at: _timestamp_pb2.Timestamp
    schema_version: int
    correlation_id: str
    payment_created: PaymentCreated
    attempt_started: AttemptStarted
    risk_decision_recorded: RiskDecisionRecorded
    processor_operation_requested: ProcessorOperationRequested
    processor_outcome_unknown: ProcessorOutcomeUnknown
    attempt_declined: AttemptDeclined
    payment_succeeded: PaymentSucceeded
    payment_expired: PaymentExpired
    def __init__(self, event_id: _Optional[str] = ..., payment_id: _Optional[str] = ..., aggregate_version: _Optional[int] = ..., integration_id: _Optional[str] = ..., occurred_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., recorded_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., schema_version: _Optional[int] = ..., correlation_id: _Optional[str] = ..., payment_created: _Optional[_Union[PaymentCreated, _Mapping]] = ..., attempt_started: _Optional[_Union[AttemptStarted, _Mapping]] = ..., risk_decision_recorded: _Optional[_Union[RiskDecisionRecorded, _Mapping]] = ..., processor_operation_requested: _Optional[_Union[ProcessorOperationRequested, _Mapping]] = ..., processor_outcome_unknown: _Optional[_Union[ProcessorOutcomeUnknown, _Mapping]] = ..., attempt_declined: _Optional[_Union[AttemptDeclined, _Mapping]] = ..., payment_succeeded: _Optional[_Union[PaymentSucceeded, _Mapping]] = ..., payment_expired: _Optional[_Union[PaymentExpired, _Mapping]] = ...) -> None: ...

class PaymentCreated(_message.Message):
    __slots__ = ("merchant", "customer", "snapshot")
    MERCHANT_FIELD_NUMBER: _ClassVar[int]
    CUSTOMER_FIELD_NUMBER: _ClassVar[int]
    SNAPSHOT_FIELD_NUMBER: _ClassVar[int]
    merchant: _types_pb2.MerchantRef
    customer: _types_pb2.CustomerRef
    snapshot: _types_pb2.CheckoutSnapshot
    def __init__(self, merchant: _Optional[_Union[_types_pb2.MerchantRef, _Mapping]] = ..., customer: _Optional[_Union[_types_pb2.CustomerRef, _Mapping]] = ..., snapshot: _Optional[_Union[_types_pb2.CheckoutSnapshot, _Mapping]] = ...) -> None: ...

class AttemptStarted(_message.Message):
    __slots__ = ("attempt_id", "signals")
    ATTEMPT_ID_FIELD_NUMBER: _ClassVar[int]
    SIGNALS_FIELD_NUMBER: _ClassVar[int]
    attempt_id: str
    signals: _types_pb2.AttemptSignals
    def __init__(self, attempt_id: _Optional[str] = ..., signals: _Optional[_Union[_types_pb2.AttemptSignals, _Mapping]] = ...) -> None: ...

class RiskDecisionRecorded(_message.Message):
    __slots__ = ("attempt_id", "assessment")
    ATTEMPT_ID_FIELD_NUMBER: _ClassVar[int]
    ASSESSMENT_FIELD_NUMBER: _ClassVar[int]
    attempt_id: str
    assessment: _types_pb2.RiskAssessment
    def __init__(self, attempt_id: _Optional[str] = ..., assessment: _Optional[_Union[_types_pb2.RiskAssessment, _Mapping]] = ...) -> None: ...

class ProcessorOperationRequested(_message.Message):
    __slots__ = ("attempt_id", "operation_key")
    ATTEMPT_ID_FIELD_NUMBER: _ClassVar[int]
    OPERATION_KEY_FIELD_NUMBER: _ClassVar[int]
    attempt_id: str
    operation_key: str
    def __init__(self, attempt_id: _Optional[str] = ..., operation_key: _Optional[str] = ...) -> None: ...

class ProcessorOutcomeUnknown(_message.Message):
    __slots__ = ("attempt_id",)
    ATTEMPT_ID_FIELD_NUMBER: _ClassVar[int]
    attempt_id: str
    def __init__(self, attempt_id: _Optional[str] = ...) -> None: ...

class AttemptDeclined(_message.Message):
    __slots__ = ("attempt_id", "source", "reason")
    ATTEMPT_ID_FIELD_NUMBER: _ClassVar[int]
    SOURCE_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    attempt_id: str
    source: DeclineSource
    reason: str
    def __init__(self, attempt_id: _Optional[str] = ..., source: _Optional[_Union[DeclineSource, str]] = ..., reason: _Optional[str] = ...) -> None: ...

class PaymentSucceeded(_message.Message):
    __slots__ = ("attempt_id", "processor_reference")
    ATTEMPT_ID_FIELD_NUMBER: _ClassVar[int]
    PROCESSOR_REFERENCE_FIELD_NUMBER: _ClassVar[int]
    attempt_id: str
    processor_reference: str
    def __init__(self, attempt_id: _Optional[str] = ..., processor_reference: _Optional[str] = ...) -> None: ...

class PaymentExpired(_message.Message):
    __slots__ = ("present",)
    PRESENT_FIELD_NUMBER: _ClassVar[int]
    present: bool
    def __init__(self, present: _Optional[bool] = ...) -> None: ...
