import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class EntityView(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    ENTITY_VIEW_UNSPECIFIED: _ClassVar[EntityView]
    ENTITY_VIEW_CUSTOMER: _ClassVar[EntityView]
    ENTITY_VIEW_METHOD: _ClassVar[EntityView]

class Coverage(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    COVERAGE_UNSPECIFIED: _ClassVar[Coverage]
    COVERAGE_UNKNOWN: _ClassVar[Coverage]
    COVERAGE_COMPLETE: _ClassVar[Coverage]

class ViewStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    VIEW_STATUS_UNSPECIFIED: _ClassVar[ViewStatus]
    VIEW_STATUS_AVAILABLE: _ClassVar[ViewStatus]
    VIEW_STATUS_MISSING: _ClassVar[ViewStatus]
    VIEW_STATUS_TIMEOUT: _ClassVar[ViewStatus]
    VIEW_STATUS_ERROR: _ClassVar[ViewStatus]
    VIEW_STATUS_STALE: _ClassVar[ViewStatus]
    VIEW_STATUS_FUTURE: _ClassVar[ViewStatus]
    VIEW_STATUS_INCOMPATIBLE: _ClassVar[ViewStatus]
    VIEW_STATUS_PARTIAL: _ClassVar[ViewStatus]
    VIEW_STATUS_DISABLED: _ClassVar[ViewStatus]

class GuardStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    GUARD_STATUS_UNSPECIFIED: _ClassVar[GuardStatus]
    GUARD_STATUS_REGISTERED: _ClassVar[GuardStatus]
    GUARD_STATUS_TIMEOUT: _ClassVar[GuardStatus]
    GUARD_STATUS_ERROR: _ClassVar[GuardStatus]
    GUARD_STATUS_COVERAGE_UNKNOWN: _ClassVar[GuardStatus]
    GUARD_STATUS_EXPIRED_RETRY: _ClassVar[GuardStatus]
    GUARD_STATUS_CONFLICT: _ClassVar[GuardStatus]
    GUARD_STATUS_DISABLED: _ClassVar[GuardStatus]

ENTITY_VIEW_UNSPECIFIED: EntityView
ENTITY_VIEW_CUSTOMER: EntityView
ENTITY_VIEW_METHOD: EntityView
COVERAGE_UNSPECIFIED: Coverage
COVERAGE_UNKNOWN: Coverage
COVERAGE_COMPLETE: Coverage
VIEW_STATUS_UNSPECIFIED: ViewStatus
VIEW_STATUS_AVAILABLE: ViewStatus
VIEW_STATUS_MISSING: ViewStatus
VIEW_STATUS_TIMEOUT: ViewStatus
VIEW_STATUS_ERROR: ViewStatus
VIEW_STATUS_STALE: ViewStatus
VIEW_STATUS_FUTURE: ViewStatus
VIEW_STATUS_INCOMPATIBLE: ViewStatus
VIEW_STATUS_PARTIAL: ViewStatus
VIEW_STATUS_DISABLED: ViewStatus
GUARD_STATUS_UNSPECIFIED: GuardStatus
GUARD_STATUS_REGISTERED: GuardStatus
GUARD_STATUS_TIMEOUT: GuardStatus
GUARD_STATUS_ERROR: GuardStatus
GUARD_STATUS_COVERAGE_UNKNOWN: GuardStatus
GUARD_STATUS_EXPIRED_RETRY: GuardStatus
GUARD_STATUS_CONFLICT: GuardStatus
GUARD_STATUS_DISABLED: GuardStatus

class WindowCoverage(_message.Message):
    __slots__ = ("five_minutes", "one_hour", "twenty_four_hours")
    FIVE_MINUTES_FIELD_NUMBER: _ClassVar[int]
    ONE_HOUR_FIELD_NUMBER: _ClassVar[int]
    TWENTY_FOUR_HOURS_FIELD_NUMBER: _ClassVar[int]
    five_minutes: Coverage
    one_hour: Coverage
    twenty_four_hours: Coverage
    def __init__(
        self,
        five_minutes: _Optional[_Union[Coverage, str]] = ...,
        one_hour: _Optional[_Union[Coverage, str]] = ...,
        twenty_four_hours: _Optional[_Union[Coverage, str]] = ...,
    ) -> None: ...

class DefinitionIdentity(_message.Message):
    __slots__ = ("version", "sha256")
    VERSION_FIELD_NUMBER: _ClassVar[int]
    SHA256_FIELD_NUMBER: _ClassVar[int]
    version: str
    sha256: str
    def __init__(self, version: _Optional[str] = ..., sha256: _Optional[str] = ...) -> None: ...

class SourcePartitionProgress(_message.Message):
    __slots__ = (
        "topic",
        "partition",
        "consumed_offset",
        "observed_high_watermark",
        "observed_at",
        "heartbeat_at",
    )
    TOPIC_FIELD_NUMBER: _ClassVar[int]
    PARTITION_FIELD_NUMBER: _ClassVar[int]
    CONSUMED_OFFSET_FIELD_NUMBER: _ClassVar[int]
    OBSERVED_HIGH_WATERMARK_FIELD_NUMBER: _ClassVar[int]
    OBSERVED_AT_FIELD_NUMBER: _ClassVar[int]
    HEARTBEAT_AT_FIELD_NUMBER: _ClassVar[int]
    topic: str
    partition: int
    consumed_offset: int
    observed_high_watermark: int
    observed_at: _timestamp_pb2.Timestamp
    heartbeat_at: _timestamp_pb2.Timestamp
    def __init__(
        self,
        topic: _Optional[str] = ...,
        partition: _Optional[int] = ...,
        consumed_offset: _Optional[int] = ...,
        observed_high_watermark: _Optional[int] = ...,
        observed_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...,
        heartbeat_at: _Optional[
            _Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]
        ] = ...,
    ) -> None: ...

class SnapshotValues(_message.Message):
    __slots__ = (
        "attempt_count_5m",
        "attempt_count_1h",
        "attempt_count_24h",
        "distinct_devices_24h",
    )
    ATTEMPT_COUNT_5M_FIELD_NUMBER: _ClassVar[int]
    ATTEMPT_COUNT_1H_FIELD_NUMBER: _ClassVar[int]
    ATTEMPT_COUNT_24H_FIELD_NUMBER: _ClassVar[int]
    DISTINCT_DEVICES_24H_FIELD_NUMBER: _ClassVar[int]
    attempt_count_5m: int
    attempt_count_1h: int
    attempt_count_24h: int
    distinct_devices_24h: int
    def __init__(
        self,
        attempt_count_5m: _Optional[int] = ...,
        attempt_count_1h: _Optional[int] = ...,
        attempt_count_24h: _Optional[int] = ...,
        distinct_devices_24h: _Optional[int] = ...,
    ) -> None: ...

class SnapshotUpdate(_message.Message):
    __slots__ = (
        "schema_version",
        "definition",
        "epoch",
        "view",
        "integration_id",
        "entity_id",
        "snapshot_id",
        "revision",
        "anchor",
        "source_available_at",
        "online_write_at",
        "coverage",
        "source_progress",
        "values",
    )
    SCHEMA_VERSION_FIELD_NUMBER: _ClassVar[int]
    DEFINITION_FIELD_NUMBER: _ClassVar[int]
    EPOCH_FIELD_NUMBER: _ClassVar[int]
    VIEW_FIELD_NUMBER: _ClassVar[int]
    INTEGRATION_ID_FIELD_NUMBER: _ClassVar[int]
    ENTITY_ID_FIELD_NUMBER: _ClassVar[int]
    SNAPSHOT_ID_FIELD_NUMBER: _ClassVar[int]
    REVISION_FIELD_NUMBER: _ClassVar[int]
    ANCHOR_FIELD_NUMBER: _ClassVar[int]
    SOURCE_AVAILABLE_AT_FIELD_NUMBER: _ClassVar[int]
    ONLINE_WRITE_AT_FIELD_NUMBER: _ClassVar[int]
    COVERAGE_FIELD_NUMBER: _ClassVar[int]
    SOURCE_PROGRESS_FIELD_NUMBER: _ClassVar[int]
    VALUES_FIELD_NUMBER: _ClassVar[int]
    schema_version: int
    definition: DefinitionIdentity
    epoch: str
    view: EntityView
    integration_id: str
    entity_id: str
    snapshot_id: str
    revision: int
    anchor: _timestamp_pb2.Timestamp
    source_available_at: _timestamp_pb2.Timestamp
    online_write_at: _timestamp_pb2.Timestamp
    coverage: WindowCoverage
    source_progress: _containers.RepeatedCompositeFieldContainer[SourcePartitionProgress]
    values: SnapshotValues
    def __init__(
        self,
        schema_version: _Optional[int] = ...,
        definition: _Optional[_Union[DefinitionIdentity, _Mapping]] = ...,
        epoch: _Optional[str] = ...,
        view: _Optional[_Union[EntityView, str]] = ...,
        integration_id: _Optional[str] = ...,
        entity_id: _Optional[str] = ...,
        snapshot_id: _Optional[str] = ...,
        revision: _Optional[int] = ...,
        anchor: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...,
        source_available_at: _Optional[
            _Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]
        ] = ...,
        online_write_at: _Optional[
            _Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]
        ] = ...,
        coverage: _Optional[_Union[WindowCoverage, _Mapping]] = ...,
        source_progress: _Optional[_Iterable[_Union[SourcePartitionProgress, _Mapping]]] = ...,
        values: _Optional[_Union[SnapshotValues, _Mapping]] = ...,
    ) -> None: ...

class ViewEvidence(_message.Message):
    __slots__ = ("view", "status", "observed", "read_at", "online_available_at", "available_values")
    VIEW_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    OBSERVED_FIELD_NUMBER: _ClassVar[int]
    READ_AT_FIELD_NUMBER: _ClassVar[int]
    ONLINE_AVAILABLE_AT_FIELD_NUMBER: _ClassVar[int]
    AVAILABLE_VALUES_FIELD_NUMBER: _ClassVar[int]
    view: EntityView
    status: ViewStatus
    observed: SnapshotUpdate
    read_at: _timestamp_pb2.Timestamp
    online_available_at: _timestamp_pb2.Timestamp
    available_values: SnapshotValues
    def __init__(
        self,
        view: _Optional[_Union[EntityView, str]] = ...,
        status: _Optional[_Union[ViewStatus, str]] = ...,
        observed: _Optional[_Union[SnapshotUpdate, _Mapping]] = ...,
        read_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...,
        online_available_at: _Optional[
            _Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]
        ] = ...,
        available_values: _Optional[_Union[SnapshotValues, _Mapping]] = ...,
    ) -> None: ...

class GuardEvidence(_message.Message):
    __slots__ = (
        "view",
        "status",
        "recovery_epoch",
        "registered_at",
        "read_at",
        "submission_count_10s",
        "submission_count_60s",
        "threshold_exceeded",
        "policy_version",
        "observe_only",
        "threshold_10s",
        "threshold_60s",
    )
    VIEW_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    RECOVERY_EPOCH_FIELD_NUMBER: _ClassVar[int]
    REGISTERED_AT_FIELD_NUMBER: _ClassVar[int]
    READ_AT_FIELD_NUMBER: _ClassVar[int]
    SUBMISSION_COUNT_10S_FIELD_NUMBER: _ClassVar[int]
    SUBMISSION_COUNT_60S_FIELD_NUMBER: _ClassVar[int]
    THRESHOLD_EXCEEDED_FIELD_NUMBER: _ClassVar[int]
    POLICY_VERSION_FIELD_NUMBER: _ClassVar[int]
    OBSERVE_ONLY_FIELD_NUMBER: _ClassVar[int]
    THRESHOLD_10S_FIELD_NUMBER: _ClassVar[int]
    THRESHOLD_60S_FIELD_NUMBER: _ClassVar[int]
    view: EntityView
    status: GuardStatus
    recovery_epoch: str
    registered_at: _timestamp_pb2.Timestamp
    read_at: _timestamp_pb2.Timestamp
    submission_count_10s: int
    submission_count_60s: int
    threshold_exceeded: bool
    policy_version: str
    observe_only: bool
    threshold_10s: int
    threshold_60s: int
    def __init__(
        self,
        view: _Optional[_Union[EntityView, str]] = ...,
        status: _Optional[_Union[GuardStatus, str]] = ...,
        recovery_epoch: _Optional[str] = ...,
        registered_at: _Optional[
            _Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]
        ] = ...,
        read_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...,
        submission_count_10s: _Optional[int] = ...,
        submission_count_60s: _Optional[int] = ...,
        threshold_exceeded: _Optional[bool] = ...,
        policy_version: _Optional[str] = ...,
        observe_only: _Optional[bool] = ...,
        threshold_10s: _Optional[int] = ...,
        threshold_60s: _Optional[int] = ...,
    ) -> None: ...

class AssessmentEvidence(_message.Message):
    __slots__ = (
        "schema_version",
        "assessment_id",
        "request_sha256",
        "integration_id",
        "payment_id",
        "attempt_id",
        "policy_version",
        "observed_definition",
        "customer",
        "method",
        "customer_guard",
        "method_guard",
        "evaluated_at",
        "used_amount_minor",
        "used_customer_created_at",
        "used_attempted_at",
        "used_shipping_country",
        "used_ip_country",
        "historical_features_shadow_only",
    )
    SCHEMA_VERSION_FIELD_NUMBER: _ClassVar[int]
    ASSESSMENT_ID_FIELD_NUMBER: _ClassVar[int]
    REQUEST_SHA256_FIELD_NUMBER: _ClassVar[int]
    INTEGRATION_ID_FIELD_NUMBER: _ClassVar[int]
    PAYMENT_ID_FIELD_NUMBER: _ClassVar[int]
    ATTEMPT_ID_FIELD_NUMBER: _ClassVar[int]
    POLICY_VERSION_FIELD_NUMBER: _ClassVar[int]
    OBSERVED_DEFINITION_FIELD_NUMBER: _ClassVar[int]
    CUSTOMER_FIELD_NUMBER: _ClassVar[int]
    METHOD_FIELD_NUMBER: _ClassVar[int]
    CUSTOMER_GUARD_FIELD_NUMBER: _ClassVar[int]
    METHOD_GUARD_FIELD_NUMBER: _ClassVar[int]
    EVALUATED_AT_FIELD_NUMBER: _ClassVar[int]
    USED_AMOUNT_MINOR_FIELD_NUMBER: _ClassVar[int]
    USED_CUSTOMER_CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    USED_ATTEMPTED_AT_FIELD_NUMBER: _ClassVar[int]
    USED_SHIPPING_COUNTRY_FIELD_NUMBER: _ClassVar[int]
    USED_IP_COUNTRY_FIELD_NUMBER: _ClassVar[int]
    HISTORICAL_FEATURES_SHADOW_ONLY_FIELD_NUMBER: _ClassVar[int]
    schema_version: int
    assessment_id: str
    request_sha256: str
    integration_id: str
    payment_id: str
    attempt_id: str
    policy_version: str
    observed_definition: DefinitionIdentity
    customer: ViewEvidence
    method: ViewEvidence
    customer_guard: GuardEvidence
    method_guard: GuardEvidence
    evaluated_at: _timestamp_pb2.Timestamp
    used_amount_minor: int
    used_customer_created_at: _timestamp_pb2.Timestamp
    used_attempted_at: _timestamp_pb2.Timestamp
    used_shipping_country: str
    used_ip_country: str
    historical_features_shadow_only: bool
    def __init__(
        self,
        schema_version: _Optional[int] = ...,
        assessment_id: _Optional[str] = ...,
        request_sha256: _Optional[str] = ...,
        integration_id: _Optional[str] = ...,
        payment_id: _Optional[str] = ...,
        attempt_id: _Optional[str] = ...,
        policy_version: _Optional[str] = ...,
        observed_definition: _Optional[_Union[DefinitionIdentity, _Mapping]] = ...,
        customer: _Optional[_Union[ViewEvidence, _Mapping]] = ...,
        method: _Optional[_Union[ViewEvidence, _Mapping]] = ...,
        customer_guard: _Optional[_Union[GuardEvidence, _Mapping]] = ...,
        method_guard: _Optional[_Union[GuardEvidence, _Mapping]] = ...,
        evaluated_at: _Optional[
            _Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]
        ] = ...,
        used_amount_minor: _Optional[int] = ...,
        used_customer_created_at: _Optional[
            _Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]
        ] = ...,
        used_attempted_at: _Optional[
            _Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]
        ] = ...,
        used_shipping_country: _Optional[str] = ...,
        used_ip_country: _Optional[str] = ...,
        historical_features_shadow_only: _Optional[bool] = ...,
    ) -> None: ...
