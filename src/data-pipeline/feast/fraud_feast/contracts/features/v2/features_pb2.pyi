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

ENTITY_VIEW_UNSPECIFIED: EntityView
ENTITY_VIEW_CUSTOMER: EntityView
ENTITY_VIEW_METHOD: EntityView
COVERAGE_UNSPECIFIED: Coverage
COVERAGE_UNKNOWN: Coverage
COVERAGE_COMPLETE: Coverage

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
    __slots__ = ("topic", "partition", "consumed_offset", "observed_high_watermark", "observed_at")
    TOPIC_FIELD_NUMBER: _ClassVar[int]
    PARTITION_FIELD_NUMBER: _ClassVar[int]
    CONSUMED_OFFSET_FIELD_NUMBER: _ClassVar[int]
    OBSERVED_HIGH_WATERMARK_FIELD_NUMBER: _ClassVar[int]
    OBSERVED_AT_FIELD_NUMBER: _ClassVar[int]
    topic: str
    partition: int
    consumed_offset: int
    observed_high_watermark: int
    observed_at: _timestamp_pb2.Timestamp
    def __init__(
        self,
        topic: _Optional[str] = ...,
        partition: _Optional[int] = ...,
        consumed_offset: _Optional[int] = ...,
        observed_high_watermark: _Optional[int] = ...,
        observed_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...,
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
