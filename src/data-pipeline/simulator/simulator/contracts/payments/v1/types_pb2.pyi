import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class RiskOutcome(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    RISK_OUTCOME_UNSPECIFIED: _ClassVar[RiskOutcome]
    RISK_OUTCOME_APPROVE: _ClassVar[RiskOutcome]
    RISK_OUTCOME_DECLINE: _ClassVar[RiskOutcome]
RISK_OUTCOME_UNSPECIFIED: RiskOutcome
RISK_OUTCOME_APPROVE: RiskOutcome
RISK_OUTCOME_DECLINE: RiskOutcome

class Money(_message.Message):
    __slots__ = ("minor_units", "currency")
    MINOR_UNITS_FIELD_NUMBER: _ClassVar[int]
    CURRENCY_FIELD_NUMBER: _ClassVar[int]
    minor_units: int
    currency: str
    def __init__(self, minor_units: _Optional[int] = ..., currency: _Optional[str] = ...) -> None: ...

class MerchantRef(_message.Message):
    __slots__ = ("merchant_id", "external_seller_id")
    MERCHANT_ID_FIELD_NUMBER: _ClassVar[int]
    EXTERNAL_SELLER_ID_FIELD_NUMBER: _ClassVar[int]
    merchant_id: str
    external_seller_id: str
    def __init__(self, merchant_id: _Optional[str] = ..., external_seller_id: _Optional[str] = ...) -> None: ...

class CustomerRef(_message.Message):
    __slots__ = ("customer_id", "external_buyer_id")
    CUSTOMER_ID_FIELD_NUMBER: _ClassVar[int]
    EXTERNAL_BUYER_ID_FIELD_NUMBER: _ClassVar[int]
    customer_id: str
    external_buyer_id: str
    def __init__(self, customer_id: _Optional[str] = ..., external_buyer_id: _Optional[str] = ...) -> None: ...

class LineItem(_message.Message):
    __slots__ = ("product_id", "category", "condition", "quantity", "unit_price")
    PRODUCT_ID_FIELD_NUMBER: _ClassVar[int]
    CATEGORY_FIELD_NUMBER: _ClassVar[int]
    CONDITION_FIELD_NUMBER: _ClassVar[int]
    QUANTITY_FIELD_NUMBER: _ClassVar[int]
    UNIT_PRICE_FIELD_NUMBER: _ClassVar[int]
    product_id: str
    category: str
    condition: str
    quantity: int
    unit_price: Money
    def __init__(self, product_id: _Optional[str] = ..., category: _Optional[str] = ..., condition: _Optional[str] = ..., quantity: _Optional[int] = ..., unit_price: _Optional[_Union[Money, _Mapping]] = ...) -> None: ...

class ShippingLocation(_message.Message):
    __slots__ = ("country", "region", "postal_code", "address_fingerprint")
    COUNTRY_FIELD_NUMBER: _ClassVar[int]
    REGION_FIELD_NUMBER: _ClassVar[int]
    POSTAL_CODE_FIELD_NUMBER: _ClassVar[int]
    ADDRESS_FINGERPRINT_FIELD_NUMBER: _ClassVar[int]
    country: str
    region: str
    postal_code: str
    address_fingerprint: str
    def __init__(self, country: _Optional[str] = ..., region: _Optional[str] = ..., postal_code: _Optional[str] = ..., address_fingerprint: _Optional[str] = ...) -> None: ...

class CheckoutSnapshot(_message.Message):
    __slots__ = ("external_order_id", "total", "items", "merchant_account_created_at", "customer_account_created_at", "shipping")
    EXTERNAL_ORDER_ID_FIELD_NUMBER: _ClassVar[int]
    TOTAL_FIELD_NUMBER: _ClassVar[int]
    ITEMS_FIELD_NUMBER: _ClassVar[int]
    MERCHANT_ACCOUNT_CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    CUSTOMER_ACCOUNT_CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    SHIPPING_FIELD_NUMBER: _ClassVar[int]
    external_order_id: str
    total: Money
    items: _containers.RepeatedCompositeFieldContainer[LineItem]
    merchant_account_created_at: _timestamp_pb2.Timestamp
    customer_account_created_at: _timestamp_pb2.Timestamp
    shipping: ShippingLocation
    def __init__(self, external_order_id: _Optional[str] = ..., total: _Optional[_Union[Money, _Mapping]] = ..., items: _Optional[_Iterable[_Union[LineItem, _Mapping]]] = ..., merchant_account_created_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., customer_account_created_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., shipping: _Optional[_Union[ShippingLocation, _Mapping]] = ...) -> None: ...

class PaymentMethod(_message.Message):
    __slots__ = ("type", "fingerprint")
    TYPE_FIELD_NUMBER: _ClassVar[int]
    FINGERPRINT_FIELD_NUMBER: _ClassVar[int]
    type: str
    fingerprint: str
    def __init__(self, type: _Optional[str] = ..., fingerprint: _Optional[str] = ...) -> None: ...

class AttemptSignals(_message.Message):
    __slots__ = ("attempted_at", "payment_method", "device_fingerprint", "ip_country", "ip_network_fingerprint", "user_agent_family")
    ATTEMPTED_AT_FIELD_NUMBER: _ClassVar[int]
    PAYMENT_METHOD_FIELD_NUMBER: _ClassVar[int]
    DEVICE_FINGERPRINT_FIELD_NUMBER: _ClassVar[int]
    IP_COUNTRY_FIELD_NUMBER: _ClassVar[int]
    IP_NETWORK_FINGERPRINT_FIELD_NUMBER: _ClassVar[int]
    USER_AGENT_FAMILY_FIELD_NUMBER: _ClassVar[int]
    attempted_at: _timestamp_pb2.Timestamp
    payment_method: PaymentMethod
    device_fingerprint: str
    ip_country: str
    ip_network_fingerprint: str
    user_agent_family: str
    def __init__(self, attempted_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., payment_method: _Optional[_Union[PaymentMethod, _Mapping]] = ..., device_fingerprint: _Optional[str] = ..., ip_country: _Optional[str] = ..., ip_network_fingerprint: _Optional[str] = ..., user_agent_family: _Optional[str] = ...) -> None: ...

class RiskAssessment(_message.Message):
    __slots__ = ("outcome", "reason_codes", "policy_version", "model_version", "feature_set_version", "risk_score")
    OUTCOME_FIELD_NUMBER: _ClassVar[int]
    REASON_CODES_FIELD_NUMBER: _ClassVar[int]
    POLICY_VERSION_FIELD_NUMBER: _ClassVar[int]
    MODEL_VERSION_FIELD_NUMBER: _ClassVar[int]
    FEATURE_SET_VERSION_FIELD_NUMBER: _ClassVar[int]
    RISK_SCORE_FIELD_NUMBER: _ClassVar[int]
    outcome: RiskOutcome
    reason_codes: _containers.RepeatedScalarFieldContainer[str]
    policy_version: str
    model_version: str
    feature_set_version: str
    risk_score: float
    def __init__(self, outcome: _Optional[_Union[RiskOutcome, str]] = ..., reason_codes: _Optional[_Iterable[str]] = ..., policy_version: _Optional[str] = ..., model_version: _Optional[str] = ..., feature_set_version: _Optional[str] = ..., risk_score: _Optional[float] = ...) -> None: ...
