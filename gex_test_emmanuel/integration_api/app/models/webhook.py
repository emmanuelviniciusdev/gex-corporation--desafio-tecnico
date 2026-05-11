from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


class CustomerPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    email: str
    first_name: str | None = None
    phone: str | None = None
    last_name: str
    country: str = Field(min_length=2, max_length=2)


class ProductPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str
    name: str
    niche: str


class PaymentPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    amount_usd: float
    method: Literal["credit_card", "paypal", "pix"]
    status: Literal["approved", "declined", "pending", "refunded"]


class Payload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    transaction_id: str
    transaction_time: datetime
    event: Literal["order.approved", "order.refunded", "order.declined"]
    customer: CustomerPayload
    product: ProductPayload
    quantity: int
    payment: PaymentPayload

    @field_validator("transaction_time")
    @classmethod
    def validate_transaction_time_has_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError("transaction_time must include timezone")
        return value


class EncryptedPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    iv: str
    ciphertext: str


class WebhookResponse(BaseModel):
    correlation_id: str
    gateway: Literal["grummer", "lous"]
    schema_valid: bool
    invalid_email: bool
    invalid_phone: bool
    normalized_payload: dict
    validation_errors: list[dict]


class DuplicateResponse(BaseModel):
    status: Literal["duplicate"]
    correlation_id: str
