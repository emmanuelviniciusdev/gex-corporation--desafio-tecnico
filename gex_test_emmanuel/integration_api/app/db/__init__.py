from datetime import UTC, datetime

from sqlalchemy import JSON, DateTime, ForeignKey, Index, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from app.core.database import Base


class RawPayload(Base):
    __tablename__ = "raw_payloads"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    correlation_id: Mapped[str] = mapped_column(String(36), nullable=False)
    gateway: Mapped[str] = mapped_column(String(20), nullable=False)
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    headers: Mapped[dict] = mapped_column(JSON, nullable=False)
    original_body: Mapped[str] = mapped_column(Text, nullable=False)
    decrypted_body: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(UTC)
    )

    __table_args__ = (
        Index("idx_correlation_id", "correlation_id"),
        Index("idx_gateway", "gateway"),
        Index("idx_received_at", "received_at"),
    )


class ProcessedWebhook(Base):
    __tablename__ = "processed_webhooks"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    transaction_id: Mapped[str] = mapped_column(String(255), nullable=False)
    event: Mapped[str] = mapped_column(String(50), nullable=False)
    correlation_id: Mapped[str] = mapped_column(String(36), nullable=False)
    processed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(UTC)
    )


class LeadDeadLetter(Base):
    __tablename__ = "lead_dead_letter"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    correlation_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    origin: Mapped[str] = mapped_column(String(50), nullable=False)
    raw_payload_id: Mapped[int | None] = mapped_column(
        Integer, ForeignKey("raw_payloads.id"), nullable=True
    )
    payload: Mapped[str] = mapped_column(Text, nullable=False)
    error_message: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(UTC)
    )

    __table_args__ = (
        Index("idx_dlq_origin_created", "origin", "created_at"),
        Index("idx_dlq_correlation", "correlation_id"),
        Index("idx_dlq_raw_payload", "raw_payload_id"),
    )
