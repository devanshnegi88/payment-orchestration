"""
SQLAlchemy ORM Models.
All monetary amounts stored as BIGINT paise — never float.
transaction_state_log is INSERT-only (immutable audit trail).
"""
import uuid
from datetime import datetime
from typing import Optional
from sqlalchemy import (
    String, BigInteger, Integer, Float, Boolean, Text,
    DateTime, Enum as SAEnum, ForeignKey, UniqueConstraint,
    Index, CheckConstraint, func, text
)
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base
from app.domain.state_machine import TransactionState, TransactionEvent


def _uuid() -> str:
    return str(uuid.uuid4())


class Transaction(Base):
    __tablename__ = "transactions"

    id: Mapped[str] = mapped_column(UUID(as_uuid=False), primary_key=True, default=_uuid)
    merchant_id: Mapped[str] = mapped_column(String(100), nullable=False)
    merchant_order_id: Mapped[str] = mapped_column(String(255), nullable=False)
    amount_paise: Mapped[int] = mapped_column(BigInteger, nullable=False)
    currency: Mapped[str] = mapped_column(String(3), nullable=False, default="INR")
    authorized_amount_paise: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)
    captured_amount_paise: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True, default=0)
    refunded_amount_paise: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True, default=0)
    payment_method: Mapped[str] = mapped_column(String(50), nullable=False)
    state: Mapped[TransactionState] = mapped_column(
        SAEnum(TransactionState, name="transaction_state_enum", create_type=False),
        nullable=False, default=TransactionState.CREATED,
    )
    gateway: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    gateway_reference: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    gateway_order_id: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    idempotency_key: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    retry_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    max_retries: Mapped[int] = mapped_column(Integer, nullable=False, default=3)
    customer_id: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    customer_email: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    version: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    trace_id: Mapped[Optional[str]] = mapped_column(UUID(as_uuid=False), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )
    expires_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)

    state_logs: Mapped[list["TransactionStateLog"]] = relationship(
        "TransactionStateLog", back_populates="transaction",
        order_by="TransactionStateLog.created_at",
    )
    gateway_routes: Mapped[list["GatewayRoute"]] = relationship(
        "GatewayRoute", back_populates="transaction"
    )
    refunds: Mapped[list["Refund"]] = relationship("Refund", back_populates="transaction")
    webhook_events: Mapped[list["ProcessedWebhookEvent"]] = relationship(
        "ProcessedWebhookEvent", back_populates="transaction"
    )

    __table_args__ = (
        UniqueConstraint("merchant_id", "merchant_order_id", name="uq_merchant_order"),
        UniqueConstraint("merchant_id", "idempotency_key", name="uq_merchant_idempotency"),
        Index("idx_transactions_state", "state"),
        Index("idx_transactions_gateway_ref", "gateway_reference"),
        Index("idx_transactions_merchant_order", "merchant_id", "merchant_order_id"),
        Index("idx_transactions_created_at", "created_at"),
        Index("idx_transactions_gateway_state", "gateway", "state"),
        CheckConstraint("amount_paise > 0", name="chk_amount_positive"),
    )


class TransactionStateLog(Base):
    """INSERT-only immutable audit trail — never UPDATE or DELETE."""
    __tablename__ = "transaction_state_log"

    id: Mapped[str] = mapped_column(UUID(as_uuid=False), primary_key=True, default=_uuid)
    transaction_id: Mapped[str] = mapped_column(
        UUID(as_uuid=False), ForeignKey("transactions.id"), nullable=False
    )
    from_state: Mapped[str] = mapped_column(
        SAEnum(TransactionState, name="transaction_state_enum", create_type=False), nullable=False
    )
    to_state: Mapped[str] = mapped_column(
        SAEnum(TransactionState, name="transaction_state_enum", create_type=False), nullable=False
    )
    event: Mapped[str] = mapped_column(
        SAEnum(TransactionEvent, name="transaction_event_enum", create_type=False), nullable=False
    )
    gateway_reference: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    gateway_response: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    log_metadata: Mapped[Optional[dict]] = mapped_column("log_metadata", JSONB, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    created_by: Mapped[str] = mapped_column(String(100), nullable=False, default="system")

    transaction: Mapped["Transaction"] = relationship("Transaction", back_populates="state_logs")

    __table_args__ = (
        Index("idx_state_log_transaction", "transaction_id"),
        Index("idx_state_log_created_at", "created_at"),
    )


class GatewayRoute(Base):
    __tablename__ = "gateway_routes"

    id: Mapped[str] = mapped_column(UUID(as_uuid=False), primary_key=True, default=_uuid)
    transaction_id: Mapped[str] = mapped_column(
        UUID(as_uuid=False), ForeignKey("transactions.id"), nullable=False
    )
    gateway: Mapped[str] = mapped_column(String(50), nullable=False)
    attempt_number: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    composite_score: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    success_rate_score: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    latency_score: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    cost_score: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    health_score: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    fit_score: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    outcome: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    latency_ms: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    error_type: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    transaction: Mapped["Transaction"] = relationship("Transaction", back_populates="gateway_routes")

    __table_args__ = (Index("idx_gateway_routes_transaction", "transaction_id"),)


class IdempotencyKey(Base):
    __tablename__ = "idempotency_keys"

    id: Mapped[str] = mapped_column(UUID(as_uuid=False), primary_key=True, default=_uuid)
    merchant_id: Mapped[str] = mapped_column(String(100), nullable=False)
    key: Mapped[str] = mapped_column(String(255), nullable=False)
    request_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="PROCESSING")
    response_code: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    response_body: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    transaction_id: Mapped[Optional[str]] = mapped_column(UUID(as_uuid=False), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )
    expires_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False,
        server_default=text("NOW() + INTERVAL '24 hours'"),
    )

    __table_args__ = (
        UniqueConstraint("merchant_id", "key", name="uq_merchant_idempotency_key"),
        Index("idx_idempotency_expires", "expires_at"),
        CheckConstraint("status IN ('PROCESSING','COMPLETED','FAILED')",
                        name="chk_idempotency_status"),
    )


class ProcessedWebhookEvent(Base):
    __tablename__ = "processed_webhook_events"

    event_id: Mapped[str] = mapped_column(String(255), nullable=False, primary_key=True)
    gateway: Mapped[str] = mapped_column(String(50), nullable=False, primary_key=True)
    event_type: Mapped[str] = mapped_column(String(100), nullable=False)
    payload_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    transaction_id: Mapped[Optional[str]] = mapped_column(
        UUID(as_uuid=False), ForeignKey("transactions.id"), nullable=True
    )
    processed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    transaction: Mapped[Optional["Transaction"]] = relationship(
        "Transaction", back_populates="webhook_events"
    )
    __table_args__ = (Index("idx_webhook_events_transaction", "transaction_id"),)


class WebhookQueue(Base):
    __tablename__ = "webhook_queue"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    gateway: Mapped[str] = mapped_column(String(50), nullable=False)
    event_id: Mapped[str] = mapped_column(String(255), nullable=False)
    payload: Mapped[dict] = mapped_column(JSONB, nullable=False)
    raw_body: Mapped[str] = mapped_column(Text, nullable=False)
    signature: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="PENDING")
    retry_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    max_retries: Mapped[int] = mapped_column(Integer, nullable=False, default=3)
    next_retry_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    error_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    processed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)

    __table_args__ = (
        Index("idx_webhook_queue_status", "status", "created_at"),
        CheckConstraint("status IN ('PENDING','PROCESSING','COMPLETED','FAILED','DLQ')",
                        name="chk_webhook_queue_status"),
    )


class DeadLetterQueue(Base):
    __tablename__ = "dead_letter_queue"

    id: Mapped[str] = mapped_column(UUID(as_uuid=False), primary_key=True, default=_uuid)
    original_queue_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    gateway: Mapped[str] = mapped_column(String(50), nullable=False)
    event_id: Mapped[str] = mapped_column(String(255), nullable=False)
    payload: Mapped[dict] = mapped_column(JSONB, nullable=False)
    raw_body: Mapped[str] = mapped_column(Text, nullable=False)
    signature: Mapped[str] = mapped_column(Text, nullable=False)
    failure_reason: Mapped[str] = mapped_column(Text, nullable=False)
    retry_count: Mapped[int] = mapped_column(Integer, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    resolved_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    resolved_by: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)


class GatewayHealthMetric(Base):
    __tablename__ = "gateway_health_metrics"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    gateway: Mapped[str] = mapped_column(String(50), nullable=False)
    payment_method: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    window_start: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    window_end: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    total_requests: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    successful_requests: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    failed_requests: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    timeout_requests: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    avg_latency_ms: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    p50_latency_ms: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    p95_latency_ms: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    p99_latency_ms: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    recorded_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    __table_args__ = (
        Index("idx_health_metrics_gateway_time", "gateway", "window_start"),
    )


class GatewayConfig(Base):
    __tablename__ = "gateway_config"

    id: Mapped[str] = mapped_column(UUID(as_uuid=False), primary_key=True, default=_uuid)
    gateway_name: Mapped[str] = mapped_column(String(50), nullable=False, unique=True)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    base_url: Mapped[str] = mapped_column(String(500), nullable=False)
    supports_auth_capture: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    supports_partial_capture: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    supports_partial_refund: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    supported_payment_methods: Mapped[list] = mapped_column(JSONB, nullable=False, default=list)
    supported_currencies: Mapped[list] = mapped_column(JSONB, nullable=False, default=list)
    cb_failure_threshold: Mapped[int] = mapped_column(Integer, nullable=False, default=5)
    cb_success_threshold: Mapped[int] = mapped_column(Integer, nullable=False, default=2)
    cb_timeout_seconds: Mapped[int] = mapped_column(Integer, nullable=False, default=30)
    fee_percentage: Mapped[float] = mapped_column(Float, nullable=False, default=2.0)
    fee_fixed_paise: Mapped[int] = mapped_column(Integer, nullable=False, default=200)
    rate_limit_per_second: Mapped[int] = mapped_column(Integer, nullable=False, default=100)
    auth_timeout_seconds: Mapped[int] = mapped_column(Integer, nullable=False, default=30)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )


class RoutingConfig(Base):
    __tablename__ = "routing_config"

    id: Mapped[str] = mapped_column(UUID(as_uuid=False), primary_key=True, default=_uuid)
    config_key: Mapped[str] = mapped_column(String(100), nullable=False, unique=True)
    config_value: Mapped[dict] = mapped_column(JSONB, nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )
    updated_by: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)


class Refund(Base):
    __tablename__ = "refunds"

    id: Mapped[str] = mapped_column(UUID(as_uuid=False), primary_key=True, default=_uuid)
    transaction_id: Mapped[str] = mapped_column(
        UUID(as_uuid=False), ForeignKey("transactions.id"), nullable=False
    )
    amount_paise: Mapped[int] = mapped_column(BigInteger, nullable=False)
    currency: Mapped[str] = mapped_column(String(3), nullable=False, default="INR")
    state: Mapped[str] = mapped_column(String(50), nullable=False, default="INITIATED")
    gateway: Mapped[str] = mapped_column(String(50), nullable=False)
    gateway_refund_id: Mapped[Optional[str]] = mapped_column(String(255), nullable=True, unique=True)
    reason: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)
    idempotency_key: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )
    transaction: Mapped["Transaction"] = relationship("Transaction", back_populates="refunds")

    __table_args__ = (
        Index("idx_refunds_transaction", "transaction_id"),
        CheckConstraint("amount_paise > 0", name="chk_refund_amount_positive"),
    )


class ReconciliationRun(Base):
    __tablename__ = "reconciliation_runs"

    id: Mapped[str] = mapped_column(UUID(as_uuid=False), primary_key=True, default=_uuid)
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="RUNNING")
    transactions_checked: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    discrepancies_found: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    anomalies_found: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    triggered_by: Mapped[str] = mapped_column(String(100), nullable=False, default="scheduler")

    logs: Mapped[list["ReconciliationLog"]] = relationship("ReconciliationLog", back_populates="run")


class ReconciliationLog(Base):
    __tablename__ = "reconciliation_log"

    id: Mapped[str] = mapped_column(UUID(as_uuid=False), primary_key=True, default=_uuid)
    run_id: Mapped[str] = mapped_column(
        UUID(as_uuid=False), ForeignKey("reconciliation_runs.id"), nullable=False
    )
    transaction_id: Mapped[str] = mapped_column(
        UUID(as_uuid=False), ForeignKey("transactions.id"), nullable=False
    )
    gateway: Mapped[str] = mapped_column(String(50), nullable=False)
    internal_state: Mapped[str] = mapped_column(String(50), nullable=False)
    gateway_state: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    discrepancy_type: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    internal_amount_paise: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)
    gateway_amount_paise: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)
    is_anomaly: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    resolution: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    run: Mapped["ReconciliationRun"] = relationship("ReconciliationRun", back_populates="logs")

    __table_args__ = (
        Index("idx_recon_log_run", "run_id"),
        Index("idx_recon_log_anomaly", "is_anomaly"),
    )


class APIKey(Base):
    """Merchant API key store. Raw key is never stored — only SHA-256 hash."""
    __tablename__ = "api_keys"

    id: Mapped[str] = mapped_column(UUID(as_uuid=False), primary_key=True, default=_uuid)
    merchant_id: Mapped[str] = mapped_column(String(100), nullable=False)
    key_hash: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
    name: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    last_used_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)

    __table_args__ = (
        Index("idx_api_keys_hash", "key_hash"),
        Index("idx_api_keys_merchant", "merchant_id"),
    )
