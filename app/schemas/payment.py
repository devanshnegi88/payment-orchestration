"""
API Schemas — Pydantic v2 request/response models.

All monetary amounts are in paise (INR smallest unit).
API consumers receive amounts as paise; display conversion is client responsibility.
"""
from datetime import datetime
from typing import Optional, Any
from pydantic import BaseModel, Field, field_validator, model_validator
import uuid


# ── Common ─────────────────────────────────────────────────────────────────────

class ErrorDetail(BaseModel):
    code: str
    message: str
    details: Optional[dict] = None
    request_id: Optional[str] = None
    timestamp: datetime = Field(default_factory=datetime.utcnow)


class ErrorResponse(BaseModel):
    error: ErrorDetail


class HealthResponse(BaseModel):
    status: str
    version: str
    timestamp: datetime
    services: dict[str, str]


# ── Payment Schemas ────────────────────────────────────────────────────────────

class CreatePaymentRequest(BaseModel):
    merchant_order_id: str = Field(..., min_length=1, max_length=255, description="Merchant's unique order ID")
    amount_paise: int = Field(..., gt=0, description="Amount in paise (₹1 = 100 paise). Min ₹1.")
    currency: str = Field(default="INR", pattern="^[A-Z]{3}$")
    payment_method: str = Field(..., description="CARD | UPI | NETBANKING | WALLET | EMI")
    customer_id: Optional[str] = Field(None, max_length=255)
    customer_email: Optional[str] = Field(None, max_length=255)
    description: Optional[str] = Field(None, max_length=1000)
    metadata: Optional[dict[str, Any]] = None

    @field_validator("payment_method")
    @classmethod
    def validate_payment_method(cls, v: str) -> str:
        allowed = {"CARD", "UPI", "NETBANKING", "WALLET", "EMI"}
        if v.upper() not in allowed:
            raise ValueError(f"payment_method must be one of: {allowed}")
        return v.upper()

    @field_validator("amount_paise")
    @classmethod
    def validate_min_amount(cls, v: int) -> int:
        if v < 100:  # Minimum ₹1
            raise ValueError("Minimum payment amount is ₹1 (100 paise)")
        return v

    model_config = {"json_schema_extra": {
        "example": {
            "merchant_order_id": "order_123456",
            "amount_paise": 50000,
            "currency": "INR",
            "payment_method": "CARD",
            "customer_id": "cust_abc123",
            "description": "Premium Plan Subscription",
        }
    }}


class TransactionResponse(BaseModel):
    id: str
    merchant_id: str
    merchant_order_id: str
    amount_paise: int
    currency: str
    payment_method: str
    state: str
    gateway: Optional[str] = None
    gateway_reference: Optional[str] = None
    authorized_amount_paise: Optional[int] = None
    captured_amount_paise: Optional[int] = None
    refunded_amount_paise: Optional[int] = None
    retry_count: int
    trace_id: Optional[str] = None
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


class CaptureRequest(BaseModel):
    amount_paise: Optional[int] = Field(None, gt=0, description="Amount to capture. Defaults to full authorized amount.")
    idempotency_key: Optional[str] = Field(None, max_length=255)

    model_config = {"json_schema_extra": {
        "example": {"amount_paise": 50000}
    }}


class RefundRequest(BaseModel):
    amount_paise: Optional[int] = Field(None, gt=0, description="Amount to refund. Defaults to full captured amount.")
    reason: Optional[str] = Field(None, max_length=500)
    idempotency_key: Optional[str] = Field(None, max_length=255)

    model_config = {"json_schema_extra": {
        "example": {"amount_paise": 25000, "reason": "customer_request"}
    }}


class RefundResponse(BaseModel):
    id: str
    transaction_id: str
    amount_paise: int
    currency: str
    state: str
    gateway: str
    gateway_refund_id: Optional[str] = None
    reason: Optional[str] = None
    created_at: datetime

    model_config = {"from_attributes": True}


class StateLogEntry(BaseModel):
    id: str
    from_state: str
    to_state: str
    event: str
    gateway_reference: Optional[str] = None
    metadata: Optional[dict] = None
    created_at: datetime
    created_by: str

    model_config = {"from_attributes": True}


class TransactionTimeline(BaseModel):
    transaction_id: str
    current_state: str
    timeline: list[StateLogEntry]


# ── Gateway Schemas ────────────────────────────────────────────────────────────

class CircuitBreakerStatusResponse(BaseModel):
    state: str
    failure_count: int
    success_count: int
    last_state_change: float
    health_score: float


class GatewayHealthResponse(BaseModel):
    gateway: str
    circuit_breaker: CircuitBreakerStatusResponse
    metrics_15min: Optional[dict] = None
    is_active: bool


class GatewayMetricsResponse(BaseModel):
    gateway: str
    window_minutes: int
    total_requests: int
    successful_requests: int
    failed_requests: int
    success_rate: float
    p50_latency_ms: Optional[int] = None
    p95_latency_ms: Optional[int] = None
    p99_latency_ms: Optional[int] = None


class RoutingWeightsRequest(BaseModel):
    success: float = Field(..., ge=0.0, le=1.0)
    latency: float = Field(..., ge=0.0, le=1.0)
    cost: float = Field(..., ge=0.0, le=1.0)
    health: float = Field(..., ge=0.0, le=1.0)
    fit: float = Field(..., ge=0.0, le=1.0)

    @model_validator(mode="after")
    def validate_weights_sum(self) -> "RoutingWeightsRequest":
        total = self.success + self.latency + self.cost + self.health + self.fit
        if abs(total - 1.0) > 0.001:
            raise ValueError(f"Routing weights must sum to 1.0, got {total:.3f}")
        return self


class RoutingWeightsResponse(BaseModel):
    success: float
    latency: float
    cost: float
    health: float
    fit: float
    updated_at: Optional[datetime] = None


# ── Reconciliation Schemas ────────────────────────────────────────────────────

class ReconciliationRunResponse(BaseModel):
    id: str
    started_at: datetime
    completed_at: Optional[datetime] = None
    status: str
    transactions_checked: int
    discrepancies_found: int
    anomalies_found: int
    triggered_by: str

    model_config = {"from_attributes": True}


class ReconciliationLogEntry(BaseModel):
    id: str
    transaction_id: str
    gateway: str
    internal_state: str
    gateway_state: Optional[str] = None
    discrepancy_type: Optional[str] = None
    is_anomaly: bool
    created_at: datetime

    model_config = {"from_attributes": True}


# ── Analytics Schemas ──────────────────────────────────────────────────────────

class SuccessRateResponse(BaseModel):
    gateway: str
    window_hours: int
    total_transactions: int
    successful_transactions: int
    success_rate_pct: float


class VolumeResponse(BaseModel):
    date: str
    total_transactions: int
    total_amount_paise: int
    successful_transactions: int
    failed_transactions: int


class FailoverMetricsResponse(BaseModel):
    total_failovers: int
    avg_failover_time_ms: float
    failover_success_rate: float
    by_gateway: dict[str, int]


# ── DLQ Schemas ────────────────────────────────────────────────────────────────

class DLQItemResponse(BaseModel):
    id: str
    gateway: str
    event_id: str
    failure_reason: str
    retry_count: int
    created_at: datetime
    resolved_at: Optional[datetime] = None

    model_config = {"from_attributes": True}


class DLQReplayRequest(BaseModel):
    dlq_id: str
