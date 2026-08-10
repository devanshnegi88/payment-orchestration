"""
Domain-layer exceptions.

All exceptions are typed and carry enough context for structured logging
and correct HTTP status code mapping in the API layer.
"""
from typing import Optional, Any


class PaymentOrchestratorError(Exception):
    """Base exception for all orchestrator errors."""

    def __init__(self, message: str, context: Optional[dict] = None):
        super().__init__(message)
        self.message = message
        self.context = context or {}


# ── State Machine ──────────────────────────────────────────────────────────────

class InvalidStateTransitionError(PaymentOrchestratorError):
    """
    Raised when a state transition is not allowed by the FSM.
    This is a hard guard — no catch-and-ignore allowed.
    """

    def __init__(self, from_state: str, to_state: str, transaction_id: str, valid_transitions: list[str]):
        super().__init__(
            f"Invalid transition {from_state} → {to_state} for transaction {transaction_id}. "
            f"Valid transitions from {from_state}: {valid_transitions}",
            {
                "from_state": from_state,
                "to_state": to_state,
                "transaction_id": transaction_id,
                "valid_transitions": valid_transitions,
            },
        )
        self.from_state = from_state
        self.to_state = to_state
        self.transaction_id = transaction_id
        self.valid_transitions = valid_transitions


class TransactionNotFoundError(PaymentOrchestratorError):
    def __init__(self, transaction_id: str):
        super().__init__(f"Transaction {transaction_id} not found", {"transaction_id": transaction_id})
        self.transaction_id = transaction_id


class TransactionLockedError(PaymentOrchestratorError):
    """Transaction is being processed by another worker."""

    def __init__(self, transaction_id: str):
        super().__init__(f"Transaction {transaction_id} is currently locked", {"transaction_id": transaction_id})


# ── Idempotency ────────────────────────────────────────────────────────────────

class IdempotencyConflictError(PaymentOrchestratorError):
    """Duplicate request detected — another request with same key is in-flight."""

    def __init__(self, idempotency_key: str, status: str):
        super().__init__(
            f"Request with idempotency key {idempotency_key} already {status}",
            {"idempotency_key": idempotency_key, "status": status},
        )
        self.idempotency_key = idempotency_key
        self.status = status


class IdempotencyRequestMismatchError(PaymentOrchestratorError):
    """Same idempotency key used with different request payload."""

    def __init__(self, idempotency_key: str):
        super().__init__(
            f"Request body mismatch for idempotency key {idempotency_key}",
            {"idempotency_key": idempotency_key},
        )


# ── Gateway ────────────────────────────────────────────────────────────────────

class GatewayError(PaymentOrchestratorError):
    """Base class for all gateway errors."""

    def __init__(self, gateway: str, message: str, context: Optional[dict] = None, retryable: bool = False):
        super().__init__(message, {**(context or {}), "gateway": gateway})
        self.gateway = gateway
        self.retryable = retryable


class GatewayTimeoutError(GatewayError):
    def __init__(self, gateway: str, timeout_seconds: float):
        super().__init__(gateway, f"{gateway} timed out after {timeout_seconds}s", retryable=True)
        self.timeout_seconds = timeout_seconds


class GatewayServerError(GatewayError):
    """5xx errors from gateway."""

    def __init__(self, gateway: str, status_code: int, response_body: str = ""):
        super().__init__(
            gateway,
            f"{gateway} returned {status_code}",
            {"status_code": status_code, "response_body": response_body[:500]},
            retryable=True,
        )
        self.status_code = status_code


class GatewayDeclineError(GatewayError):
    """Hard decline — do NOT retry."""

    def __init__(self, gateway: str, decline_code: str, decline_reason: str):
        super().__init__(
            gateway,
            f"{gateway} declined: {decline_reason}",
            {"decline_code": decline_code, "decline_reason": decline_reason},
            retryable=False,
        )
        self.decline_code = decline_code
        self.decline_reason = decline_reason


class GatewayRateLimitError(GatewayError):
    """429 rate limit — retry after specified seconds."""

    def __init__(self, gateway: str, retry_after_seconds: float):
        super().__init__(
            gateway,
            f"{gateway} rate limit exceeded, retry after {retry_after_seconds}s",
            {"retry_after_seconds": retry_after_seconds},
            retryable=True,
        )
        self.retry_after_seconds = retry_after_seconds


class GatewayUnavailableError(GatewayError):
    """Circuit breaker is OPEN — gateway unavailable."""

    def __init__(self, gateway: str):
        super().__init__(gateway, f"{gateway} circuit breaker is OPEN", retryable=False)


class NoAvailableGatewayError(PaymentOrchestratorError):
    """All gateways for the payment method are unavailable."""

    def __init__(self, payment_method: str):
        super().__init__(
            f"No available gateways for payment method: {payment_method}",
            {"payment_method": payment_method},
        )


# ── Webhook ────────────────────────────────────────────────────────────────────

class WebhookSignatureError(PaymentOrchestratorError):
    """HMAC signature verification failed."""

    def __init__(self, gateway: str):
        super().__init__(f"Webhook signature verification failed for {gateway}", {"gateway": gateway})


class WebhookDuplicateError(PaymentOrchestratorError):
    """Event ID already processed — safe to acknowledge and discard."""

    def __init__(self, gateway: str, event_id: str):
        super().__init__(
            f"Duplicate webhook event {event_id} from {gateway}",
            {"gateway": gateway, "event_id": event_id},
        )


class WebhookAmountMismatchError(PaymentOrchestratorError):
    """Webhook amount does not match transaction amount — potential fraud."""

    def __init__(self, transaction_id: str, expected_paise: int, received_paise: int):
        super().__init__(
            f"Webhook amount mismatch for {transaction_id}: "
            f"expected {expected_paise} paise, got {received_paise} paise",
            {
                "transaction_id": transaction_id,
                "expected_paise": expected_paise,
                "received_paise": received_paise,
            },
        )


# ── Reconciliation ─────────────────────────────────────────────────────────────

class ReconciliationAnomalyError(PaymentOrchestratorError):
    """Critical discrepancy detected during reconciliation — requires human review."""

    def __init__(self, transaction_id: str, internal_state: str, gateway_state: str):
        super().__init__(
            f"CRITICAL: Transaction {transaction_id} internal={internal_state}, gateway={gateway_state}",
            {
                "transaction_id": transaction_id,
                "internal_state": internal_state,
                "gateway_state": gateway_state,
            },
        )
