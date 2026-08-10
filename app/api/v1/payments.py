"""
Payment REST API — v1

Endpoints:
  POST   /api/v1/payments
  GET    /api/v1/payments/{id}
  GET    /api/v1/payments?merchant_order_id=
  POST   /api/v1/payments/{id}/capture
  POST   /api/v1/payments/{id}/void
  POST   /api/v1/payments/{id}/refund
  GET    /api/v1/payments/{id}/refunds
  GET    /api/v1/payments/{id}/timeline
"""
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, Header, Query, Request
from fastapi import status as http_status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from app.database import get_db
from app.api.deps import get_orchestrator, get_idempotency_service
from app.models.transaction import Transaction, TransactionStateLog, Refund
from app.schemas.payment import (
    CreatePaymentRequest,
    TransactionResponse,
    CaptureRequest,
    RefundRequest,
    RefundResponse,
    TransactionTimeline,
    StateLogEntry,
    ErrorResponse,
)
from app.domain.exceptions import (
    TransactionNotFoundError,
    InvalidStateTransitionError,
    IdempotencyConflictError,
    IdempotencyRequestMismatchError,
    NoAvailableGatewayError,
)
from app.api.middleware.auth import require_api_key
from app.services.orchestrator import PaymentOrchestrator

router = APIRouter(prefix="/payments", tags=["Payments"])


@router.post(
    "",
    response_model=TransactionResponse,
    status_code=http_status.HTTP_201_CREATED,
    summary="Initiate a new payment",
    responses={
        201: {"description": "Payment created and routing initiated"},
        409: {"description": "Duplicate request — idempotency conflict", "model": ErrorResponse},
        422: {"description": "Validation error"},
        503: {"description": "No gateways available"},
    },
)
async def create_payment(
    request: Request,
    body: CreatePaymentRequest,
    idempotency_key: str = Header(..., alias="Idempotency-Key", description="Client-generated UUID"),
    orchestrator: PaymentOrchestrator = Depends(get_orchestrator),
    _: None = Depends(require_api_key),
):
    """
    Initiate a new payment with intelligent gateway routing.

    **Idempotency**: Include the `Idempotency-Key` header with a unique UUID per payment intent.
    Retrying with the same key returns the original response without creating a duplicate charge.

    **Failover**: If the primary gateway fails, the orchestrator automatically routes to the
    next-best gateway within <2 seconds.
    """
    # Extract merchant from API key (simplified — in production, look up from key store)
    merchant_id = request.state.merchant_id

    try:
        txn = await orchestrator.create_payment(
            merchant_id=merchant_id,
            merchant_order_id=body.merchant_order_id,
            amount_paise=body.amount_paise,
            currency=body.currency,
            payment_method=body.payment_method,
            idempotency_key=idempotency_key,
            customer_id=body.customer_id,
            customer_email=body.customer_email,
            description=body.description,
            metadata=body.metadata,
        )
        return TransactionResponse.model_validate(txn)

    except IdempotencyConflictError as e:
        raise HTTPException(
            status_code=http_status.HTTP_409_CONFLICT,
            detail={"code": "IDEMPOTENCY_CONFLICT", "message": str(e)},
        )
    except IdempotencyRequestMismatchError:
        raise HTTPException(
            status_code=http_status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={"code": "IDEMPOTENCY_REQUEST_MISMATCH", "message": "Request body differs from original"},
        )
    except NoAvailableGatewayError as e:
        raise HTTPException(
            status_code=http_status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={"code": "NO_GATEWAY_AVAILABLE", "message": str(e)},
        )


@router.get(
    "/{transaction_id}",
    response_model=TransactionResponse,
    summary="Get payment details",
)
async def get_payment(
    transaction_id: str,
    db: AsyncSession = Depends(get_db),
    _: None = Depends(require_api_key),
):
    """Retrieve complete payment details by transaction ID."""
    result = await db.execute(
        select(Transaction).where(Transaction.id == transaction_id)
    )
    txn = result.scalar_one_or_none()
    if txn is None:
        raise HTTPException(
            status_code=http_status.HTTP_404_NOT_FOUND,
            detail={"code": "TRANSACTION_NOT_FOUND", "message": f"Transaction {transaction_id} not found"},
        )
    return TransactionResponse.model_validate(txn)


@router.get(
    "",
    response_model=TransactionResponse,
    summary="Find payment by merchant order ID",
)
async def find_payment_by_order(
    request: Request,
    merchant_order_id: str = Query(..., description="Merchant's order ID"),
    db: AsyncSession = Depends(get_db),
    _: None = Depends(require_api_key),
):
    """Retrieve payment by merchant order ID."""
    merchant_id = request.state.merchant_id
    result = await db.execute(
        select(Transaction).where(
            Transaction.merchant_id == merchant_id,
            Transaction.merchant_order_id == merchant_order_id,
        )
    )
    txn = result.scalar_one_or_none()
    if txn is None:
        raise HTTPException(
            status_code=http_status.HTTP_404_NOT_FOUND,
            detail={"code": "TRANSACTION_NOT_FOUND", "message": f"Order {merchant_order_id} not found"},
        )
    return TransactionResponse.model_validate(txn)


@router.post(
    "/{transaction_id}/capture",
    response_model=TransactionResponse,
    summary="Capture an authorised payment",
    responses={
        409: {"description": "Invalid state for capture"},
    },
)
async def capture_payment(
    transaction_id: str,
    body: CaptureRequest,
    orchestrator: PaymentOrchestrator = Depends(get_orchestrator),
    _: None = Depends(require_api_key),
):
    """
    Capture a previously authorised payment.
    Supports partial capture (FS-05): capture less than authorised amount.
    """
    try:
        txn = await orchestrator.capture_payment(
            transaction_id=transaction_id,
            amount_paise=body.amount_paise,
            idempotency_key=body.idempotency_key,
        )
        return TransactionResponse.model_validate(txn)

    except TransactionNotFoundError:
        raise HTTPException(status_code=404, detail={"code": "TRANSACTION_NOT_FOUND"})
    except InvalidStateTransitionError as e:
        raise HTTPException(
            status_code=http_status.HTTP_409_CONFLICT,
            detail={
                "code": "INVALID_STATE_FOR_CAPTURE",
                "message": str(e),
                "valid_transitions": e.valid_transitions,
            },
        )


@router.post(
    "/{transaction_id}/void",
    response_model=TransactionResponse,
    summary="Void an authorised payment",
)
async def void_payment(
    transaction_id: str,
    orchestrator: PaymentOrchestrator = Depends(get_orchestrator),
    _: None = Depends(require_api_key),
):
    """Void an authorised payment before capture. Releases the held funds."""
    try:
        txn = await orchestrator.void_payment(transaction_id)
        return TransactionResponse.model_validate(txn)
    except TransactionNotFoundError:
        raise HTTPException(status_code=404, detail={"code": "TRANSACTION_NOT_FOUND"})
    except InvalidStateTransitionError as e:
        raise HTTPException(status_code=409, detail={"code": "INVALID_STATE_FOR_VOID", "message": str(e)})


@router.post(
    "/{transaction_id}/refund",
    response_model=RefundResponse,
    status_code=http_status.HTTP_201_CREATED,
    summary="Initiate a refund",
)
async def refund_payment(
    transaction_id: str,
    body: RefundRequest,
    orchestrator: PaymentOrchestrator = Depends(get_orchestrator),
    _: None = Depends(require_api_key),
):
    """
    Initiate a full or partial refund.
    Valid from: CAPTURED, PARTIALLY_CAPTURED, SETTLED, PARTIALLY_REFUNDED states (FS-08).
    """
    try:
        refund = await orchestrator.refund_payment(
            transaction_id=transaction_id,
            amount_paise=body.amount_paise,
            reason=body.reason,
            idempotency_key=body.idempotency_key,
        )
        return RefundResponse.model_validate(refund)
    except TransactionNotFoundError:
        raise HTTPException(status_code=404, detail={"code": "TRANSACTION_NOT_FOUND"})
    except InvalidStateTransitionError as e:
        raise HTTPException(status_code=409, detail={"code": "INVALID_STATE_FOR_REFUND", "message": str(e)})
    except ValueError as e:
        raise HTTPException(status_code=422, detail={"code": "REFUND_AMOUNT_EXCEEDS_CAPTURED", "message": str(e)})


@router.get(
    "/{transaction_id}/refunds",
    response_model=list[RefundResponse],
    summary="List all refunds for a payment",
)
async def list_refunds(
    transaction_id: str,
    db: AsyncSession = Depends(get_db),
    _: None = Depends(require_api_key),
):
    """List all refunds associated with a payment."""
    result = await db.execute(
        select(Refund).where(Refund.transaction_id == transaction_id)
    )
    return [RefundResponse.model_validate(r) for r in result.scalars().all()]


@router.get(
    "/{transaction_id}/timeline",
    response_model=TransactionTimeline,
    summary="Get complete state transition history",
)
async def get_timeline(
    transaction_id: str,
    db: AsyncSession = Depends(get_db),
    _: None = Depends(require_api_key),
):
    """
    Return the complete immutable audit trail for a transaction.
    Shows every state transition, event trigger, gateway response (PII redacted),
    and timestamp. Required for dispute resolution.
    """
    txn_result = await db.execute(
        select(Transaction).where(Transaction.id == transaction_id)
    )
    txn = txn_result.scalar_one_or_none()
    if txn is None:
        raise HTTPException(status_code=404, detail={"code": "TRANSACTION_NOT_FOUND"})

    logs_result = await db.execute(
        select(TransactionStateLog)
        .where(TransactionStateLog.transaction_id == transaction_id)
        .order_by(TransactionStateLog.created_at)
    )
    logs = logs_result.scalars().all()

    return TransactionTimeline(
        transaction_id=transaction_id,
        current_state=txn.state.value,
        timeline=[StateLogEntry.model_validate(log) for log in logs],
    )
