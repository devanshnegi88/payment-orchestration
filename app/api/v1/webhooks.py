"""
Webhook Receiver Endpoints

POST /api/v1/webhooks/razorpay
POST /api/v1/webhooks/stripe
POST /api/v1/webhooks/payu
POST /api/v1/webhooks/upi

Each endpoint:
1. Extracts raw body (MUST be raw bytes for HMAC verification)
2. Extracts signature from gateway-specific header
3. Delegates to WebhookProcessor
4. Returns 200 immediately (even for duplicates — gateways may retry on non-200)
"""
from fastapi import APIRouter, Depends, Header, HTTPException, Request
from fastapi import status as http_status
from typing import Optional

from app.api.deps import get_webhook_processor
from app.services.webhook import WebhookProcessor
from app.domain.exceptions import (
    WebhookSignatureError,
    WebhookDuplicateError,
    WebhookAmountMismatchError,
)

router = APIRouter(prefix="/webhooks", tags=["Webhooks"])


async def _get_raw_body(request: Request) -> bytes:
    """Read raw body — critical for HMAC verification (never parse then re-serialize)."""
    return await request.body()


@router.post(
    "/razorpay",
    status_code=http_status.HTTP_200_OK,
    summary="Razorpay webhook receiver",
    description="Receives Razorpay payment events. Signature verified via HMAC-SHA256.",
)
async def razorpay_webhook(
    request: Request,
    x_razorpay_signature: str = Header(..., alias="X-Razorpay-Signature"),
    processor: WebhookProcessor = Depends(get_webhook_processor),
):
    raw_body = await _get_raw_body(request)
    return await _handle_webhook(
        processor=processor,
        gateway="razorpay",
        raw_body=raw_body,
        signature=x_razorpay_signature,
        headers=dict(request.headers),
    )


@router.post(
    "/stripe",
    status_code=http_status.HTTP_200_OK,
    summary="Stripe webhook receiver",
    description="Receives Stripe events. Signature verified via Stripe-Signature header (HMAC-SHA256 with timestamp).",
)
async def stripe_webhook(
    request: Request,
    stripe_signature: str = Header(..., alias="Stripe-Signature"),
    processor: WebhookProcessor = Depends(get_webhook_processor),
):
    raw_body = await _get_raw_body(request)
    return await _handle_webhook(
        processor=processor,
        gateway="stripe",
        raw_body=raw_body,
        signature=stripe_signature,
        headers=dict(request.headers),
    )


@router.post(
    "/payu",
    status_code=http_status.HTTP_200_OK,
    summary="PayU webhook receiver",
    description="Receives PayU payment callbacks. Signature verified via HMAC-SHA512.",
)
async def payu_webhook(
    request: Request,
    x_verify: Optional[str] = Header(None, alias="X-VERIFY"),
    processor: WebhookProcessor = Depends(get_webhook_processor),
):
    raw_body = await _get_raw_body(request)
    signature = x_verify or ""
    return await _handle_webhook(
        processor=processor,
        gateway="payu",
        raw_body=raw_body,
        signature=signature,
        headers=dict(request.headers),
    )


@router.post(
    "/upi",
    status_code=http_status.HTTP_200_OK,
    summary="UPI callback receiver",
    description="Receives UPI payment callbacks from NPCI/aggregator.",
)
async def upi_webhook(
    request: Request,
    x_verify: Optional[str] = Header(None, alias="X-VERIFY"),
    processor: WebhookProcessor = Depends(get_webhook_processor),
):
    raw_body = await _get_raw_body(request)
    signature = x_verify or ""
    return await _handle_webhook(
        processor=processor,
        gateway="upi",
        raw_body=raw_body,
        signature=signature,
        headers=dict(request.headers),
    )


async def _handle_webhook(
    processor: WebhookProcessor,
    gateway: str,
    raw_body: bytes,
    signature: str,
    headers: dict,
) -> dict:
    """
    Common webhook handling logic.

    Returns 200 for:
    - Successfully queued webhooks
    - Duplicate webhooks (idempotent acknowledgment)

    Returns 401 for invalid signatures (replay attack prevention — FS-10).
    Returns 500 for unexpected errors (causes gateway to retry — correct behavior).
    """
    try:
        result = await processor.ingest(
            gateway=gateway,
            raw_body=raw_body,
            signature=signature,
            headers=headers,
        )
        return {"status": "queued", **result}

    except WebhookSignatureError:
        # 401 — signature failed, do NOT acknowledge
        raise HTTPException(
            status_code=http_status.HTTP_401_UNAUTHORIZED,
            detail={"code": "WEBHOOK_SIGNATURE_INVALID", "message": "Signature verification failed"},
        )

    except WebhookDuplicateError as e:
        # 200 — acknowledge duplicates silently (gateway won't retry on 200)
        return {"status": "duplicate", "event_id": str(e)}

    except WebhookAmountMismatchError as e:
        # Log fraud attempt, return 400 to prevent gateway retry
        raise HTTPException(
            status_code=http_status.HTTP_400_BAD_REQUEST,
            detail={"code": "WEBHOOK_AMOUNT_MISMATCH", "message": "Amount mismatch detected"},
        )
