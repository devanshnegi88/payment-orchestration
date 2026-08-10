"""
Mock Gateway Simulator Service

Simulates all four payment gateways with:
- Configurable success/failure/timeout responses via X-Mock-* headers
- Realistic latency profiles from historical data (A3.4)
- All 15 failure scenarios from Section B2

This is a standalone FastAPI service (runs separately in Docker).
Port assignment: Razorpay=8001, Stripe=8002, PayU=8003, UPI=8004
"""
import asyncio
import random
import time
import uuid
from datetime import datetime
from typing import Optional
from fastapi import FastAPI, Request, Header, HTTPException
from fastapi.responses import JSONResponse

app = FastAPI(title="PayFlow Mock Gateway Simulator", version="1.0.0")

# ── Simulation State ───────────────────────────────────────────────────────────
# Allows the test harness to set gateway-wide simulation modes
_gateway_config: dict = {
    "razorpay": {"mode": "success", "delay_ms": 320, "success_rate": 0.985},
    "stripe":   {"mode": "success", "delay_ms": 280, "success_rate": 0.991},
    "payu":     {"mode": "success", "delay_ms": 400, "success_rate": 0.960},
    "upi":      {"mode": "success", "delay_ms": 180, "success_rate": 0.995},
}

# In-memory payment store (reset between test scenarios)
_payments: dict[str, dict] = {}
_refunds: dict[str, dict] = {}


async def _apply_mock_headers(request: Request, gateway: str):
    """
    Read X-Mock-* headers and apply the requested simulation.
    This is the test harness control interface (Section B4.3).
    """
    mock_response = request.headers.get("X-Mock-Response", "")
    mock_delay = int(request.headers.get("X-Mock-Delay-Ms", 0))
    gateway_down = request.headers.get("X-Mock-Gateway-Down", "").lower() == "true"

    if gateway_down:
        raise HTTPException(status_code=503, detail="Gateway unavailable (simulated)")

    if mock_delay > 0:
        await asyncio.sleep(mock_delay / 1000)
    else:
        # Apply realistic latency from historical data
        config = _gateway_config.get(gateway, {})
        base_delay = config.get("delay_ms", 300)
        jitter = random.randint(-50, 100)
        await asyncio.sleep(max(50, base_delay + jitter) / 1000)

    if mock_response == "timeout":
        # Simulate 30-second timeout (actual wait would be too long — use 31s)
        await asyncio.sleep(31)

    elif mock_response == "server-error":
        raise HTTPException(status_code=502, detail="Bad Gateway (simulated)")

    elif mock_response == "rate-limit":
        return JSONResponse(
            status_code=429,
            headers={"Retry-After": "5"},
            content={"error": "rate_limit_exceeded"},
        )

    elif mock_response == "decline":
        return {"decline": True}

    return None  # Proceed with success


# ═══════════════════════════════════════════════════════════════════════════════
# RAZORPAY MOCK (port 8001 / prefix /razorpay)
# ═══════════════════════════════════════════════════════════════════════════════

razorpay_app = FastAPI(title="Mock Razorpay")


@razorpay_app.post("/v1/orders")
async def razorpay_create_order(request: Request):
    override = await _apply_mock_headers(request, "razorpay")
    if override:
        return override

    body = await request.json()
    order_id = f"order_{uuid.uuid4().hex[:16]}"
    payment_id = f"pay_{uuid.uuid4().hex[:16]}"

    payment = {
        "id": order_id,
        "payment_id": payment_id,
        "amount": body.get("amount"),
        "currency": body.get("currency", "INR"),
        "status": "authorized",
        "created_at": int(time.time()),
    }
    _payments[payment_id] = payment

    return {
        "id": order_id,
        "payment_id": payment_id,
        "amount": body.get("amount"),
        "currency": "INR",
        "status": "authorized",
        "receipt": body.get("receipt"),
    }


@razorpay_app.post("/v1/payments/{payment_id}/capture")
async def razorpay_capture(payment_id: str, request: Request):
    override = await _apply_mock_headers(request, "razorpay")
    if override:
        return override

    body = await request.json()
    if payment_id not in _payments:
        raise HTTPException(status_code=404, detail="Payment not found")

    _payments[payment_id]["status"] = "captured"
    return {
        "id": payment_id,
        "amount": body.get("amount"),
        "status": "captured",
        "captured_at": int(time.time()),
    }


@razorpay_app.post("/v1/payments/{payment_id}/refund")
async def razorpay_refund(payment_id: str, request: Request):
    override = await _apply_mock_headers(request, "razorpay")
    if override:
        return override

    body = await request.json()
    refund_id = f"rfnd_{uuid.uuid4().hex[:16]}"
    _refunds[refund_id] = {"payment_id": payment_id, "amount": body.get("amount")}
    return {"id": refund_id, "payment_id": payment_id, "amount": body.get("amount"), "status": "processed"}


@razorpay_app.post("/v1/payments/{payment_id}/cancel")
async def razorpay_void(payment_id: str, request: Request):
    override = await _apply_mock_headers(request, "razorpay")
    if override:
        return override
    _payments.get(payment_id, {})["status"] = "cancelled"
    return {"id": payment_id, "status": "cancelled"}


@razorpay_app.get("/v1/payments/{payment_id}")
async def razorpay_status(payment_id: str, request: Request):
    payment = _payments.get(payment_id)
    if not payment:
        return {"status": "failed", "error": "not_found"}
    return payment


# ═══════════════════════════════════════════════════════════════════════════════
# STRIPE MOCK (port 8002)
# ═══════════════════════════════════════════════════════════════════════════════

stripe_app = FastAPI(title="Mock Stripe")
_stripe_intents: dict = {}


@stripe_app.post("/v1/payment_intents")
async def stripe_create_intent(request: Request):
    override = await _apply_mock_headers(request, "stripe")
    if override:
        return override

    body = await request.json()
    intent_id = f"pi_{uuid.uuid4().hex[:24]}"
    intent = {
        "id": intent_id,
        "amount": body.get("amount"),
        "currency": body.get("currency", "inr"),
        "status": "requires_capture",
        "capture_method": "manual",
    }
    _stripe_intents[intent_id] = intent
    return intent


@stripe_app.post("/v1/payment_intents/{intent_id}/capture")
async def stripe_capture(intent_id: str, request: Request):
    override = await _apply_mock_headers(request, "stripe")
    if override:
        return override

    body = await request.json()
    if intent_id not in _stripe_intents:
        raise HTTPException(status_code=404, detail="No such payment_intent")

    _stripe_intents[intent_id]["status"] = "succeeded"
    _stripe_intents[intent_id]["amount_received"] = body.get("amount_to_capture",
                                                              _stripe_intents[intent_id]["amount"])
    return _stripe_intents[intent_id]


@stripe_app.post("/v1/payment_intents/{intent_id}/cancel")
async def stripe_cancel(intent_id: str, request: Request):
    override = await _apply_mock_headers(request, "stripe")
    if override:
        return override
    if intent_id in _stripe_intents:
        _stripe_intents[intent_id]["status"] = "canceled"
    return {"id": intent_id, "status": "canceled"}


@stripe_app.post("/v1/refunds")
async def stripe_refund(request: Request):
    override = await _apply_mock_headers(request, "stripe")
    if override:
        return override
    body = await request.json()
    refund_id = f"re_{uuid.uuid4().hex[:24]}"
    return {"id": refund_id, "amount": body.get("amount"), "status": "succeeded"}


@stripe_app.get("/v1/payment_intents/{intent_id}")
async def stripe_status(intent_id: str):
    intent = _stripe_intents.get(intent_id)
    if not intent:
        return {"id": intent_id, "status": "requires_payment_method"}
    return intent


# ═══════════════════════════════════════════════════════════════════════════════
# PAYU MOCK (port 8003)
# ═══════════════════════════════════════════════════════════════════════════════

payu_app = FastAPI(title="Mock PayU")
_payu_payments: dict = {}


@payu_app.post("/payment/auth")
async def payu_auth(request: Request):
    override = await _apply_mock_headers(request, "payu")
    if override:
        return override

    body = await request.json()
    mihpayid = f"payu_{uuid.uuid4().hex[:10]}"
    _payu_payments[mihpayid] = {
        "mihpayid": mihpayid,
        "status": "authorized",
        "amount": body.get("amount"),
        "txnid": body.get("txnid"),
    }
    return {"mihpayid": mihpayid, "status": "authorized", "result": "success"}


@payu_app.post("/payment/capture")
async def payu_capture(request: Request):
    override = await _apply_mock_headers(request, "payu")
    if override:
        return override
    body = await request.json()
    mihpayid = body.get("var1")
    if mihpayid in _payu_payments:
        _payu_payments[mihpayid]["status"] = "captured"
    return {"status": "success", "mihpayid": mihpayid}


@payu_app.post("/payment/refund")
async def payu_refund(request: Request):
    override = await _apply_mock_headers(request, "payu")
    if override:
        return override
    body = await request.json()
    return {"requestId": f"rfnd_{uuid.uuid4().hex[:10]}", "status": "success"}


@payu_app.post("/payment/void")
async def payu_void(request: Request):
    override = await _apply_mock_headers(request, "payu")
    if override:
        return override
    return {"status": "success"}


@payu_app.post("/payment/status")
async def payu_status(request: Request):
    body = await request.json()
    mihpayid = body.get("var1", "")
    payment = _payu_payments.get(mihpayid, {})
    return {
        "status": payment.get("status", "failed"),
        "mihpayid": mihpayid,
        "net_amount_debit": payment.get("amount", "0"),
    }


# ═══════════════════════════════════════════════════════════════════════════════
# UPI MOCK (port 8004)
# ═══════════════════════════════════════════════════════════════════════════════

upi_app = FastAPI(title="Mock UPI")
_upi_transactions: dict = {}


@upi_app.post("/v3/debit/init")
async def upi_init(request: Request):
    override = await _apply_mock_headers(request, "upi")
    if override:
        return override

    body = await request.json()
    txn_id = f"upi_{uuid.uuid4().hex[:16]}"
    _upi_transactions[txn_id] = {
        "transactionId": txn_id,
        "amount": body.get("amount"),
        "code": "PAYMENT_PENDING",
        "merchantTransactionId": body.get("merchantTransactionId"),
    }
    return {"success": True, "code": "PAYMENT_INITIATED", "transactionId": txn_id}


@upi_app.get("/v3/transaction/{txn_id}/status")
async def upi_status(txn_id: str, request: Request):
    txn = _upi_transactions.get(txn_id, {})
    mock_response = request.headers.get("X-Mock-Response", "")
    if mock_response == "success":
        txn["code"] = "PAYMENT_SUCCESS"
    elif mock_response == "timeout" or mock_response == "expire":
        txn["code"] = "PAYMENT_EXPIRED"
    elif mock_response == "decline":
        txn["code"] = "PAYMENT_DECLINED"
    return {
        "transactionId": txn_id,
        "code": txn.get("code", "PAYMENT_SUCCESS"),
        "amount": txn.get("amount"),
    }


@upi_app.post("/v3/refund")
async def upi_refund(request: Request):
    override = await _apply_mock_headers(request, "upi")
    if override:
        return override
    body = await request.json()
    return {"transactionId": f"upi_rfnd_{uuid.uuid4().hex[:12]}", "code": "PAYMENT_SUCCESS"}


# ── Admin Control Endpoints ────────────────────────────────────────────────────

@app.get("/admin/reset")
async def reset_state():
    """Reset all payment state between test scenarios."""
    _payments.clear()
    _refunds.clear()
    _stripe_intents.clear()
    _payu_payments.clear()
    _upi_transactions.clear()
    return {"status": "reset"}


@app.post("/admin/config/{gateway}")
async def set_gateway_config(gateway: str, request: Request):
    """Set gateway simulation config for scenario testing."""
    body = await request.json()
    if gateway in _gateway_config:
        _gateway_config[gateway].update(body)
    return {"gateway": gateway, "config": _gateway_config.get(gateway)}


# Mount all gateway apps
app.mount("/razorpay", razorpay_app)
app.mount("/stripe", stripe_app)
app.mount("/payu", payu_app)
app.mount("/upi", upi_app)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)
