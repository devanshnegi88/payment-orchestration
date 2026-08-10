"""
Real Payment Gateway Adapters — actual API calls, no mocks.
Credentials loaded from environment variables.
"""
import asyncio
import base64
import hashlib
import hmac
import json
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Optional

import httpx
import structlog

from app.config import settings
from app.domain.exceptions import (
    GatewayDeclineError,
    GatewayRateLimitError,
    GatewayServerError,
    GatewayTimeoutError,
)

logger = structlog.get_logger(__name__)


class GatewayAuthStatus(str, Enum):
    AUTHORISED = "authorised"
    DECLINED = "declined"
    TIMEOUT = "timeout"
    EXPIRED = "expired"
    PENDING = "pending"


class GatewayCaptureStatus(str, Enum):
    CAPTURED = "captured"
    PARTIAL = "partial"
    FAILED = "failed"


class GatewayRefundStatus(str, Enum):
    REFUNDED = "refunded"
    PARTIAL = "partial"
    FAILED = "failed"


@dataclass
class AuthResponse:
    status: GatewayAuthStatus
    gateway_reference: str
    gateway_order_id: Optional[str] = None
    authorized_amount_paise: Optional[int] = None
    raw_response: Optional[dict] = None
    latency_ms: int = 0


@dataclass
class CaptureResponse:
    status: GatewayCaptureStatus
    gateway_reference: str
    captured_amount_paise: int
    raw_response: Optional[dict] = None
    latency_ms: int = 0


@dataclass
class RefundResponse:
    status: GatewayRefundStatus
    gateway_refund_id: str
    refunded_amount_paise: int
    raw_response: Optional[dict] = None
    latency_ms: int = 0


@dataclass
class VoidResponse:
    success: bool
    gateway_reference: str
    raw_response: Optional[dict] = None


@dataclass
class PaymentStatus:
    gateway_state: str
    amount_paise: Optional[int] = None
    gateway_reference: Optional[str] = None
    raw_response: Optional[dict] = None


_PII = {"card_number", "cvv", "expiry", "account_number", "email", "phone",
        "vpa", "upi_id", "bank_account", "ifsc", "card", "number"}


def _redact(data: dict) -> dict:
    if not isinstance(data, dict):
        return data
    return {
        k: "[REDACTED]" if k.lower() in _PII
        else (_redact(v) if isinstance(v, dict) else v)
        for k, v in data.items()
        if k != "_latency_ms"
    }


class PaymentGateway(ABC):
    @property
    @abstractmethod
    def name(self) -> str: ...

    @abstractmethod
    async def authorise(self, amount_paise: int, currency: str, payment_method: str,
                        merchant_order_id: str, idempotency_key: str,
                        customer_id: Optional[str] = None,
                        metadata: Optional[dict] = None) -> AuthResponse: ...

    @abstractmethod
    async def capture(self, gateway_reference: str, amount_paise: int,
                      idempotency_key: str) -> CaptureResponse: ...

    @abstractmethod
    async def refund(self, gateway_reference: str, amount_paise: int,
                     refund_idempotency_key: str,
                     reason: Optional[str] = None) -> RefundResponse: ...

    @abstractmethod
    async def void(self, gateway_reference: str) -> VoidResponse: ...

    @abstractmethod
    async def get_payment_status(self, gateway_reference: str) -> PaymentStatus: ...

    @abstractmethod
    def verify_webhook_signature(self, raw_body: bytes, signature: str) -> bool: ...


class GatewayBase(PaymentGateway):
    """Shared HTTP client with timeout, error classification, and latency tracking."""

    def __init__(self, base_url: str, connect_timeout: float = 5.0, read_timeout: float = 30.0):
        self.base_url = base_url.rstrip("/")
        self._timeout = httpx.Timeout(connect=connect_timeout, read=read_timeout,
                                       write=10.0, pool=5.0)
        self._client: Optional[httpx.AsyncClient] = None

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                base_url=self.base_url,
                timeout=self._timeout,
                http2=True,
            )
        return self._client

    async def _request(self, method: str, path: str, *,
                       json_body: Optional[dict] = None,
                       data: Optional[dict] = None,
                       headers: Optional[dict] = None) -> tuple[dict, int]:
        client = await self._get_client()
        t0 = time.monotonic()
        try:
            resp = await client.request(method, path, json=json_body, data=data, headers=headers)
        except httpx.ConnectTimeout:
            raise GatewayTimeoutError(self.name, self._timeout.connect)
        except httpx.ReadTimeout:
            raise GatewayTimeoutError(self.name, self._timeout.read)
        except httpx.TimeoutException:
            raise GatewayTimeoutError(self.name, 30.0)
        except httpx.RequestError as e:
            raise GatewayServerError(self.name, 503, str(e))

        latency_ms = int((time.monotonic() - t0) * 1000)

        if resp.status_code == 429:
            retry_after = float(resp.headers.get("Retry-After", "5"))
            raise GatewayRateLimitError(self.name, retry_after)

        try:
            body = resp.json()
        except Exception:
            body = {"_raw": resp.text[:500]}

        body["_latency_ms"] = latency_ms

        if resp.status_code >= 500:
            raise GatewayServerError(self.name, resp.status_code, resp.text[:300])

        if resp.status_code in (400, 402):
            self._raise_decline(body)

        return body, latency_ms

    def _raise_decline(self, body: dict) -> None:
        raise GatewayDeclineError(self.name, "DECLINED", str(body)[:200])

    async def close(self) -> None:
        if self._client and not self._client.is_closed:
            await self._client.aclose()


# ── Razorpay ──────────────────────────────────────────────────────────────────

class RazorpayAdapter(GatewayBase):
    """
    Razorpay REST API. Two-phase: Create Order → Payment (via webhook) → Capture.
    Auth: HTTP Basic (key_id:key_secret)
    Docs: https://razorpay.com/docs/api/
    """
    BASE_URL = "https://api.razorpay.com"

    def __init__(self):
        super().__init__(self.BASE_URL, connect_timeout=5.0, read_timeout=30.0)
        creds = base64.b64encode(
            f"{settings.RAZORPAY_KEY_ID}:{settings.RAZORPAY_KEY_SECRET}".encode()
        ).decode()
        self._auth = {"Authorization": f"Basic {creds}"}
        self._webhook_secret = settings.RAZORPAY_WEBHOOK_SECRET

    @property
    def name(self) -> str:
        return "razorpay"

    async def authorise(self, amount_paise: int, currency: str, payment_method: str,
                        merchant_order_id: str, idempotency_key: str,
                        customer_id: Optional[str] = None,
                        metadata: Optional[dict] = None) -> AuthResponse:
        body = {
            "amount": amount_paise,
            "currency": currency,
            "receipt": merchant_order_id[:40],
            "payment_capture": 0,
            "notes": {"merchant_order_id": merchant_order_id,
                      "idempotency_key": idempotency_key},
        }
        resp, latency = await self._request(
            "POST", "/v1/orders", json_body=body,
            headers={**self._auth, "X-Razorpay-Idempotency-Key": idempotency_key},
        )
        return AuthResponse(
            status=GatewayAuthStatus.AUTHORISED,
            gateway_reference=resp.get("id", ""),
            gateway_order_id=resp.get("id", ""),
            authorized_amount_paise=resp.get("amount", amount_paise),
            raw_response=_redact(resp),
            latency_ms=latency,
        )

    async def capture(self, gateway_reference: str, amount_paise: int,
                      idempotency_key: str) -> CaptureResponse:
        resp, latency = await self._request(
            "POST", f"/v1/payments/{gateway_reference}/capture",
            json_body={"amount": amount_paise, "currency": "INR"},
            headers={**self._auth, "X-Razorpay-Idempotency-Key": idempotency_key},
        )
        return CaptureResponse(
            status=GatewayCaptureStatus.CAPTURED,
            gateway_reference=gateway_reference,
            captured_amount_paise=resp.get("amount", amount_paise),
            raw_response=_redact(resp),
            latency_ms=latency,
        )

    async def refund(self, gateway_reference: str, amount_paise: int,
                     refund_idempotency_key: str, reason: Optional[str] = None) -> RefundResponse:
        resp, latency = await self._request(
            "POST", f"/v1/payments/{gateway_reference}/refund",
            json_body={"amount": amount_paise, "speed": "normal",
                       "notes": {"reason": reason or "merchant_initiated"}},
            headers={**self._auth, "X-Razorpay-Idempotency-Key": refund_idempotency_key},
        )
        return RefundResponse(
            status=GatewayRefundStatus.REFUNDED,
            gateway_refund_id=resp.get("id", ""),
            refunded_amount_paise=resp.get("amount", amount_paise),
            raw_response=_redact(resp),
            latency_ms=latency,
        )

    async def void(self, gateway_reference: str) -> VoidResponse:
        resp, _ = await self._request(
            "POST", f"/v1/payments/{gateway_reference}/cancel",
            json_body={}, headers=self._auth,
        )
        return VoidResponse(success=True, gateway_reference=gateway_reference,
                            raw_response=_redact(resp))

    async def get_payment_status(self, gateway_reference: str) -> PaymentStatus:
        resp, _ = await self._request("GET", f"/v1/payments/{gateway_reference}",
                                       headers=self._auth)
        state_map = {"captured": "CAPTURED", "authorized": "AUTHORISED",
                     "failed": "FAILED", "refunded": "REFUNDED", "created": "AUTH_INITIATED"}
        return PaymentStatus(
            gateway_state=state_map.get(resp.get("status", ""), "UNKNOWN"),
            amount_paise=resp.get("amount"),
            gateway_reference=gateway_reference,
            raw_response=_redact(resp),
        )

    def verify_webhook_signature(self, raw_body: bytes, signature: str) -> bool:
        expected = hmac.new(self._webhook_secret.encode(), raw_body,
                             digestmod=hashlib.sha256).hexdigest()
        return hmac.compare_digest(expected, signature)

    def _raise_decline(self, body: dict) -> None:
        err = body.get("error", {})
        raise GatewayDeclineError(self.name, err.get("code", "DECLINED"),
                                  err.get("description", "Payment declined"))


# ── Stripe ────────────────────────────────────────────────────────────────────

class StripeAdapter(GatewayBase):
    """
    Stripe Payment Intents API. capture_method=manual for two-phase flow.
    Auth: Bearer secret key
    Docs: https://stripe.com/docs/api/payment_intents
    """
    BASE_URL = "https://api.stripe.com"

    def __init__(self):
        super().__init__(self.BASE_URL, connect_timeout=5.0, read_timeout=30.0)
        self._auth = {"Authorization": f"Bearer {settings.STRIPE_SECRET_KEY}"}
        self._webhook_secret = settings.STRIPE_WEBHOOK_SECRET

    @property
    def name(self) -> str:
        return "stripe"

    async def authorise(self, amount_paise: int, currency: str, payment_method: str,
                        merchant_order_id: str, idempotency_key: str,
                        customer_id: Optional[str] = None,
                        metadata: Optional[dict] = None) -> AuthResponse:
        # Stripe accepts form-encoded data for most endpoints
        data: dict = {
            "amount": str(amount_paise),
            "currency": currency.lower(),
            "capture_method": "manual",
            "metadata[merchant_order_id]": merchant_order_id,
        }
        if customer_id:
            data["customer"] = customer_id
        resp, latency = await self._request(
            "POST", "/v1/payment_intents", data=data,
            headers={**self._auth, "Idempotency-Key": idempotency_key},
        )
        return AuthResponse(
            status=GatewayAuthStatus.AUTHORISED,
            gateway_reference=resp.get("id", ""),
            authorized_amount_paise=resp.get("amount", amount_paise),
            raw_response=_redact(resp),
            latency_ms=latency,
        )

    async def capture(self, gateway_reference: str, amount_paise: int,
                      idempotency_key: str) -> CaptureResponse:
        resp, latency = await self._request(
            "POST", f"/v1/payment_intents/{gateway_reference}/capture",
            data={"amount_to_capture": str(amount_paise)},
            headers={**self._auth, "Idempotency-Key": idempotency_key},
        )
        captured = resp.get("amount_received") or resp.get("amount", amount_paise)
        return CaptureResponse(
            status=GatewayCaptureStatus.CAPTURED,
            gateway_reference=gateway_reference,
            captured_amount_paise=captured,
            raw_response=_redact(resp),
            latency_ms=latency,
        )

    async def refund(self, gateway_reference: str, amount_paise: int,
                     refund_idempotency_key: str, reason: Optional[str] = None) -> RefundResponse:
        valid_reasons = {"duplicate", "fraudulent", "requested_by_customer"}
        data = {
            "payment_intent": gateway_reference,
            "amount": str(amount_paise),
            "reason": reason if reason in valid_reasons else "requested_by_customer",
        }
        resp, latency = await self._request(
            "POST", "/v1/refunds", data=data,
            headers={**self._auth, "Idempotency-Key": refund_idempotency_key},
        )
        return RefundResponse(
            status=GatewayRefundStatus.REFUNDED,
            gateway_refund_id=resp.get("id", ""),
            refunded_amount_paise=resp.get("amount", amount_paise),
            raw_response=_redact(resp),
            latency_ms=latency,
        )

    async def void(self, gateway_reference: str) -> VoidResponse:
        resp, _ = await self._request(
            "POST", f"/v1/payment_intents/{gateway_reference}/cancel",
            data={"cancellation_reason": "abandoned"},
            headers=self._auth,
        )
        return VoidResponse(success=True, gateway_reference=gateway_reference,
                            raw_response=_redact(resp))

    async def get_payment_status(self, gateway_reference: str) -> PaymentStatus:
        resp, _ = await self._request("GET", f"/v1/payment_intents/{gateway_reference}",
                                       headers=self._auth)
        state_map = {
            "succeeded": "CAPTURED", "requires_capture": "AUTHORISED",
            "requires_payment_method": "FAILED", "canceled": "VOIDED",
            "processing": "CAPTURE_INITIATED",
            "requires_confirmation": "AUTH_INITIATED",
            "requires_action": "AUTH_INITIATED",
        }
        return PaymentStatus(
            gateway_state=state_map.get(resp.get("status", ""), "UNKNOWN"),
            amount_paise=resp.get("amount"),
            gateway_reference=gateway_reference,
            raw_response=_redact(resp),
        )

    def verify_webhook_signature(self, raw_body: bytes, signature: str) -> bool:
        """
        Stripe-Signature header: t=<ts>,v1=<sig>
        Signed payload: "<ts>.<raw_body>"
        Replay tolerance: 300 seconds.
        """
        try:
            parts = dict(item.split("=", 1) for item in signature.split(",") if "=" in item)
            timestamp = parts.get("t", "")
            received_sig = parts.get("v1", "")
            if abs(time.time() - int(timestamp)) > 300:
                logger.warning("stripe_webhook_replay_too_old", age=time.time() - int(timestamp))
                return False
            signed = f"{timestamp}.{raw_body.decode()}".encode()
            expected = hmac.new(self._webhook_secret.encode(), signed,
                                 digestmod=hashlib.sha256).hexdigest()
            return hmac.compare_digest(expected, received_sig)
        except Exception as e:
            logger.error("stripe_signature_error", error=str(e))
            return False

    def _raise_decline(self, body: dict) -> None:
        err = body.get("error", {})
        raise GatewayDeclineError(self.name,
                                  err.get("decline_code") or err.get("code", "DECLINED"),
                                  err.get("message", "Card declined"))


# ── PayU ──────────────────────────────────────────────────────────────────────

class PayUAdapter(GatewayBase):
    """
    PayU Money API.
    Auth: SHA-512 hash of key|txnid|amount|productinfo|firstname|email|||||||||||salt
    Docs: https://developer.payumoney.com/
    """
    BASE_URL = "https://secure.payu.in"      # prod
    SANDBOX_URL = "https://test.payu.in"     # test

    def __init__(self):
        is_test = settings.ENVIRONMENT != "production"
        super().__init__(self.SANDBOX_URL if is_test else self.BASE_URL,
                         connect_timeout=5.0, read_timeout=45.0)
        self._key = settings.PAYU_MERCHANT_KEY
        self._salt = settings.PAYU_MERCHANT_SALT
        self._webhook_secret = settings.PAYU_WEBHOOK_SECRET

    @property
    def name(self) -> str:
        return "payu"

    def _hash(self, txnid: str, amount: str, productinfo: str,
              firstname: str, email: str) -> str:
        s = f"{self._key}|{txnid}|{amount}|{productinfo}|{firstname}|{email}|||||||||||{self._salt}"
        return hashlib.sha512(s.encode()).hexdigest()

    def _cmd_hash(self, command: str, var1: str) -> str:
        s = f"{self._key}|{command}|{var1}|{self._salt}"
        return hashlib.sha512(s.encode()).hexdigest()

    async def authorise(self, amount_paise: int, currency: str, payment_method: str,
                        merchant_order_id: str, idempotency_key: str,
                        customer_id: Optional[str] = None,
                        metadata: Optional[dict] = None) -> AuthResponse:
        amount_str = f"{amount_paise / 100:.2f}"
        md = metadata or {}
        firstname = md.get("customer_name", "Customer")[:60]
        email = md.get("customer_email", "customer@payflow.in")[:50]
        productinfo = md.get("description", "Payment")[:100]
        phone = md.get("phone", "9999999999")[:20]

        data = {
            "key": self._key,
            "txnid": idempotency_key[:50],
            "amount": amount_str,
            "productinfo": productinfo,
            "firstname": firstname,
            "email": email,
            "phone": phone,
            "surl": settings.PAYU_SUCCESS_URL,
            "furl": settings.PAYU_FAILURE_URL,
            "hash": self._hash(idempotency_key[:50], amount_str, productinfo, firstname, email),
            "udf1": merchant_order_id[:255],
        }
        resp, latency = await self._request("POST", "/_payment", data=data)
        mihpayid = resp.get("payuMoneyId") or resp.get("mihpayid", "")
        return AuthResponse(
            status=GatewayAuthStatus.AUTHORISED,
            gateway_reference=mihpayid,
            authorized_amount_paise=amount_paise,
            raw_response=_redact(resp),
            latency_ms=latency,
        )

    async def capture(self, gateway_reference: str, amount_paise: int,
                      idempotency_key: str) -> CaptureResponse:
        cmd = "capture_transaction"
        data = {"key": self._key, "command": cmd, "var1": gateway_reference,
                "var2": f"{amount_paise / 100:.2f}", "hash": self._cmd_hash(cmd, gateway_reference)}
        resp, latency = await self._request("POST", "/merchant/postservice.php?form=2", data=data)
        return CaptureResponse(
            status=GatewayCaptureStatus.CAPTURED,
            gateway_reference=gateway_reference,
            captured_amount_paise=amount_paise,
            raw_response=_redact(resp),
            latency_ms=latency,
        )

    async def refund(self, gateway_reference: str, amount_paise: int,
                     refund_idempotency_key: str, reason: Optional[str] = None) -> RefundResponse:
        cmd = "cancel_refund_transaction"
        data = {
            "key": self._key, "command": cmd,
            "var1": gateway_reference,
            "var2": refund_idempotency_key[:50],
            "var3": f"{amount_paise / 100:.2f}",
            "hash": self._cmd_hash(cmd, gateway_reference),
        }
        resp, latency = await self._request("POST", "/merchant/postservice.php?form=2", data=data)
        return RefundResponse(
            status=GatewayRefundStatus.REFUNDED,
            gateway_refund_id=resp.get("requestId", refund_idempotency_key),
            refunded_amount_paise=amount_paise,
            raw_response=_redact(resp),
            latency_ms=latency,
        )

    async def void(self, gateway_reference: str) -> VoidResponse:
        cmd = "cancel_transaction"
        data = {"key": self._key, "command": cmd, "var1": gateway_reference,
                "hash": self._cmd_hash(cmd, gateway_reference)}
        resp, _ = await self._request("POST", "/merchant/postservice.php?form=2", data=data)
        return VoidResponse(success=True, gateway_reference=gateway_reference,
                            raw_response=_redact(resp))

    async def get_payment_status(self, gateway_reference: str) -> PaymentStatus:
        cmd = "verify_payment"
        data = {"key": self._key, "command": cmd, "var1": gateway_reference,
                "hash": self._cmd_hash(cmd, gateway_reference)}
        resp, _ = await self._request("POST", "/merchant/postservice.php?form=2", data=data)
        txn = resp.get("transaction_details", {}).get(gateway_reference, {})
        state_map = {"captured": "CAPTURED", "failed": "FAILED",
                     "refunded": "REFUNDED", "pending": "AUTH_INITIATED"}
        return PaymentStatus(
            gateway_state=state_map.get(txn.get("status", ""), "UNKNOWN"),
            amount_paise=int(float(txn.get("net_amount_debit", 0)) * 100),
            gateway_reference=gateway_reference,
            raw_response=_redact(resp),
        )

    def verify_webhook_signature(self, raw_body: bytes, signature: str) -> bool:
        """
        PayU reverse hash:
        SHA512(salt|status|||||||||||udf5|udf4|udf3|udf2|udf1|email|firstname|productinfo|amount|txnid|key)
        """
        try:
            params: dict = {}
            for part in raw_body.decode().split("&"):
                if "=" in part:
                    k, v = part.split("=", 1)
                    params[k] = v
            s = (f"{self._salt}|{params.get('status', '')}|||||||||||"
                 f"{params.get('udf5','')}|{params.get('udf4','')}|"
                 f"{params.get('udf3','')}|{params.get('udf2','')}|"
                 f"{params.get('udf1','')}|{params.get('email','')}|"
                 f"{params.get('firstname','')}|{params.get('productinfo','')}|"
                 f"{params.get('amount','')}|{params.get('txnid','')}|{self._key}")
            expected = hashlib.sha512(s.encode()).hexdigest()
            return hmac.compare_digest(expected, signature)
        except Exception:
            return False


# ── UPI via PhonePe ───────────────────────────────────────────────────────────

class UPIAdapter(GatewayBase):
    """
    UPI via PhonePe Payment Gateway (NPCI aggregator).
    Auth: SHA256(base64(payload) + endpoint + saltKey) + "###" + saltIndex
    Docs: https://developer.phonepe.com/v1/reference/pay-api-1
    Single-phase — no auth+capture. Funds transfer is atomic on customer approval.
    """
    BASE_URL = "https://api.phonepe.com/apis/hermes"
    SANDBOX_URL = "https://api-preprod.phonepe.com/apis/hermes"

    def __init__(self):
        is_test = settings.ENVIRONMENT != "production"
        super().__init__(self.SANDBOX_URL if is_test else self.BASE_URL,
                         connect_timeout=5.0, read_timeout=60.0)
        self._merchant_id = settings.PHONEPE_MERCHANT_ID
        self._salt_key = settings.PHONEPE_SALT_KEY
        self._salt_index = settings.PHONEPE_SALT_INDEX

    @property
    def name(self) -> str:
        return "upi"

    def _x_verify(self, payload_b64: str, endpoint: str) -> str:
        digest = hashlib.sha256(
            (payload_b64 + endpoint + self._salt_key).encode()
        ).hexdigest()
        return f"{digest}###{self._salt_index}"

    async def authorise(self, amount_paise: int, currency: str, payment_method: str,
                        merchant_order_id: str, idempotency_key: str,
                        customer_id: Optional[str] = None,
                        metadata: Optional[dict] = None) -> AuthResponse:
        endpoint = "/pg/v1/pay"
        payload = {
            "merchantId": self._merchant_id,
            "merchantTransactionId": idempotency_key[:35],
            "merchantUserId": customer_id or "MUID001",
            "amount": amount_paise,
            "redirectUrl": settings.UPI_REDIRECT_URL,
            "redirectMode": "POST",
            "callbackUrl": settings.UPI_CALLBACK_URL,
            "paymentInstrument": {
                "type": "UPI_COLLECT",
                "vpa": (metadata or {}).get("upi_id", ""),
            },
        }
        payload_b64 = base64.b64encode(json.dumps(payload).encode()).decode()
        resp, latency = await self._request(
            "POST", endpoint,
            json_body={"request": payload_b64},
            headers={"X-VERIFY": self._x_verify(payload_b64, endpoint),
                     "X-MERCHANT-ID": self._merchant_id,
                     "Content-Type": "application/json"},
        )
        txn_id = resp.get("data", {}).get("transactionId", idempotency_key)
        return AuthResponse(
            status=GatewayAuthStatus.PENDING,
            gateway_reference=txn_id,
            authorized_amount_paise=amount_paise,
            raw_response=_redact(resp),
            latency_ms=latency,
        )

    async def capture(self, gateway_reference: str, amount_paise: int,
                      idempotency_key: str) -> CaptureResponse:
        raise NotImplementedError("UPI is single-phase — no separate capture")

    async def refund(self, gateway_reference: str, amount_paise: int,
                     refund_idempotency_key: str, reason: Optional[str] = None) -> RefundResponse:
        endpoint = "/pg/v1/refund"
        payload = {
            "merchantId": self._merchant_id,
            "merchantUserId": "MUID001",
            "originalTransactionId": gateway_reference,
            "merchantTransactionId": refund_idempotency_key[:35],
            "amount": amount_paise,
            "callbackUrl": settings.UPI_CALLBACK_URL,
        }
        payload_b64 = base64.b64encode(json.dumps(payload).encode()).decode()
        resp, latency = await self._request(
            "POST", endpoint,
            json_body={"request": payload_b64},
            headers={"X-VERIFY": self._x_verify(payload_b64, endpoint),
                     "X-MERCHANT-ID": self._merchant_id},
        )
        refund_id = resp.get("data", {}).get("transactionId", refund_idempotency_key)
        return RefundResponse(
            status=GatewayRefundStatus.REFUNDED,
            gateway_refund_id=refund_id,
            refunded_amount_paise=amount_paise,
            raw_response=_redact(resp),
            latency_ms=latency,
        )

    async def void(self, gateway_reference: str) -> VoidResponse:
        raise NotImplementedError("UPI does not support void operations")

    async def get_payment_status(self, gateway_reference: str) -> PaymentStatus:
        endpoint = f"/pg/v1/status/{self._merchant_id}/{gateway_reference}"
        digest = hashlib.sha256((endpoint + self._salt_key).encode()).hexdigest()
        x_verify = f"{digest}###{self._salt_index}"
        resp, _ = await self._request(
            "GET", endpoint,
            headers={"X-VERIFY": x_verify, "X-MERCHANT-ID": self._merchant_id},
        )
        code = resp.get("code", "")
        state_map = {
            "PAYMENT_SUCCESS": "CAPTURED", "PAYMENT_PENDING": "AUTH_INITIATED",
            "PAYMENT_DECLINED": "FAILED", "TIMED_OUT": "AUTH_EXPIRED",
            "PAYMENT_ERROR": "FAILED",
        }
        return PaymentStatus(
            gateway_state=state_map.get(code, "UNKNOWN"),
            amount_paise=resp.get("data", {}).get("amount"),
            gateway_reference=gateway_reference,
            raw_response=_redact(resp),
        )

    def verify_webhook_signature(self, raw_body: bytes, signature: str) -> bool:
        try:
            payload_b64 = base64.b64encode(raw_body).decode()
            digest = hashlib.sha256(
                (payload_b64 + self._salt_key).encode()
            ).hexdigest()
            expected = f"{digest}###{self._salt_index}"
            return hmac.compare_digest(expected, signature)
        except Exception:
            return False


_REGISTRY: dict[str, PaymentGateway] = {}


def get_gateway(name: str) -> PaymentGateway:
    global _REGISTRY
    if not _REGISTRY:
        _REGISTRY = {
            "razorpay": RazorpayAdapter(),
            "stripe": StripeAdapter(),
            "payu": PayUAdapter(),
            "upi": UPIAdapter(),
        }
    adapter = _REGISTRY.get(name)
    if not adapter:
        raise ValueError(f"Unknown gateway: {name}. Available: {list(_REGISTRY)}")
    return adapter
