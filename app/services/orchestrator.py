"""
Payment Orchestrator — central coordinator with real metrics, retry, and failover.

Every operation:
1. Validates idempotency (advisory lock)
2. Writes state transitions to immutable audit log
3. Calls gateway through the retry engine (backoff + classification)
4. Records Prometheus metrics (latency, success rate, failover count)
5. Updates circuit breaker on every gateway outcome
6. Releases DB connection before any gateway call (flash sale lesson — C5)
"""
import asyncio
import time
import uuid
from datetime import datetime
from typing import Optional

import structlog
import pytz
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from app.config import settings
from app.models.transaction import Transaction, TransactionStateLog, GatewayRoute, Refund
from app.domain.state_machine import TransactionState, TransactionEvent, get_state_machine
from app.domain.exceptions import (
    GatewayError, GatewayTimeoutError, GatewayServerError,
    GatewayDeclineError, GatewayRateLimitError, GatewayUnavailableError,
    InvalidStateTransitionError, NoAvailableGatewayError,
    TransactionNotFoundError,
)
from app.gateways.adapters import get_gateway, GatewayAuthStatus, GatewayCaptureStatus, GatewayRefundStatus
from app.services.router import FailoverRouter
from app.services.circuit_breaker import CircuitBreakerRegistry
from app.services.idempotency import IdempotencyService
from app.services.retry import with_retry, classify, RetryClass
from app.services.audit import AuditService
from app.services import metrics as m

logger = structlog.get_logger(__name__)
fsm = get_state_machine()


class PaymentOrchestrator:
    def __init__(
        self,
        db: AsyncSession,
        cb_registry: CircuitBreakerRegistry,
        failover_router: FailoverRouter,
        idempotency_service: IdempotencyService,
        audit_service: AuditService,
    ):
        self.db = db
        self.cb = cb_registry
        self.router = failover_router
        self.idempotency = idempotency_service
        self.audit = audit_service

    # ── Create payment ─────────────────────────────────────────────────────────

    async def create_payment(
        self,
        merchant_id: str,
        merchant_order_id: str,
        amount_paise: int,
        currency: str,
        payment_method: str,
        idempotency_key: str,
        customer_id: Optional[str] = None,
        customer_email: Optional[str] = None,
        description: Optional[str] = None,
        metadata: Optional[dict] = None,
    ) -> Transaction:
        trace_id = str(uuid.uuid4())
        log = logger.bind(trace_id=trace_id, merchant_id=merchant_id,
                          order_id=merchant_order_id, amount_paise=amount_paise)

        # ── 1. Idempotency check ──────────────────────────────────────────────
        request_body = {
            "merchant_id": merchant_id, "merchant_order_id": merchant_order_id,
            "amount_paise": amount_paise, "currency": currency,
            "payment_method": payment_method,
        }
        cached = await self.idempotency.acquire_lock(merchant_id, idempotency_key, request_body)
        if cached is not None:
            m.idempotency_hits.labels(result="replay").inc()
            return await self._get_txn(cached["transaction_id"])

        try:
            # ── 2. Create transaction ─────────────────────────────────────────
            txn = Transaction(
                merchant_id=merchant_id,
                merchant_order_id=merchant_order_id,
                amount_paise=amount_paise,
                currency=currency,
                payment_method=payment_method,
                idempotency_key=idempotency_key,
                customer_id=customer_id,
                customer_email=customer_email,
                description=description,
                state=TransactionState.CREATED,
                trace_id=trace_id,
            )
            self.db.add(txn)
            await self.db.flush()
            log = log.bind(transaction_id=txn.id)

            # ── 3. Route selection ────────────────────────────────────────────
            t_route = time.monotonic()
            decision = await self.router.select_with_failover(
                payment_method=payment_method,
                amount_paise=amount_paise,
                trace_id=trace_id,
            )
            m.routing_duration.observe(time.monotonic() - t_route)

            gateway_name = decision.selected_gateway
            await self._transition(txn, TransactionEvent.ROUTE_DECISION_MADE,
                                   metadata={"gateway": gateway_name})
            txn.gateway = gateway_name

            # Record routing decision with full score breakdown
            top_score = decision.scores[0] if decision.scores else None
            self.db.add(GatewayRoute(
                transaction_id=txn.id,
                gateway=gateway_name,
                attempt_number=1,
                composite_score=top_score.composite_score if top_score else None,
                success_rate_score=top_score.success_rate_score if top_score else None,
                latency_score=top_score.latency_score if top_score else None,
                cost_score=top_score.cost_score if top_score else None,
                health_score=top_score.health_score if top_score else None,
                fit_score=top_score.fit_score if top_score else None,
            ))

            # ── 4. Release DB before gateway call ─────────────────────────────
            # Never hold a connection while waiting on external APIs.
            await self.db.commit()

            # ── 5. Auth with retry ────────────────────────────────────────────
            await self._transition_by_id(txn.id, TransactionEvent.GATEWAY_AUTH_CALLED)
            gateway = get_gateway(gateway_name)
            cb = self.cb.get(gateway_name, payment_method)

            t_auth = time.monotonic()
            try:
                auth_resp = await with_retry(
                    fn=lambda: gateway.authorise(
                        amount_paise=amount_paise, currency=currency,
                        payment_method=payment_method,
                        merchant_order_id=merchant_order_id,
                        idempotency_key=idempotency_key,
                        customer_id=customer_id, metadata=metadata,
                    ),
                    gateway=gateway_name,
                    operation="auth",
                    max_retries=2,
                )
                latency_ms = int((time.monotonic() - t_auth) * 1000)
                m.record_gateway_latency(gateway_name, "auth", latency_ms)
                await cb.record_success()
                m.record_payment_attempt(gateway_name, payment_method, "success")

            except GatewayError as exc:
                latency_ms = int((time.monotonic() - t_auth) * 1000)
                m.record_gateway_latency(gateway_name, "auth", latency_ms)
                await cb.record_failure()
                m.record_payment_attempt(gateway_name, payment_method, "failed")

                rc = classify(exc)
                if rc == RetryClass.FAILOVER or rc == RetryClass.RETRYABLE:
                    txn = await self._handle_failover(
                        txn, exc, payment_method, amount_paise,
                        currency, merchant_order_id, idempotency_key,
                        customer_id, metadata, trace_id,
                    )
                else:
                    # Hard decline — mark failed
                    await self._transition_by_id(txn.id, TransactionEvent.GATEWAY_AUTH_DECLINED,
                                                  gateway_response={"error": str(exc)[:200]})
                    await self._transition_by_id(txn.id, TransactionEvent.MAX_RETRIES_EXCEEDED)
                    txn = await self._get_txn(txn.id)

                await self._finalize_idempotency(merchant_id, idempotency_key, txn)
                return txn

            # ── 6. Write auth result ──────────────────────────────────────────
            if auth_resp.status == GatewayAuthStatus.AUTHORISED:
                event = TransactionEvent.GATEWAY_AUTH_SUCCESS
            elif auth_resp.status == GatewayAuthStatus.PENDING:
                event = TransactionEvent.GATEWAY_AUTH_CALLED   # stays AUTH_INITIATED for UPI
            else:
                event = TransactionEvent.GATEWAY_AUTH_DECLINED

            await self._transition_by_id(
                txn.id, event,
                gateway_reference=auth_resp.gateway_reference,
                gateway_response=auth_resp.raw_response,
                metadata={"latency_ms": latency_ms},
            )

            txn = await self._get_txn(txn.id)
            txn.gateway_reference = auth_resp.gateway_reference
            txn.gateway_order_id = auth_resp.gateway_order_id
            txn.authorized_amount_paise = auth_resp.authorized_amount_paise
            await self.db.commit()

            await self._finalize_idempotency(merchant_id, idempotency_key, txn)
            log.info("payment_created", state=txn.state, ref=txn.gateway_reference)
            return txn

        except Exception as exc:
            await self.idempotency.fail(merchant_id, idempotency_key, str(exc))
            raise

    # ── Capture ────────────────────────────────────────────────────────────────

    async def capture_payment(
        self,
        transaction_id: str,
        amount_paise: Optional[int] = None,
        idempotency_key: Optional[str] = None,
    ) -> Transaction:
        txn = await self._get_txn_locked(transaction_id)
        if txn.state not in (TransactionState.AUTHORISED, TransactionState.PARTIALLY_CAPTURED):
            raise InvalidStateTransitionError(
                txn.state.value, "CAPTURE_INITIATED", transaction_id,
                [e.value for e in fsm.get_valid_transitions(txn.state)],
            )

        capture_amount = amount_paise or txn.authorized_amount_paise or txn.amount_paise
        cap_key = idempotency_key or f"cap_{transaction_id}"

        await self._transition(txn, TransactionEvent.GATEWAY_CAPTURE_CALLED)
        await self.db.commit()

        gateway = get_gateway(txn.gateway)
        cb = self.cb.get(txn.gateway, txn.payment_method)
        t0 = time.monotonic()

        try:
            cap_resp = await with_retry(
                fn=lambda: gateway.capture(txn.gateway_reference, capture_amount, cap_key),
                gateway=txn.gateway, operation="capture", max_retries=3,
            )
            latency_ms = int((time.monotonic() - t0) * 1000)
            m.record_gateway_latency(txn.gateway, "capture", latency_ms)
            await cb.record_success()

        except GatewayError as exc:
            latency_ms = int((time.monotonic() - t0) * 1000)
            m.record_gateway_latency(txn.gateway, "capture", latency_ms)
            await cb.record_failure()
            await self._transition_by_id(transaction_id, TransactionEvent.GATEWAY_CAPTURE_FAILED,
                                          gateway_response={"error": str(exc)[:200]})
            return await self._get_txn(transaction_id)

        captured = cap_resp.captured_amount_paise
        event = (TransactionEvent.GATEWAY_CAPTURE_PARTIAL
                 if captured < txn.amount_paise
                 else TransactionEvent.GATEWAY_CAPTURE_SUCCESS)

        await self._transition_by_id(transaction_id, event,
                                      gateway_reference=cap_resp.gateway_reference,
                                      gateway_response=cap_resp.raw_response)
        txn = await self._get_txn(transaction_id)
        txn.captured_amount_paise = (txn.captured_amount_paise or 0) + captured
        await self.db.commit()
        return txn

    # ── Refund ─────────────────────────────────────────────────────────────────

    async def refund_payment(
        self,
        transaction_id: str,
        amount_paise: Optional[int] = None,
        reason: Optional[str] = None,
        idempotency_key: Optional[str] = None,
    ) -> Refund:
        txn = await self._get_txn_locked(transaction_id)
        refundable = {TransactionState.CAPTURED, TransactionState.PARTIALLY_CAPTURED,
                      TransactionState.SETTLED, TransactionState.PARTIALLY_REFUNDED}
        if txn.state not in refundable:
            raise InvalidStateTransitionError(
                txn.state.value, "REFUND_INITIATED", transaction_id,
                [e.value for e in fsm.get_valid_transitions(txn.state)],
            )

        refund_amount = amount_paise or txn.captured_amount_paise or txn.amount_paise
        already_refunded = txn.refunded_amount_paise or 0
        max_refundable = (txn.captured_amount_paise or txn.amount_paise) - already_refunded
        if refund_amount > max_refundable:
            raise ValueError(
                f"Refund {refund_amount} paise exceeds refundable {max_refundable} paise"
            )

        rfnd_key = idempotency_key or f"rfnd_{transaction_id}_{uuid.uuid4().hex[:8]}"
        await self._transition(txn, TransactionEvent.GATEWAY_REFUND_CALLED)
        await self.db.commit()

        gateway = get_gateway(txn.gateway)
        t0 = time.monotonic()
        refund_resp = await with_retry(
            fn=lambda: gateway.refund(txn.gateway_reference, refund_amount, rfnd_key, reason),
            gateway=txn.gateway, operation="refund", max_retries=2,
        )
        m.record_gateway_latency(txn.gateway, "refund", int((time.monotonic() - t0) * 1000))

        if refund_resp.status == GatewayRefundStatus.REFUNDED:
            event = TransactionEvent.GATEWAY_REFUND_SUCCESS
        elif refund_resp.status == GatewayRefundStatus.PARTIAL:
            event = TransactionEvent.GATEWAY_REFUND_PARTIAL
        else:
            event = TransactionEvent.GATEWAY_REFUND_FAILED

        await self._transition_by_id(transaction_id, event,
                                      gateway_response=refund_resp.raw_response)

        refund = Refund(
            transaction_id=transaction_id,
            amount_paise=refund_resp.refunded_amount_paise,
            currency=txn.currency,
            state="COMPLETED" if refund_resp.status == GatewayRefundStatus.REFUNDED else "FAILED",
            gateway=txn.gateway,
            gateway_refund_id=refund_resp.gateway_refund_id,
            reason=reason,
            idempotency_key=rfnd_key,
        )
        self.db.add(refund)
        txn = await self._get_txn(transaction_id)
        txn.refunded_amount_paise = already_refunded + refund_resp.refunded_amount_paise
        await self.db.commit()
        return refund

    # ── Void ───────────────────────────────────────────────────────────────────

    async def void_payment(self, transaction_id: str) -> Transaction:
        txn = await self._get_txn_locked(transaction_id)
        if txn.state not in (TransactionState.AUTHORISED, TransactionState.CAPTURE_FAILED):
            raise InvalidStateTransitionError(
                txn.state.value, "VOID_INITIATED", transaction_id,
                [e.value for e in fsm.get_valid_transitions(txn.state)],
            )

        await self._transition(txn, TransactionEvent.GATEWAY_VOID_CALLED)
        await self.db.commit()

        gateway = get_gateway(txn.gateway)
        t0 = time.monotonic()
        void_resp = await gateway.void(txn.gateway_reference)
        m.record_gateway_latency(txn.gateway, "void", int((time.monotonic() - t0) * 1000))

        if void_resp.success:
            await self._transition_by_id(transaction_id, TransactionEvent.GATEWAY_VOID_SUCCESS)

        return await self._get_txn(transaction_id)

    # ── Failover ───────────────────────────────────────────────────────────────

    async def _handle_failover(
        self, txn: Transaction, error: GatewayError,
        payment_method: str, amount_paise: int, currency: str,
        merchant_order_id: str, idempotency_key: str,
        customer_id: Optional[str], metadata: Optional[dict], trace_id: str,
    ) -> Transaction:
        failed_gateways = [txn.gateway]

        logger.warning("failover_initiated", transaction_id=txn.id,
                       failed_gateway=txn.gateway, error=type(error).__name__)
        m.record_failover(txn.gateway, "unknown", type(error).__name__)

        for attempt in range(2):
            try:
                decision = await self.router.select_with_failover(
                    payment_method=payment_method, amount_paise=amount_paise,
                    trace_id=trace_id, previous_failed_gateways=failed_gateways,
                )
            except NoAvailableGatewayError:
                break

            new_gateway = decision.selected_gateway
            m.record_failover(txn.gateway, new_gateway, type(error).__name__)

            await self._transition_by_id(
                txn.id, TransactionEvent.ROUTE_DECISION_MADE,
                metadata={"failover_to": new_gateway, "attempt": attempt + 2},
            )
            txn = await self._get_txn(txn.id)
            txn.gateway = new_gateway
            txn.retry_count += 1
            await self.db.commit()

            await self._transition_by_id(txn.id, TransactionEvent.GATEWAY_AUTH_CALLED)
            gateway = get_gateway(new_gateway)
            cb = self.cb.get(new_gateway, payment_method)
            t0 = time.monotonic()

            try:
                auth_resp = await with_retry(
                    fn=lambda: gateway.authorise(
                        amount_paise=amount_paise, currency=currency,
                        payment_method=payment_method,
                        merchant_order_id=merchant_order_id,
                        idempotency_key=idempotency_key,
                        customer_id=customer_id, metadata=metadata,
                    ),
                    gateway=new_gateway, operation="auth", max_retries=1,
                )
                latency_ms = int((time.monotonic() - t0) * 1000)
                m.record_gateway_latency(new_gateway, "auth", latency_ms)
                await cb.record_success()
                m.record_payment_attempt(new_gateway, payment_method, "success")

                await self._transition_by_id(
                    txn.id, TransactionEvent.GATEWAY_AUTH_SUCCESS,
                    gateway_reference=auth_resp.gateway_reference,
                    gateway_response=auth_resp.raw_response,
                )
                txn = await self._get_txn(txn.id)
                txn.gateway_reference = auth_resp.gateway_reference
                txn.authorized_amount_paise = auth_resp.authorized_amount_paise
                await self.db.commit()

                logger.info("failover_succeeded", transaction_id=txn.id, gateway=new_gateway)
                return txn

            except GatewayError as exc2:
                latency_ms = int((time.monotonic() - t0) * 1000)
                m.record_gateway_latency(new_gateway, "auth", latency_ms)
                await cb.record_failure()
                m.record_payment_attempt(new_gateway, payment_method, "failed")
                failed_gateways.append(new_gateway)
                error = exc2

        await self._transition_by_id(txn.id, TransactionEvent.MAX_RETRIES_EXCEEDED)
        return await self._get_txn(txn.id)

    # ── State machine helpers ──────────────────────────────────────────────────

    async def _transition(
        self, txn: Transaction, event: TransactionEvent,
        gateway_reference: Optional[str] = None,
        gateway_response: Optional[dict] = None,
        metadata: Optional[dict] = None,
        created_by: str = "orchestrator",
    ) -> None:
        result = fsm.transition(txn.state, event, txn.id, metadata)
        old_state = txn.state
        txn.state = result.to_state
        txn.version += 1
        self.db.add(TransactionStateLog(
            transaction_id=txn.id,
            from_state=old_state,
            to_state=result.to_state,
            event=event,
            gateway_reference=gateway_reference or txn.gateway_reference,
            gateway_response=gateway_response,
            log_metadata=metadata or {},
            created_by=created_by,
        ))
        await self.db.flush()

    async def _transition_by_id(
        self, transaction_id: str, event: TransactionEvent,
        gateway_reference: Optional[str] = None,
        gateway_response: Optional[dict] = None,
        metadata: Optional[dict] = None,
        created_by: str = "orchestrator",
    ) -> None:
        txn = await self._get_txn_locked(transaction_id)
        await self._transition(txn, event, gateway_reference, gateway_response, metadata, created_by)
        await self.db.commit()

    async def _get_txn(self, transaction_id: str) -> Transaction:
        r = await self.db.execute(select(Transaction).where(Transaction.id == transaction_id))
        txn = r.scalar_one_or_none()
        if txn is None:
            raise TransactionNotFoundError(transaction_id)
        return txn

    async def _get_txn_locked(self, transaction_id: str) -> Transaction:
        r = await self.db.execute(
            select(Transaction).where(Transaction.id == transaction_id).with_for_update()
        )
        txn = r.scalar_one_or_none()
        if txn is None:
            raise TransactionNotFoundError(transaction_id)
        return txn

    async def _finalize_idempotency(
        self, merchant_id: str, key: str, txn: Transaction
    ) -> None:
        await self.idempotency.complete(
            merchant_id=merchant_id,
            idempotency_key=key,
            transaction_id=txn.id,
            response_code=200,
            response_body={"transaction_id": txn.id, "state": txn.state.value},
        )
