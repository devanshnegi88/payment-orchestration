"""
Reconciliation Engine (Section A5.5)

Runs every 15 minutes to detect discrepancies between internal state
and gateway settlement data. Addresses the silent settlement leak (Case Study C2).

Process:
1. Identify stale transactions (stuck in transitional states > 5 min)
2. Poll gateway status API for each
3. Compare and reconcile
4. Alert on critical anomalies (CAPTURED internally but FAILED at gateway)
"""
import asyncio
from datetime import datetime, timedelta
from typing import Optional
import structlog
import pytz
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, and_, or_, update

from app.models.transaction import (
    Transaction,
    TransactionStateLog,
    ReconciliationRun,
    ReconciliationLog,
)
from app.domain.state_machine import TransactionState, TransactionEvent, get_state_machine
from app.gateways.adapters import get_gateway
from app.config import settings

logger = structlog.get_logger(__name__)
fsm = get_state_machine()

# States that should NOT be stuck for more than STALE_TRANSACTION_MINUTES
STALE_TRANSITIONAL_STATES = [
    TransactionState.AUTH_INITIATED,
    TransactionState.CAPTURE_INITIATED,
    TransactionState.REFUND_INITIATED,
    TransactionState.VOID_INITIATED,
    TransactionState.ROUTE_SELECTED,
]

# Critical anomalies: internal says money moved, gateway disagrees
CRITICAL_ANOMALY_PAIRS = {
    ("CAPTURED", "FAILED"),
    ("CAPTURED", "REVERSED"),
    ("REFUNDED", "FAILED"),
    ("SETTLED", "REVERSED"),
}


class ReconciliationEngine:
    """
    Periodic batch reconciliation between internal state and gateway truth.
    """

    def __init__(self, db: AsyncSession):
        self.db = db

    async def run(self, triggered_by: str = "scheduler") -> ReconciliationRun:
        """
        Execute a full reconciliation run.
        Returns a ReconciliationRun record with results summary.
        """
        run = ReconciliationRun(
            started_at=datetime.now(pytz.UTC),
            triggered_by=triggered_by,
            status="RUNNING",
        )
        self.db.add(run)
        await self.db.flush()

        logger.info("reconciliation_run_started", run_id=run.id, triggered_by=triggered_by)

        txns_checked = 0
        discrepancies = 0
        anomalies = 0

        try:
            # Step 1: Find stale transactions
            stale_transactions = await self._find_stale_transactions()
            logger.info("reconciliation_stale_found", count=len(stale_transactions))

            # Step 2: Process in batches (avoid memory pressure)
            batch_size = 50
            for i in range(0, len(stale_transactions), batch_size):
                batch = stale_transactions[i:i + batch_size]
                results = await asyncio.gather(
                    *[self._reconcile_transaction(txn, run.id) for txn in batch],
                    return_exceptions=True,
                )

                for r in results:
                    if isinstance(r, Exception):
                        logger.error("reconciliation_item_error", error=str(r))
                        continue
                    if r:
                        txns_checked += 1
                        if r.get("discrepancy"):
                            discrepancies += 1
                        if r.get("anomaly"):
                            anomalies += 1

            run.transactions_checked = txns_checked
            run.discrepancies_found = discrepancies
            run.anomalies_found = anomalies
            run.status = "COMPLETED"
            run.completed_at = datetime.now(pytz.UTC)
            await self.db.commit()

            logger.info(
                "reconciliation_run_completed",
                run_id=run.id,
                checked=txns_checked,
                discrepancies=discrepancies,
                anomalies=anomalies,
            )

        except Exception as e:
            run.status = "FAILED"
            await self.db.commit()
            logger.error("reconciliation_run_failed", run_id=run.id, error=str(e))
            raise

        return run

    async def _find_stale_transactions(self) -> list[Transaction]:
        """
        Find transactions stuck in transitional states beyond the stale threshold.
        """
        cutoff = datetime.now(pytz.UTC) - timedelta(minutes=settings.STALE_TRANSACTION_MINUTES)

        result = await self.db.execute(
            select(Transaction).where(
                and_(
                    Transaction.state.in_(STALE_TRANSITIONAL_STATES),
                    Transaction.updated_at < cutoff,
                    Transaction.gateway.isnot(None),
                    Transaction.gateway_reference.isnot(None),
                )
            ).limit(500)  # Safety cap per run
        )
        return list(result.scalars().all())

    async def _reconcile_transaction(
        self, txn: Transaction, run_id: str
    ) -> Optional[dict]:
        """
        Reconcile a single transaction against its gateway.
        Returns dict with discrepancy/anomaly flags.
        """
        try:
            gateway = get_gateway(txn.gateway)
            gateway_status = await gateway.get_payment_status(txn.gateway_reference)
        except Exception as e:
            logger.warning(
                "reconciliation_gateway_poll_failed",
                transaction_id=txn.id,
                gateway=txn.gateway,
                error=str(e),
            )
            return None

        internal_state = txn.state.value
        gateway_state = gateway_status.gateway_state

        # No discrepancy — states match
        if internal_state == gateway_state or gateway_state == "UNKNOWN":
            return {"discrepancy": False, "anomaly": False}

        # Detect critical anomalies (C2: silent settlement leak)
        is_anomaly = (internal_state, gateway_state) in CRITICAL_ANOMALY_PAIRS

        log_entry = ReconciliationLog(
            run_id=run_id,
            transaction_id=txn.id,
            gateway=txn.gateway,
            internal_state=internal_state,
            gateway_state=gateway_state,
            discrepancy_type=self._classify_discrepancy(internal_state, gateway_state),
            internal_amount_paise=txn.amount_paise,
            gateway_amount_paise=gateway_status.amount_paise,
            is_anomaly=is_anomaly,
        )
        self.db.add(log_entry)

        if is_anomaly:
            logger.error(
                "reconciliation_critical_anomaly",
                transaction_id=txn.id,
                internal_state=internal_state,
                gateway_state=gateway_state,
                gateway=txn.gateway,
                amount_paise=txn.amount_paise,
            )
            # IMPORTANT: Do NOT auto-refund on anomaly — requires human review (FS-11)
            # Flag transaction for manual review
            await self._flag_for_manual_review(txn, internal_state, gateway_state)
        else:
            # Apply gateway state as source of truth (reconciliation override)
            await self._apply_gateway_state(txn, gateway_state)

        await self.db.flush()
        return {"discrepancy": True, "anomaly": is_anomaly}

    async def _apply_gateway_state(self, txn: Transaction, gateway_state: str) -> None:
        """Apply gateway state as truth for non-critical discrepancies."""
        state_map = {
            "AUTHORISED": (TransactionEvent.GATEWAY_AUTH_SUCCESS, TransactionState.AUTHORISED),
            "CAPTURED": (TransactionEvent.GATEWAY_CAPTURE_SUCCESS, TransactionState.CAPTURED),
            "FAILED": (TransactionEvent.MAX_RETRIES_EXCEEDED, TransactionState.FAILED),
            "VOIDED": (TransactionEvent.GATEWAY_VOID_SUCCESS, TransactionState.VOIDED),
            "REFUNDED": (TransactionEvent.GATEWAY_REFUND_SUCCESS, TransactionState.REFUNDED),
        }
        if gateway_state not in state_map:
            return

        target_event, target_state = state_map[gateway_state]
        old_state = txn.state

        # Only apply if the transition is valid
        if fsm.can_transition(txn.state, target_event):
            txn.state = target_state
            txn.version += 1
            log_entry = TransactionStateLog(
                transaction_id=txn.id,
                from_state=old_state,
                to_state=target_state,
                event=TransactionEvent.RECONCILIATION_OVERRIDE,
                log_metadata={"gateway_state": gateway_state, "source": "reconciliation_engine"},
                created_by="reconciliation_engine",
            )
            self.db.add(log_entry)
            logger.info(
                "reconciliation_override_applied",
                transaction_id=txn.id,
                from_state=old_state.value,
                to_state=target_state.value,
            )

    async def _flag_for_manual_review(
        self, txn: Transaction, internal_state: str, gateway_state: str
    ) -> None:
        """Flag a critical anomaly — no automatic action, requires human review."""
        log_entry = TransactionStateLog(
            transaction_id=txn.id,
            from_state=txn.state,
            to_state=txn.state,  # State unchanged — for audit only
            event=TransactionEvent.RECONCILIATION_OVERRIDE,
            log_metadata={
                "anomaly": True,
                "internal_state": internal_state,
                "gateway_state": gateway_state,
                "action": "FLAGGED_FOR_MANUAL_REVIEW",
            },
            created_by="reconciliation_engine",
        )
        self.db.add(log_entry)

    def _classify_discrepancy(self, internal: str, gateway: str) -> str:
        if internal in ("AUTH_INITIATED", "CAPTURE_INITIATED") and gateway == "CAPTURED":
            return "LATE_SUCCESS"
        if internal == "CAPTURED" and gateway == "FAILED":
            return "GHOST_CAPTURE"
        if internal in ("AUTH_INITIATED",) and gateway == "FAILED":
            return "SILENT_FAILURE"
        if internal == "CAPTURED" and gateway == "REFUNDED":
            return "UNTRACKED_REFUND"
        return "STATE_MISMATCH"
