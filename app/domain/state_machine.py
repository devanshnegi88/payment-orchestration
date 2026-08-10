"""
Transaction State Machine (FSM)

Design:
- Strict finite state machine — invalid transitions throw InvalidStateTransitionError
- Every transition is logged to an immutable audit trail
- No direct state updates; all mutations go through transition()
- Guards (pre-conditions) prevent corrupted transitions

States follow PCI-DSS / RBI guidelines for payment intermediaries.
"""
from enum import Enum
from typing import Optional
from dataclasses import dataclass, field
import structlog

from app.domain.exceptions import InvalidStateTransitionError

logger = structlog.get_logger(__name__)


class TransactionState(str, Enum):
    # Initial states
    CREATED = "CREATED"
    ABANDONED = "ABANDONED"

    # Routing
    ROUTE_SELECTED = "ROUTE_SELECTED"
    ROUTE_FAILED = "ROUTE_FAILED"

    # Authorization flow
    AUTH_INITIATED = "AUTH_INITIATED"
    AUTHORISED = "AUTHORISED"
    AUTH_FAILED = "AUTH_FAILED"
    AUTH_TIMEOUT = "AUTH_TIMEOUT"
    AUTH_EXPIRED = "AUTH_EXPIRED"

    # Capture flow
    CAPTURE_INITIATED = "CAPTURE_INITIATED"
    CAPTURED = "CAPTURED"
    PARTIALLY_CAPTURED = "PARTIALLY_CAPTURED"
    CAPTURE_FAILED = "CAPTURE_FAILED"

    # Void flow
    VOID_INITIATED = "VOID_INITIATED"
    VOIDED = "VOIDED"

    # Refund flow
    REFUND_INITIATED = "REFUND_INITIATED"
    REFUNDED = "REFUNDED"
    PARTIALLY_REFUNDED = "PARTIALLY_REFUNDED"
    REFUND_FAILED = "REFUND_FAILED"

    # Terminal states
    FAILED = "FAILED"
    SETTLED = "SETTLED"

    # Dispute states
    DISPUTE_OPENED = "DISPUTE_OPENED"
    DISPUTE_RESOLVED = "DISPUTE_RESOLVED"


class TransactionEvent(str, Enum):
    # Customer / merchant actions
    PAYMENT_INITIATED = "PAYMENT_INITIATED"
    PAYMENT_ABANDONED = "PAYMENT_ABANDONED"
    CAPTURE_REQUESTED = "CAPTURE_REQUESTED"
    VOID_REQUESTED = "VOID_REQUESTED"
    REFUND_REQUESTED = "REFUND_REQUESTED"

    # Router events
    ROUTE_DECISION_MADE = "ROUTE_DECISION_MADE"
    ROUTE_DECISION_FAILED = "ROUTE_DECISION_FAILED"

    # Gateway auth events
    GATEWAY_AUTH_CALLED = "GATEWAY_AUTH_CALLED"
    GATEWAY_AUTH_SUCCESS = "GATEWAY_AUTH_SUCCESS"
    GATEWAY_AUTH_DECLINED = "GATEWAY_AUTH_DECLINED"
    GATEWAY_AUTH_TIMEOUT = "GATEWAY_AUTH_TIMEOUT"
    GATEWAY_AUTH_EXPIRED = "GATEWAY_AUTH_EXPIRED"

    # Gateway capture events
    GATEWAY_CAPTURE_CALLED = "GATEWAY_CAPTURE_CALLED"
    GATEWAY_CAPTURE_SUCCESS = "GATEWAY_CAPTURE_SUCCESS"
    GATEWAY_CAPTURE_PARTIAL = "GATEWAY_CAPTURE_PARTIAL"
    GATEWAY_CAPTURE_FAILED = "GATEWAY_CAPTURE_FAILED"

    # Gateway void events
    GATEWAY_VOID_CALLED = "GATEWAY_VOID_CALLED"
    GATEWAY_VOID_SUCCESS = "GATEWAY_VOID_SUCCESS"

    # Gateway refund events
    GATEWAY_REFUND_CALLED = "GATEWAY_REFUND_CALLED"
    GATEWAY_REFUND_SUCCESS = "GATEWAY_REFUND_SUCCESS"
    GATEWAY_REFUND_PARTIAL = "GATEWAY_REFUND_PARTIAL"
    GATEWAY_REFUND_FAILED = "GATEWAY_REFUND_FAILED"

    # Settlement / lifecycle
    SETTLEMENT_CONFIRMED = "SETTLEMENT_CONFIRMED"
    MAX_RETRIES_EXCEEDED = "MAX_RETRIES_EXCEEDED"

    # Webhook / reconciliation
    WEBHOOK_RECEIVED = "WEBHOOK_RECEIVED"
    RECONCILIATION_OVERRIDE = "RECONCILIATION_OVERRIDE"

    # Dispute
    DISPUTE_RAISED = "DISPUTE_RAISED"
    DISPUTE_CLOSED = "DISPUTE_CLOSED"

    # Internal guards
    REJECTED_TRANSITION = "REJECTED_TRANSITION"


# ── Transition Matrix ──────────────────────────────────────────────────────────
# Maps (from_state, event) → to_state
# Only explicitly listed transitions are valid; everything else raises.

TRANSITION_MATRIX: dict[tuple[TransactionState, TransactionEvent], TransactionState] = {
    # Created → routing
    (TransactionState.CREATED, TransactionEvent.ROUTE_DECISION_MADE): TransactionState.ROUTE_SELECTED,
    (TransactionState.CREATED, TransactionEvent.ROUTE_DECISION_FAILED): TransactionState.ROUTE_FAILED,
    (TransactionState.CREATED, TransactionEvent.PAYMENT_ABANDONED): TransactionState.ABANDONED,

    # Route selected → auth
    (TransactionState.ROUTE_SELECTED, TransactionEvent.GATEWAY_AUTH_CALLED): TransactionState.AUTH_INITIATED,
    (TransactionState.ROUTE_SELECTED, TransactionEvent.ROUTE_DECISION_FAILED): TransactionState.ROUTE_FAILED,

    # Route failed → retry routing (failover)
    (TransactionState.ROUTE_FAILED, TransactionEvent.ROUTE_DECISION_MADE): TransactionState.ROUTE_SELECTED,
    (TransactionState.ROUTE_FAILED, TransactionEvent.MAX_RETRIES_EXCEEDED): TransactionState.FAILED,

    # Auth initiated → auth outcomes
    (TransactionState.AUTH_INITIATED, TransactionEvent.GATEWAY_AUTH_SUCCESS): TransactionState.AUTHORISED,
    (TransactionState.AUTH_INITIATED, TransactionEvent.GATEWAY_AUTH_DECLINED): TransactionState.AUTH_FAILED,
    (TransactionState.AUTH_INITIATED, TransactionEvent.GATEWAY_AUTH_TIMEOUT): TransactionState.AUTH_TIMEOUT,
    (TransactionState.AUTH_INITIATED, TransactionEvent.GATEWAY_AUTH_EXPIRED): TransactionState.AUTH_EXPIRED,
    # Webhook arrives before API response (FS-06)
    (TransactionState.AUTH_INITIATED, TransactionEvent.GATEWAY_CAPTURE_SUCCESS): TransactionState.CAPTURED,

    # Auth failed → retry routing or final failure
    (TransactionState.AUTH_FAILED, TransactionEvent.ROUTE_DECISION_MADE): TransactionState.ROUTE_SELECTED,
    (TransactionState.AUTH_FAILED, TransactionEvent.MAX_RETRIES_EXCEEDED): TransactionState.FAILED,

    # Auth timeout → retry routing
    (TransactionState.AUTH_TIMEOUT, TransactionEvent.ROUTE_DECISION_MADE): TransactionState.ROUTE_SELECTED,
    (TransactionState.AUTH_TIMEOUT, TransactionEvent.MAX_RETRIES_EXCEEDED): TransactionState.FAILED,

    # Authorised → capture or void
    (TransactionState.AUTHORISED, TransactionEvent.CAPTURE_REQUESTED): TransactionState.CAPTURE_INITIATED,
    (TransactionState.AUTHORISED, TransactionEvent.GATEWAY_CAPTURE_CALLED): TransactionState.CAPTURE_INITIATED,
    (TransactionState.AUTHORISED, TransactionEvent.VOID_REQUESTED): TransactionState.VOID_INITIATED,
    (TransactionState.AUTHORISED, TransactionEvent.GATEWAY_VOID_CALLED): TransactionState.VOID_INITIATED,
    (TransactionState.AUTHORISED, TransactionEvent.GATEWAY_AUTH_EXPIRED): TransactionState.AUTH_EXPIRED,

    # Capture initiated → outcomes
    (TransactionState.CAPTURE_INITIATED, TransactionEvent.GATEWAY_CAPTURE_SUCCESS): TransactionState.CAPTURED,
    (TransactionState.CAPTURE_INITIATED, TransactionEvent.GATEWAY_CAPTURE_PARTIAL): TransactionState.PARTIALLY_CAPTURED,
    (TransactionState.CAPTURE_INITIATED, TransactionEvent.GATEWAY_CAPTURE_FAILED): TransactionState.CAPTURE_FAILED,

    # Capture failed → retry or void
    (TransactionState.CAPTURE_FAILED, TransactionEvent.GATEWAY_CAPTURE_CALLED): TransactionState.CAPTURE_INITIATED,
    (TransactionState.CAPTURE_FAILED, TransactionEvent.VOID_REQUESTED): TransactionState.VOID_INITIATED,
    (TransactionState.CAPTURE_FAILED, TransactionEvent.MAX_RETRIES_EXCEEDED): TransactionState.FAILED,

    # Captured → refund or settlement
    (TransactionState.CAPTURED, TransactionEvent.REFUND_REQUESTED): TransactionState.REFUND_INITIATED,
    (TransactionState.CAPTURED, TransactionEvent.GATEWAY_REFUND_CALLED): TransactionState.REFUND_INITIATED,
    (TransactionState.CAPTURED, TransactionEvent.SETTLEMENT_CONFIRMED): TransactionState.SETTLED,

    # Partially captured → more capture, refund, or settlement
    (TransactionState.PARTIALLY_CAPTURED, TransactionEvent.GATEWAY_CAPTURE_CALLED): TransactionState.CAPTURE_INITIATED,
    (TransactionState.PARTIALLY_CAPTURED, TransactionEvent.REFUND_REQUESTED): TransactionState.REFUND_INITIATED,
    (TransactionState.PARTIALLY_CAPTURED, TransactionEvent.GATEWAY_REFUND_CALLED): TransactionState.REFUND_INITIATED,
    (TransactionState.PARTIALLY_CAPTURED, TransactionEvent.SETTLEMENT_CONFIRMED): TransactionState.SETTLED,
    (TransactionState.PARTIALLY_CAPTURED, TransactionEvent.VOID_REQUESTED): TransactionState.VOID_INITIATED,

    # Void initiated → outcomes
    (TransactionState.VOID_INITIATED, TransactionEvent.GATEWAY_VOID_SUCCESS): TransactionState.VOIDED,

    # Refund initiated → outcomes
    (TransactionState.REFUND_INITIATED, TransactionEvent.GATEWAY_REFUND_SUCCESS): TransactionState.REFUNDED,
    (TransactionState.REFUND_INITIATED, TransactionEvent.GATEWAY_REFUND_PARTIAL): TransactionState.PARTIALLY_REFUNDED,
    (TransactionState.REFUND_INITIATED, TransactionEvent.GATEWAY_REFUND_FAILED): TransactionState.REFUND_FAILED,

    # Partially refunded → more refund
    (TransactionState.PARTIALLY_REFUNDED, TransactionEvent.REFUND_REQUESTED): TransactionState.REFUND_INITIATED,
    (TransactionState.PARTIALLY_REFUNDED, TransactionEvent.GATEWAY_REFUND_CALLED): TransactionState.REFUND_INITIATED,

    # Refund failed → retry
    (TransactionState.REFUND_FAILED, TransactionEvent.GATEWAY_REFUND_CALLED): TransactionState.REFUND_INITIATED,
    (TransactionState.REFUND_FAILED, TransactionEvent.MAX_RETRIES_EXCEEDED): TransactionState.FAILED,

    # Settled → refund is still valid (FS-08)
    (TransactionState.SETTLED, TransactionEvent.REFUND_REQUESTED): TransactionState.REFUND_INITIATED,
    (TransactionState.SETTLED, TransactionEvent.GATEWAY_REFUND_CALLED): TransactionState.REFUND_INITIATED,
    (TransactionState.SETTLED, TransactionEvent.DISPUTE_RAISED): TransactionState.DISPUTE_OPENED,

    # Dispute states
    (TransactionState.DISPUTE_OPENED, TransactionEvent.DISPUTE_CLOSED): TransactionState.DISPUTE_RESOLVED,
}

# Terminal states — no further transitions possible
TERMINAL_STATES: frozenset[TransactionState] = frozenset({
    TransactionState.FAILED,
    TransactionState.ABANDONED,
    TransactionState.VOIDED,
    TransactionState.REFUNDED,
    TransactionState.DISPUTE_RESOLVED,
    TransactionState.AUTH_EXPIRED,
})


@dataclass
class TransitionResult:
    """Result of a state machine transition."""
    from_state: TransactionState
    to_state: TransactionState
    event: TransactionEvent
    transaction_id: str
    metadata: dict = field(default_factory=dict)


class TransactionStateMachine:
    """
    Finite state machine for payment transaction lifecycle.

    Usage:
        fsm = TransactionStateMachine()
        result = fsm.transition(
            current_state=TransactionState.CREATED,
            event=TransactionEvent.ROUTE_DECISION_MADE,
            transaction_id="txn_xxx",
        )
    """

    def transition(
        self,
        current_state: TransactionState,
        event: TransactionEvent,
        transaction_id: str,
        metadata: Optional[dict] = None,
    ) -> TransitionResult:
        """
        Attempt a state transition.

        Raises:
            InvalidStateTransitionError: if the transition is not defined in TRANSITION_MATRIX
        """
        if current_state in TERMINAL_STATES:
            raise InvalidStateTransitionError(
                from_state=current_state.value,
                to_state="<any>",
                transaction_id=transaction_id,
                valid_transitions=[],
            )

        key = (current_state, event)
        next_state = TRANSITION_MATRIX.get(key)

        if next_state is None:
            valid_transitions = self.get_valid_transitions(current_state)
            logger.warning(
                "rejected_state_transition",
                transaction_id=transaction_id,
                from_state=current_state.value,
                fsm_event=event.value,
                valid_events=[e.value for e in valid_transitions],
            )
            raise InvalidStateTransitionError(
                from_state=current_state.value,
                to_state=event.value,
                transaction_id=transaction_id,
                valid_transitions=[e.value for e in valid_transitions],
            )

        result = TransitionResult(
            from_state=current_state,
            to_state=next_state,
            event=event,
            transaction_id=transaction_id,
            metadata=metadata or {},
        )

        logger.info(
            "state_transition",
            transaction_id=transaction_id,
            from_state=current_state.value,
            to_state=next_state.value,
            fsm_fsm_event=event.value,
        )
        return result

    def get_valid_transitions(self, state: TransactionState) -> list[TransactionEvent]:
        """Return all valid events from the given state."""
        return [event for (s, event) in TRANSITION_MATRIX if s == state]

    def get_valid_next_states(self, state: TransactionState) -> list[TransactionState]:
        """Return all states reachable from the given state."""
        return [
            next_state
            for (s, _), next_state in TRANSITION_MATRIX.items()
            if s == state
        ]

    def is_terminal(self, state: TransactionState) -> bool:
        return state in TERMINAL_STATES

    def can_transition(self, current_state: TransactionState, event: TransactionEvent) -> bool:
        """Check if a transition is valid without raising."""
        return (current_state, event) in TRANSITION_MATRIX


# Module-level singleton
_fsm_instance: Optional[TransactionStateMachine] = None


def get_state_machine() -> TransactionStateMachine:
    global _fsm_instance
    if _fsm_instance is None:
        _fsm_instance = TransactionStateMachine()
    return _fsm_instance
