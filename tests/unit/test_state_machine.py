"""
Unit Tests — Transaction State Machine

Tests every valid and invalid state transition.
Minimum: 20 valid transitions, 15 invalid transitions.
"""
import pytest
from app.domain.state_machine import (
    TransactionState,
    TransactionEvent,
    TransactionStateMachine,
    TERMINAL_STATES,
    TRANSITION_MATRIX,
)
from app.domain.exceptions import InvalidStateTransitionError


@pytest.fixture
def fsm():
    return TransactionStateMachine()


TXN_ID = "txn_test_12345"


# ── Valid Transition Tests ─────────────────────────────────────────────────────

class TestValidTransitions:

    def test_created_to_route_selected(self, fsm):
        result = fsm.transition(TransactionState.CREATED, TransactionEvent.ROUTE_DECISION_MADE, TXN_ID)
        assert result.to_state == TransactionState.ROUTE_SELECTED

    def test_created_to_abandoned(self, fsm):
        result = fsm.transition(TransactionState.CREATED, TransactionEvent.PAYMENT_ABANDONED, TXN_ID)
        assert result.to_state == TransactionState.ABANDONED

    def test_created_to_route_failed(self, fsm):
        result = fsm.transition(TransactionState.CREATED, TransactionEvent.ROUTE_DECISION_FAILED, TXN_ID)
        assert result.to_state == TransactionState.ROUTE_FAILED

    def test_route_selected_to_auth_initiated(self, fsm):
        result = fsm.transition(TransactionState.ROUTE_SELECTED, TransactionEvent.GATEWAY_AUTH_CALLED, TXN_ID)
        assert result.to_state == TransactionState.AUTH_INITIATED

    def test_auth_initiated_to_authorised(self, fsm):
        result = fsm.transition(TransactionState.AUTH_INITIATED, TransactionEvent.GATEWAY_AUTH_SUCCESS, TXN_ID)
        assert result.to_state == TransactionState.AUTHORISED

    def test_auth_initiated_to_auth_failed(self, fsm):
        result = fsm.transition(TransactionState.AUTH_INITIATED, TransactionEvent.GATEWAY_AUTH_DECLINED, TXN_ID)
        assert result.to_state == TransactionState.AUTH_FAILED

    def test_auth_initiated_to_auth_timeout(self, fsm):
        result = fsm.transition(TransactionState.AUTH_INITIATED, TransactionEvent.GATEWAY_AUTH_TIMEOUT, TXN_ID)
        assert result.to_state == TransactionState.AUTH_TIMEOUT

    def test_auth_initiated_to_auth_expired(self, fsm):
        result = fsm.transition(TransactionState.AUTH_INITIATED, TransactionEvent.GATEWAY_AUTH_EXPIRED, TXN_ID)
        assert result.to_state == TransactionState.AUTH_EXPIRED

    def test_auth_initiated_webhook_before_response(self, fsm):
        """FS-06: Webhook capture arrives before synchronous auth response."""
        result = fsm.transition(TransactionState.AUTH_INITIATED, TransactionEvent.GATEWAY_CAPTURE_SUCCESS, TXN_ID)
        assert result.to_state == TransactionState.CAPTURED

    def test_authorised_to_capture_initiated(self, fsm):
        result = fsm.transition(TransactionState.AUTHORISED, TransactionEvent.GATEWAY_CAPTURE_CALLED, TXN_ID)
        assert result.to_state == TransactionState.CAPTURE_INITIATED

    def test_authorised_to_void_initiated(self, fsm):
        result = fsm.transition(TransactionState.AUTHORISED, TransactionEvent.GATEWAY_VOID_CALLED, TXN_ID)
        assert result.to_state == TransactionState.VOID_INITIATED

    def test_capture_initiated_to_captured(self, fsm):
        result = fsm.transition(TransactionState.CAPTURE_INITIATED, TransactionEvent.GATEWAY_CAPTURE_SUCCESS, TXN_ID)
        assert result.to_state == TransactionState.CAPTURED

    def test_capture_initiated_to_partial(self, fsm):
        """FS-05: Partial capture."""
        result = fsm.transition(TransactionState.CAPTURE_INITIATED, TransactionEvent.GATEWAY_CAPTURE_PARTIAL, TXN_ID)
        assert result.to_state == TransactionState.PARTIALLY_CAPTURED

    def test_capture_initiated_to_failed(self, fsm):
        result = fsm.transition(TransactionState.CAPTURE_INITIATED, TransactionEvent.GATEWAY_CAPTURE_FAILED, TXN_ID)
        assert result.to_state == TransactionState.CAPTURE_FAILED

    def test_captured_to_refund_initiated(self, fsm):
        result = fsm.transition(TransactionState.CAPTURED, TransactionEvent.GATEWAY_REFUND_CALLED, TXN_ID)
        assert result.to_state == TransactionState.REFUND_INITIATED

    def test_captured_to_settled(self, fsm):
        result = fsm.transition(TransactionState.CAPTURED, TransactionEvent.SETTLEMENT_CONFIRMED, TXN_ID)
        assert result.to_state == TransactionState.SETTLED

    def test_settled_to_refund_initiated(self, fsm):
        """FS-08: Refund on settled transaction is valid."""
        result = fsm.transition(TransactionState.SETTLED, TransactionEvent.GATEWAY_REFUND_CALLED, TXN_ID)
        assert result.to_state == TransactionState.REFUND_INITIATED

    def test_refund_initiated_to_refunded(self, fsm):
        result = fsm.transition(TransactionState.REFUND_INITIATED, TransactionEvent.GATEWAY_REFUND_SUCCESS, TXN_ID)
        assert result.to_state == TransactionState.REFUNDED

    def test_refund_initiated_to_partially_refunded(self, fsm):
        result = fsm.transition(TransactionState.REFUND_INITIATED, TransactionEvent.GATEWAY_REFUND_PARTIAL, TXN_ID)
        assert result.to_state == TransactionState.PARTIALLY_REFUNDED

    def test_refund_initiated_to_refund_failed(self, fsm):
        result = fsm.transition(TransactionState.REFUND_INITIATED, TransactionEvent.GATEWAY_REFUND_FAILED, TXN_ID)
        assert result.to_state == TransactionState.REFUND_FAILED

    def test_void_initiated_to_voided(self, fsm):
        result = fsm.transition(TransactionState.VOID_INITIATED, TransactionEvent.GATEWAY_VOID_SUCCESS, TXN_ID)
        assert result.to_state == TransactionState.VOIDED

    def test_auth_failed_retry_routing(self, fsm):
        result = fsm.transition(TransactionState.AUTH_FAILED, TransactionEvent.ROUTE_DECISION_MADE, TXN_ID)
        assert result.to_state == TransactionState.ROUTE_SELECTED

    def test_capture_failed_retry(self, fsm):
        result = fsm.transition(TransactionState.CAPTURE_FAILED, TransactionEvent.GATEWAY_CAPTURE_CALLED, TXN_ID)
        assert result.to_state == TransactionState.CAPTURE_INITIATED

    def test_partially_captured_additional_capture(self, fsm):
        result = fsm.transition(TransactionState.PARTIALLY_CAPTURED, TransactionEvent.GATEWAY_CAPTURE_CALLED, TXN_ID)
        assert result.to_state == TransactionState.CAPTURE_INITIATED


# ── Invalid Transition Tests ─────────────────────────────────────────────────

class TestInvalidTransitions:
    """FS-15: State machine must reject all invalid transitions."""

    def test_created_cannot_go_to_refunded(self, fsm):
        """Critical: refund requires CAPTURED state first."""
        with pytest.raises(InvalidStateTransitionError) as exc_info:
            fsm.transition(TransactionState.CREATED, TransactionEvent.GATEWAY_REFUND_SUCCESS, TXN_ID)
        assert exc_info.value.from_state == "CREATED"

    def test_created_cannot_capture(self, fsm):
        with pytest.raises(InvalidStateTransitionError):
            fsm.transition(TransactionState.CREATED, TransactionEvent.GATEWAY_CAPTURE_SUCCESS, TXN_ID)

    def test_authorised_cannot_go_to_refunded(self, fsm):
        with pytest.raises(InvalidStateTransitionError):
            fsm.transition(TransactionState.AUTHORISED, TransactionEvent.GATEWAY_REFUND_SUCCESS, TXN_ID)

    def test_auth_initiated_cannot_void(self, fsm):
        with pytest.raises(InvalidStateTransitionError):
            fsm.transition(TransactionState.AUTH_INITIATED, TransactionEvent.GATEWAY_VOID_SUCCESS, TXN_ID)

    def test_failed_is_terminal_no_transitions(self, fsm):
        with pytest.raises(InvalidStateTransitionError):
            fsm.transition(TransactionState.FAILED, TransactionEvent.ROUTE_DECISION_MADE, TXN_ID)

    def test_refunded_is_terminal(self, fsm):
        with pytest.raises(InvalidStateTransitionError):
            fsm.transition(TransactionState.REFUNDED, TransactionEvent.GATEWAY_REFUND_CALLED, TXN_ID)

    def test_voided_is_terminal(self, fsm):
        with pytest.raises(InvalidStateTransitionError):
            fsm.transition(TransactionState.VOIDED, TransactionEvent.GATEWAY_CAPTURE_CALLED, TXN_ID)

    def test_abandoned_is_terminal(self, fsm):
        with pytest.raises(InvalidStateTransitionError):
            fsm.transition(TransactionState.ABANDONED, TransactionEvent.GATEWAY_AUTH_CALLED, TXN_ID)

    def test_captured_cannot_re_capture(self, fsm):
        with pytest.raises(InvalidStateTransitionError):
            fsm.transition(TransactionState.CAPTURED, TransactionEvent.GATEWAY_CAPTURE_SUCCESS, TXN_ID)

    def test_capture_failed_cannot_refund_directly(self, fsm):
        with pytest.raises(InvalidStateTransitionError):
            fsm.transition(TransactionState.CAPTURE_FAILED, TransactionEvent.GATEWAY_REFUND_SUCCESS, TXN_ID)

    def test_auth_expired_is_terminal(self, fsm):
        with pytest.raises(InvalidStateTransitionError):
            fsm.transition(TransactionState.AUTH_EXPIRED, TransactionEvent.GATEWAY_AUTH_CALLED, TXN_ID)

    def test_route_selected_cannot_capture(self, fsm):
        with pytest.raises(InvalidStateTransitionError):
            fsm.transition(TransactionState.ROUTE_SELECTED, TransactionEvent.GATEWAY_CAPTURE_SUCCESS, TXN_ID)

    def test_refund_initiated_cannot_capture(self, fsm):
        with pytest.raises(InvalidStateTransitionError):
            fsm.transition(TransactionState.REFUND_INITIATED, TransactionEvent.GATEWAY_CAPTURE_SUCCESS, TXN_ID)

    def test_invalid_transition_includes_valid_list(self, fsm):
        """Error message must include list of valid transitions."""
        with pytest.raises(InvalidStateTransitionError) as exc_info:
            fsm.transition(TransactionState.CREATED, TransactionEvent.GATEWAY_REFUND_SUCCESS, TXN_ID)
        assert len(exc_info.value.valid_transitions) > 0

    def test_invalid_transition_preserves_transaction_id(self, fsm):
        with pytest.raises(InvalidStateTransitionError) as exc_info:
            fsm.transition(TransactionState.CREATED, TransactionEvent.GATEWAY_REFUND_SUCCESS, TXN_ID)
        assert exc_info.value.transaction_id == TXN_ID


# ── State Machine Utility Tests ────────────────────────────────────────────────

class TestStateMachineUtils:

    def test_all_terminal_states_blocked(self, fsm):
        """Every terminal state must reject all events."""
        for terminal in TERMINAL_STATES:
            for event in TransactionEvent:
                with pytest.raises(InvalidStateTransitionError):
                    fsm.transition(terminal, event, TXN_ID)

    def test_get_valid_transitions_not_empty_for_non_terminal(self, fsm):
        non_terminal = [s for s in TransactionState if s not in TERMINAL_STATES]
        for state in non_terminal:
            transitions = fsm.get_valid_transitions(state)
            assert len(transitions) > 0, f"{state} should have valid transitions"

    def test_get_valid_transitions_empty_for_terminal(self, fsm):
        for state in TERMINAL_STATES:
            transitions = fsm.get_valid_transitions(state)
            assert len(transitions) == 0, f"Terminal {state} should have no transitions"

    def test_can_transition_returns_bool(self, fsm):
        assert fsm.can_transition(TransactionState.CREATED, TransactionEvent.ROUTE_DECISION_MADE) is True
        assert fsm.can_transition(TransactionState.CREATED, TransactionEvent.GATEWAY_REFUND_SUCCESS) is False

    def test_is_terminal_correct(self, fsm):
        assert fsm.is_terminal(TransactionState.FAILED) is True
        assert fsm.is_terminal(TransactionState.REFUNDED) is True
        assert fsm.is_terminal(TransactionState.CREATED) is False
        assert fsm.is_terminal(TransactionState.CAPTURED) is False

    def test_transition_result_contains_correct_states(self, fsm):
        result = fsm.transition(TransactionState.CREATED, TransactionEvent.ROUTE_DECISION_MADE, TXN_ID)
        assert result.from_state == TransactionState.CREATED
        assert result.to_state == TransactionState.ROUTE_SELECTED
        assert result.transaction_id == TXN_ID

    def test_transition_matrix_coverage(self):
        """All entries in TRANSITION_MATRIX should use valid state/event enums."""
        state_values = {s.value for s in TransactionState}
        event_values = {e.value for e in TransactionEvent}
        for (state, event), next_state in TRANSITION_MATRIX.items():
            assert state.value in state_values, f"Invalid from_state: {state}"
            assert event.value in event_values, f"Invalid event: {event}"
            assert next_state.value in state_values, f"Invalid to_state: {next_state}"
