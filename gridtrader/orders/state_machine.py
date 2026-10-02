"""Strict, pure order-state transitions and cumulative-fill validation."""

from dataclasses import replace

from gridtrader.core.enums import (
    ExchangeOrderStatus,
    OrderLocalState,
    TERMINAL_EXCHANGE_ORDER_STATUSES,
)

from .models import OrderRecord


TERMINAL_EXCHANGE_STATUSES = TERMINAL_EXCHANGE_ORDER_STATUSES

ALLOWED_LOCAL_TRANSITIONS: dict[OrderLocalState, frozenset[OrderLocalState]] = {
    OrderLocalState.PLANNED: frozenset({OrderLocalState.SUBMITTING}),
    OrderLocalState.SUBMITTING: frozenset(
        {
            OrderLocalState.ACTIVE,
            OrderLocalState.ACK_UNKNOWN,
            OrderLocalState.TERMINAL,
        }
    ),
    OrderLocalState.ACTIVE: frozenset(
        {
            OrderLocalState.ACTIVE,
            OrderLocalState.CANCEL_PENDING,
            OrderLocalState.TERMINAL,
            OrderLocalState.BLOCKED,
        }
    ),
    OrderLocalState.CANCEL_PENDING: frozenset(
        {
            OrderLocalState.CANCEL_PENDING,
            OrderLocalState.ACTIVE,
            OrderLocalState.TERMINAL,
            OrderLocalState.BLOCKED,
        }
    ),
    OrderLocalState.ACK_UNKNOWN: frozenset(
        {
            OrderLocalState.ACTIVE,
            OrderLocalState.TERMINAL,
            OrderLocalState.BLOCKED,
        }
    ),
    # Same-state replay is permitted solely as an idempotent terminal replay.
    OrderLocalState.TERMINAL: frozenset({OrderLocalState.TERMINAL}),
    # BLOCKED still owns its logical slot.  Only later exchange facts may
    # resolve it; generic resubmission is deliberately not exposed here.
    OrderLocalState.BLOCKED: frozenset(
        {OrderLocalState.ACTIVE, OrderLocalState.TERMINAL}
    ),
}


class InvalidOrderTransition(ValueError):
    """Raised when a local order-state transition is not permitted."""


class InvalidOrderUpdate(ValueError):
    """Raised when exchange status or cumulative fill regresses."""


def can_transition(
    current_state: OrderLocalState,
    target_state: OrderLocalState,
) -> bool:
    if not isinstance(current_state, OrderLocalState):
        raise TypeError("current_state must be OrderLocalState")
    if not isinstance(target_state, OrderLocalState):
        raise TypeError("target_state must be OrderLocalState")
    return target_state in ALLOWED_LOCAL_TRANSITIONS[current_state]


def validate_transition(
    current_state: OrderLocalState,
    target_state: OrderLocalState,
) -> None:
    if not can_transition(current_state, target_state):
        raise InvalidOrderTransition(
            f"invalid local order transition: {current_state.value} -> {target_state.value}"
        )


def _validate_cumulative_fill(record: OrderRecord, cumulative: int) -> None:
    if isinstance(cumulative, bool) or not isinstance(cumulative, int):
        raise TypeError("cumulative_filled_contracts must be an integer")
    if cumulative < 0:
        raise InvalidOrderUpdate("cumulative fill cannot be negative")
    if cumulative < record.cumulative_filled_contracts:
        raise InvalidOrderUpdate("cumulative fill cannot decrease")
    if cumulative > record.intent.quantity_contracts:
        raise InvalidOrderUpdate("cumulative fill cannot exceed order quantity")


def _validate_exchange_progression(
    current: ExchangeOrderStatus,
    target: ExchangeOrderStatus,
) -> None:
    if current in TERMINAL_EXCHANGE_STATUSES:
        if target is not current:
            raise InvalidOrderUpdate("terminal exchange status is immutable")
        return

    if (
        current is ExchangeOrderStatus.NEW
        and target is ExchangeOrderStatus.UNKNOWN
    ):
        raise InvalidOrderUpdate("exchange status cannot regress to UNKNOWN")
    if current is ExchangeOrderStatus.PARTIALLY_FILLED and target in {
        ExchangeOrderStatus.UNKNOWN,
        ExchangeOrderStatus.NEW,
    }:
        raise InvalidOrderUpdate("exchange status cannot regress after a partial fill")


def _validate_state_status_pair(
    target_state: OrderLocalState,
    exchange_status: ExchangeOrderStatus,
    cumulative: int,
    quantity: int,
) -> None:
    status_is_terminal = exchange_status in TERMINAL_EXCHANGE_STATUSES
    if target_state is OrderLocalState.TERMINAL and not status_is_terminal:
        raise InvalidOrderUpdate("TERMINAL local state requires terminal exchange status")
    if target_state is not OrderLocalState.TERMINAL and status_is_terminal:
        raise InvalidOrderUpdate("terminal exchange status requires TERMINAL local state")

    if target_state is OrderLocalState.SUBMITTING:
        if exchange_status is not ExchangeOrderStatus.UNKNOWN or cumulative != 0:
            raise InvalidOrderUpdate("SUBMITTING cannot have an exchange acknowledgement")
    elif target_state is OrderLocalState.ACK_UNKNOWN:
        if exchange_status is not ExchangeOrderStatus.UNKNOWN:
            raise InvalidOrderUpdate("ACK_UNKNOWN requires UNKNOWN exchange status")
    elif target_state is OrderLocalState.ACTIVE:
        if exchange_status not in {
            ExchangeOrderStatus.NEW,
            ExchangeOrderStatus.PARTIALLY_FILLED,
        }:
            raise InvalidOrderUpdate("ACTIVE requires NEW or PARTIALLY_FILLED status")
    elif target_state is OrderLocalState.CANCEL_PENDING:
        if exchange_status not in {
            ExchangeOrderStatus.NEW,
            ExchangeOrderStatus.PARTIALLY_FILLED,
        }:
            raise InvalidOrderUpdate(
                "CANCEL_PENDING requires NEW or PARTIALLY_FILLED status"
            )

    if exchange_status is ExchangeOrderStatus.UNKNOWN and cumulative != 0:
        raise InvalidOrderUpdate("UNKNOWN status cannot carry a fill")
    if exchange_status is ExchangeOrderStatus.NEW and cumulative != 0:
        raise InvalidOrderUpdate("NEW status cannot carry a fill")
    if (
        exchange_status is ExchangeOrderStatus.PARTIALLY_FILLED
        and not 0 < cumulative < quantity
    ):
        raise InvalidOrderUpdate(
            "PARTIALLY_FILLED requires cumulative fill between zero and quantity"
        )
    if exchange_status is ExchangeOrderStatus.FILLED and cumulative != quantity:
        raise InvalidOrderUpdate("FILLED requires cumulative fill equal to order quantity")


def transition_order(
    record: OrderRecord,
    target_state: OrderLocalState,
    *,
    exchange_status: ExchangeOrderStatus | None = None,
    cumulative_filled_contracts: int | None = None,
    exchange_order_id: str | None = None,
    last_error: str | None = None,
) -> OrderRecord:
    """Return a new record after a validated, monotonic transition."""
    if not isinstance(record, OrderRecord):
        raise TypeError("record must be OrderRecord")
    if not isinstance(target_state, OrderLocalState):
        raise TypeError("target_state must be OrderLocalState")
    if exchange_status is not None and not isinstance(
        exchange_status, ExchangeOrderStatus
    ):
        raise TypeError("exchange_status must be ExchangeOrderStatus")

    validate_transition(record.local_state, target_state)

    next_status = exchange_status or record.exchange_status
    next_cumulative = (
        record.cumulative_filled_contracts
        if cumulative_filled_contracts is None
        else cumulative_filled_contracts
    )
    _validate_cumulative_fill(record, next_cumulative)
    _validate_exchange_progression(record.exchange_status, next_status)

    if (
        exchange_order_id is not None
        and record.exchange_order_id is not None
        and exchange_order_id != record.exchange_order_id
    ):
        raise InvalidOrderUpdate("exchange_order_id is immutable once known")

    _validate_state_status_pair(
        target_state,
        next_status,
        next_cumulative,
        record.intent.quantity_contracts,
    )

    return replace(
        record,
        local_state=target_state,
        exchange_status=next_status,
        cumulative_filled_contracts=next_cumulative,
        exchange_order_id=(
            record.exchange_order_id
            if exchange_order_id is None
            else exchange_order_id
        ),
        last_error=record.last_error if last_error is None else last_error,
    )


class OrderStateMachine:
    """Namespace for the pure state-machine operations."""

    can_transition = staticmethod(can_transition)
    validate_transition = staticmethod(validate_transition)
    transition = staticmethod(transition_order)
