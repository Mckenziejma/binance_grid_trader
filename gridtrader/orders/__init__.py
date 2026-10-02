"""Pure domain models for idempotent order management."""

from .idempotency import (
    CLIENT_ORDER_ID_MAX_LENGTH,
    LogicalSlotKey,
    build_client_order_id,
    canonical_logical_slot_key,
    client_order_id_from_key,
    is_valid_client_order_id,
    logical_slot_key,
    logical_slot_key_text,
    make_client_order_id,
    serialize_logical_slot_key,
    stable_client_order_id,
)
from .models import FillRecord, OrderIntent, OrderRecord
from .state_machine import (
    ALLOWED_LOCAL_TRANSITIONS,
    InvalidOrderTransition,
    InvalidOrderUpdate,
    OrderStateMachine,
    can_transition,
    transition_order,
    validate_transition,
)

__all__ = [
    "ALLOWED_LOCAL_TRANSITIONS",
    "CLIENT_ORDER_ID_MAX_LENGTH",
    "FillRecord",
    "InvalidOrderTransition",
    "InvalidOrderUpdate",
    "LogicalSlotKey",
    "OrderIntent",
    "OrderRecord",
    "OrderStateMachine",
    "build_client_order_id",
    "canonical_logical_slot_key",
    "can_transition",
    "client_order_id_from_key",
    "is_valid_client_order_id",
    "logical_slot_key",
    "logical_slot_key_text",
    "make_client_order_id",
    "serialize_logical_slot_key",
    "stable_client_order_id",
    "transition_order",
    "validate_transition",
]
