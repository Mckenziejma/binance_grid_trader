"""Deterministic logical-slot and Binance client-order identifiers."""

import base64
import hashlib
import json
import re
from typing import TypeAlias


LogicalSlotKey: TypeAlias = tuple[str, str, str, int, str]

CLIENT_ORDER_ID_MAX_LENGTH = 36
CLIENT_ORDER_ID_PREFIX = "dg1_"
_CLIENT_ORDER_ID_PATTERN = re.compile(r"^[A-Za-z0-9._:/-]{1,36}$")


def _validated_text(value: str, field_name: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{field_name} must be a string")
    if not value or not value.strip():
        raise ValueError(f"{field_name} must not be empty")
    if value != value.strip():
        raise ValueError(f"{field_name} must not have surrounding whitespace")
    return value


def _validated_cycle_no(cycle_no: int) -> int:
    if isinstance(cycle_no, bool) or not isinstance(cycle_no, int):
        raise TypeError("cycle_no must be an integer")
    if cycle_no < 0:
        raise ValueError("cycle_no must be non-negative")
    return cycle_no


def logical_slot_key(
    strategy_id: str,
    generation_id: str,
    level_id: str,
    cycle_no: int,
    leg_role: str,
) -> LogicalSlotKey:
    """Return the complete semantic identity of one logical order slot."""
    return (
        _validated_text(strategy_id, "strategy_id"),
        _validated_text(generation_id, "generation_id"),
        _validated_text(level_id, "level_id"),
        _validated_cycle_no(cycle_no),
        _validated_text(leg_role, "leg_role"),
    )


def canonical_logical_slot_key(key: LogicalSlotKey) -> str:
    """Serialize a logical slot to a stable, versioned SQLite TEXT value."""
    if not isinstance(key, tuple) or len(key) != 5:
        raise TypeError("key must be a five-item LogicalSlotKey tuple")
    validated_key = logical_slot_key(key[0], key[1], key[2], key[3], key[4])
    return "v1:" + json.dumps(
        validated_key,
        ensure_ascii=False,
        separators=(",", ":"),
    )


def logical_slot_key_text(
    strategy_id: str,
    generation_id: str,
    level_id: str,
    cycle_no: int,
    leg_role: str,
) -> str:
    """Build the canonical persisted TEXT key from its semantic fields."""
    return canonical_logical_slot_key(
        logical_slot_key(
            strategy_id,
            generation_id,
            level_id,
            cycle_no,
            leg_role,
        )
    )


# Naming alias for callers that prefer an explicit serialization verb.
serialize_logical_slot_key = canonical_logical_slot_key


def client_order_id_from_key(key: LogicalSlotKey) -> str:
    """Derive a stable, Binance-safe ID from a validated semantic key.

    The versioned prefix leaves 32 base32 characters (160 digest bits) while
    keeping the complete identifier within Binance's 36-character limit.
    """
    if not isinstance(key, tuple) or len(key) != 5:
        raise TypeError("key must be a five-item LogicalSlotKey tuple")

    payload = canonical_logical_slot_key(key).encode("utf-8")
    digest = base64.b32encode(hashlib.sha256(payload).digest()).decode("ascii")
    client_order_id = CLIENT_ORDER_ID_PREFIX + digest.rstrip("=").lower()[:32]

    if len(client_order_id) > CLIENT_ORDER_ID_MAX_LENGTH:
        raise AssertionError("generated client order ID exceeds Binance limit")
    if not _CLIENT_ORDER_ID_PATTERN.fullmatch(client_order_id):
        raise AssertionError("generated client order ID contains unsafe characters")
    return client_order_id


def make_client_order_id(
    strategy_id: str,
    generation_id: str,
    level_id: str,
    cycle_no: int,
    leg_role: str,
) -> str:
    """Return a deterministic clientOrderId for one semantic order slot."""
    return client_order_id_from_key(
        logical_slot_key(
            strategy_id,
            generation_id,
            level_id,
            cycle_no,
            leg_role,
        )
    )


# Explicit aliases make the intended stable behavior discoverable to callers.
build_client_order_id = make_client_order_id
stable_client_order_id = make_client_order_id


def is_valid_client_order_id(value: str) -> bool:
    """Return whether a value fits the Binance clientOrderId envelope."""
    return (
        isinstance(value, str)
        and len(value) <= CLIENT_ORDER_ID_MAX_LENGTH
        and _CLIENT_ORDER_ID_PATTERN.fullmatch(value) is not None
    )
