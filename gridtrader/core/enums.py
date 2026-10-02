"""Stable domain enumerations shared by the new trading core.

This module deliberately has no dependency on exchange SDKs, persistence,
Qt, or the legacy trader implementation.  Values are serialized by their
lower-case strings so persisted records remain language-neutral.
"""

from enum import Enum
from typing import FrozenSet


class _DomainEnum(str, Enum):
    """String enum with predictable serialization and display."""

    def __str__(self) -> str:
        return self.value


class ReadinessState(_DomainEnum):
    CLOSED = "closed"
    RECOVERING = "recovering"
    READY = "ready"
    DEGRADED = "degraded"
    STOPPING = "stopping"
    STOPPED = "stopped"


class OrderLocalState(_DomainEnum):
    PLANNED = "planned"
    SUBMITTING = "submitting"
    ACK_UNKNOWN = "ack_unknown"
    ACTIVE = "active"
    CANCEL_PENDING = "cancel_pending"
    TERMINAL = "terminal"
    BLOCKED = "blocked"


class ExchangeOrderStatus(_DomainEnum):
    UNKNOWN = "unknown"
    NEW = "new"
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"
    CANCELED = "canceled"
    REJECTED = "rejected"
    EXPIRED = "expired"
    EXPIRED_IN_MATCH = "expired_in_match"


class StrategyMode(_DomainEnum):
    NEUTRAL = "neutral"
    LONG = "long"
    SHORT = "short"


class SpacingMode(_DomainEnum):
    ARITHMETIC = "arithmetic"
    GEOMETRIC = "geometric"


class GridGenerationStatus(_DomainEnum):
    DRAFT = "draft"
    PREPARING = "preparing"
    ACTIVE = "active"
    DRAINING = "draining"
    RETIRED = "retired"
    FAILED = "failed"


class GridLevelState(_DomainEnum):
    DORMANT = "dormant"
    ARMED = "armed"
    ACTIVE = "active"
    BLOCKED = "blocked"
    RETIRED = "retired"


class Side(_DomainEnum):
    BUY = "buy"
    SELL = "sell"


class PositionSide(_DomainEnum):
    BOTH = "both"
    LONG = "long"
    SHORT = "short"


TERMINAL_EXCHANGE_ORDER_STATUSES: FrozenSet[ExchangeOrderStatus] = frozenset(
    {
        ExchangeOrderStatus.FILLED,
        ExchangeOrderStatus.CANCELED,
        ExchangeOrderStatus.REJECTED,
        ExchangeOrderStatus.EXPIRED,
        ExchangeOrderStatus.EXPIRED_IN_MATCH,
    }
)

TERMINAL_LOCAL_ORDER_STATES: FrozenSet[OrderLocalState] = frozenset(
    {OrderLocalState.TERMINAL}
)

UNRESOLVED_LOCAL_ORDER_STATES: FrozenSet[OrderLocalState] = frozenset(
    state for state in OrderLocalState if state not in TERMINAL_LOCAL_ORDER_STATES
)


__all__ = [
    "ExchangeOrderStatus",
    "GridGenerationStatus",
    "GridLevelState",
    "OrderLocalState",
    "PositionSide",
    "ReadinessState",
    "Side",
    "SpacingMode",
    "StrategyMode",
    "TERMINAL_EXCHANGE_ORDER_STATUSES",
    "TERMINAL_LOCAL_ORDER_STATES",
    "UNRESOLVED_LOCAL_ORDER_STATES",
]
