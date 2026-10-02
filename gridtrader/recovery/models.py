"""Pure data returned by reconciliation."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Mapping, Optional

from gridtrader.core.enums import ExchangeOrderStatus, PositionSide
from gridtrader.exchange.models import ExchangeFill, ExchangeOrderSnapshot, ExchangePosition


class ReconciliationKind(str, Enum):
    MATCHED = "MATCHED"
    LOCAL_ONLY = "LOCAL_ONLY"
    EXCHANGE_ONLY = "EXCHANGE_ONLY"
    PARTIAL = "PARTIAL"
    TERMINAL = "TERMINAL"
    AMBIGUOUS = "AMBIGUOUS"
    EXTERNAL = "EXTERNAL"
    UNCLAIMED = "UNCLAIMED"
    POSITION_MISMATCH = "POSITION_MISMATCH"


class OrderOwnership(str, Enum):
    BOT = "BOT"
    EXTERNAL = "EXTERNAL"
    UNCLAIMED = "UNCLAIMED"


@dataclass(frozen=True)
class OrderResolution:
    client_order_id: str
    kind: ReconciliationKind
    ownership: OrderOwnership
    local: Optional[Mapping[str, Any]]
    exchange: Optional[ExchangeOrderSnapshot]
    effective_status: ExchangeOrderStatus
    cumulative_filled_contracts: int
    reason: str
    claim: Optional[Mapping[str, Any]] = None

    @property
    def blocks_ready(self) -> bool:
        return self.kind in {
            ReconciliationKind.LOCAL_ONLY,
            ReconciliationKind.AMBIGUOUS,
            ReconciliationKind.UNCLAIMED,
        }


@dataclass(frozen=True)
class PositionResolution:
    symbol: str
    position_side: PositionSide
    local_contracts: int
    exchange_contracts: int
    exchange: Optional[ExchangePosition]
    kind: ReconciliationKind

    @property
    def blocks_ready(self) -> bool:
        return self.kind is ReconciliationKind.POSITION_MISMATCH


@dataclass(frozen=True)
class ReconciliationReport:
    orders: tuple[OrderResolution, ...]
    fills: tuple[ExchangeFill, ...]
    positions: tuple[PositionResolution, ...]
    unmatched_fills: tuple[ExchangeFill, ...]
    blockers: tuple[str, ...]

    @property
    def mismatch_count(self) -> int:
        return sum(item.blocks_ready for item in self.orders) + sum(
            item.blocks_ready for item in self.positions
        )

    @property
    def ready_safe(self) -> bool:
        return not self.blockers


__all__ = [
    "OrderOwnership",
    "OrderResolution",
    "PositionResolution",
    "ReconciliationKind",
    "ReconciliationReport",
]
