"""Coherent read-only REST observation bundle."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from gridtrader.exchange.models import (
    ExchangeOrderSnapshot,
    ExchangePosition,
    InstrumentRules,
    MarginAccountSnapshot,
    PositionModeSnapshot,
    TradePage,
)


@dataclass(frozen=True)
class CoinMRestSnapshot:
    symbol: str
    observed_at: datetime
    instrument_rules: InstrumentRules
    position_mode: PositionModeSnapshot
    open_orders: tuple[ExchangeOrderSnapshot, ...]
    positions: tuple[ExchangePosition, ...]
    margin: MarginAccountSnapshot
    trades: TradePage

    @property
    def complete(self) -> bool:
        return self.trades.complete


__all__ = ["CoinMRestSnapshot"]
