"""Ports implemented by exchange adapters.

The application depends on these protocols; Binance SDK and transport types
must remain behind the adapter boundary.
"""

from datetime import datetime
from typing import Optional, Protocol, Sequence, runtime_checkable

from .models import (
    CancelOrder,
    ExchangeMarginBalance,
    ExchangeOrderSnapshot,
    ExchangePosition,
    InstrumentRules,
    MarginAccountSnapshot,
    PositionModeSnapshot,
    SubmitLimitOrder,
    TradePage,
)


@runtime_checkable
class ExchangePort(Protocol):
    """Synchronous application port for commands and REST reconciliation."""

    def get_instrument_rules(self, symbol: str) -> InstrumentRules:
        """Return current exchange filters and COIN-M contract metadata."""

        raise NotImplementedError

    def get_open_orders(
        self, symbol: Optional[str] = None
    ) -> Sequence[ExchangeOrderSnapshot]:
        """Return all open orders, optionally restricted to one symbol."""

        raise NotImplementedError

    def get_positions(
        self, symbol: Optional[str] = None
    ) -> Sequence[ExchangePosition]:
        """Return current exchange positions."""

        raise NotImplementedError

    def get_margin_balances(self) -> Sequence[ExchangeMarginBalance]:
        """Return current authoritative balances (Phase 1 compatibility)."""

        raise NotImplementedError

    def get_margin_account_snapshot(self) -> MarginAccountSnapshot:
        """Return one coherent margin-account observation."""

        raise NotImplementedError

    def get_position_mode(self) -> PositionModeSnapshot:
        """Read the account-level one-way/hedge setting without changing it."""

        raise NotImplementedError

    def get_user_trades(
        self,
        symbol: str,
        *,
        cursor: Optional[str] = None,
        from_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        limit: Optional[int] = None,
    ) -> TradePage:
        """Return a resumable page and explicit completeness evidence."""

        raise NotImplementedError

    def get_order_by_client_id(
        self, symbol: str, client_order_id: str
    ) -> ExchangeOrderSnapshot:
        """Resolve an order or raise ExchangeNotFoundError.

        Transport failure must never be represented as a missing order.
        """

        raise NotImplementedError

    def get_order_by_exchange_id(
        self, symbol: str, exchange_order_id: str
    ) -> ExchangeOrderSnapshot:
        """Resolve an order by exchange identity or raise ExchangeNotFoundError.

        Transport failure must never be represented as a missing order.
        """

        raise NotImplementedError

    def submit_limit_order(self, command: SubmitLimitOrder) -> ExchangeOrderSnapshot:
        """Submit using the caller-owned client order ID."""

        raise NotImplementedError

    def cancel_order(self, command: CancelOrder) -> ExchangeOrderSnapshot:
        """Cancel by the caller-owned client order ID."""

        raise NotImplementedError


__all__ = ["ExchangePort"]
