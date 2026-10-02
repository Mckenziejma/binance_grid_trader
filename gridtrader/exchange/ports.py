"""Ports implemented by exchange adapters.

The application depends on these protocols; Binance SDK and transport types
must remain behind the adapter boundary.
"""

from datetime import datetime
from typing import Optional, Protocol, Sequence, runtime_checkable

from .models import (
    CancelOrder,
    ExchangeFill,
    ExchangeMarginBalance,
    ExchangeOrderSnapshot,
    ExchangePosition,
    InstrumentRules,
    SubmitLimitOrder,
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
        """Return current authoritative margin balances."""

        raise NotImplementedError

    def get_user_trades(
        self,
        symbol: str,
        *,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        from_trade_id: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> Sequence[ExchangeFill]:
        """Return real fills for replay and gap recovery."""

        raise NotImplementedError

    def get_order_by_client_id(
        self, symbol: str, client_order_id: str
    ) -> Optional[ExchangeOrderSnapshot]:
        """Resolve an owned order, especially one in ACK_UNKNOWN."""

        raise NotImplementedError

    def submit_limit_order(self, command: SubmitLimitOrder) -> ExchangeOrderSnapshot:
        """Submit using the caller-owned client order ID."""

        raise NotImplementedError

    def cancel_order(self, command: CancelOrder) -> ExchangeOrderSnapshot:
        """Cancel by the caller-owned client order ID."""

        raise NotImplementedError


__all__ = ["ExchangePort"]
