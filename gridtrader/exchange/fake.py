"""Read-only Phase 2 exchange adapter backed by deterministic fake truth."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any, Optional, Sequence

from gridtrader.core.errors import TradingDisabledError
from gridtrader.exchange.errors import (
    ExchangeAmbiguousResultError,
    ExchangeAuthError,
    ExchangeBannedError,
    ExchangeNotFoundError,
    ExchangePermanentRequestError,
    ExchangeRateLimitError,
    ExchangeTimeoutError,
    ExchangeUnavailableError,
    RequestClass,
)

from .fake_backend import FakeExchangeBackend, FakeFault, FakeFaultKind
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


READ_ONLY_MODE = True
TRADING_ENABLED = False
_TRADING_DISABLED_MESSAGE = "Trading disabled in Phase 2"


class FakeExchangeAdapter:
    """Application-facing adapter whose state lives entirely in its backend."""

    def __init__(self, backend: FakeExchangeBackend) -> None:
        if not isinstance(backend, FakeExchangeBackend):
            raise TypeError("backend must be FakeExchangeBackend")
        self._backend = backend

    @property
    def backend(self) -> FakeExchangeBackend:
        return self._backend

    def get_instrument_rules(self, symbol: str) -> InstrumentRules:
        fault = self._begin_read("get_instrument_rules")
        self._raise_fault(fault, "get_instrument_rules")
        rules = self._backend.get_instrument_rules(symbol)
        if rules is None:
            raise ExchangeNotFoundError(
                "instrument rules not found",
                code="FAKE_INSTRUMENT_NOT_FOUND",
                http_status=404,
                request_class=RequestClass.READ_ONLY,
            )
        return rules

    def get_open_orders(
        self,
        symbol: Optional[str] = None,
    ) -> Sequence[ExchangeOrderSnapshot]:
        fault = self._begin_read("get_open_orders")
        self._raise_fault(fault, "get_open_orders")
        return self._backend.get_open_orders(symbol)

    def get_positions(
        self,
        symbol: Optional[str] = None,
    ) -> Sequence[ExchangePosition]:
        fault = self._begin_read("get_positions")
        self._raise_fault(fault, "get_positions")
        return self._backend.get_positions(symbol)

    def get_margin_balances(self) -> Sequence[ExchangeMarginBalance]:
        return self.get_margin_account_snapshot().balances

    def get_margin_account_snapshot(self) -> MarginAccountSnapshot:
        fault = self._begin_read("get_margin_account_snapshot")
        self._raise_fault(fault, "get_margin_account_snapshot")
        return self._backend.get_margin_snapshot()

    def get_position_mode(self) -> PositionModeSnapshot:
        fault = self._begin_read("get_position_mode")
        self._raise_fault(fault, "get_position_mode")
        return self._backend.get_position_mode()

    def get_user_trades(
        self,
        symbol: str,
        *,
        cursor: Optional[str] = None,
        from_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        limit: Optional[int] = None,
    ) -> TradePage:
        fault = self._begin_read("get_user_trades")
        duplicate = fault is not None and fault.kind is FakeFaultKind.DUPLICATE_DATA
        out_of_order = (
            fault is not None and fault.kind is FakeFaultKind.OUT_OF_ORDER_DATA
        )
        self._raise_fault(fault, "get_user_trades")
        if duplicate or out_of_order:
            self._backend.configure_trade_page_data(
                duplicate_items=duplicate,
                out_of_order=out_of_order,
            )
        try:
            return self._backend.get_user_trades(
                symbol,
                cursor=cursor,
                from_time=from_time,
                end_time=end_time,
                limit=limit,
            )
        finally:
            if duplicate or out_of_order:
                self._backend.configure_trade_page_data()

    def get_order_by_client_id(
        self,
        symbol: str,
        client_order_id: str,
    ) -> ExchangeOrderSnapshot:
        fault = self._begin_read("get_order_by_client_id")
        if fault is not None and fault.kind is FakeFaultKind.MISSING_ORDER:
            raise self._not_found(client_order_id)
        self._raise_fault(fault, "get_order_by_client_id")
        order = self._backend.get_order(symbol, client_order_id)
        if order is None:
            raise self._not_found(client_order_id)
        return order

    def get_order_by_exchange_id(
        self,
        symbol: str,
        exchange_order_id: str,
    ) -> ExchangeOrderSnapshot:
        fault = self._begin_read("get_order_by_exchange_id")
        if fault is not None and fault.kind is FakeFaultKind.MISSING_ORDER:
            raise self._not_found(exchange_order_id, identity_name="exchange order id")
        self._raise_fault(fault, "get_order_by_exchange_id")
        order = self._backend.get_order_by_exchange_id(symbol, exchange_order_id)
        if order is None:
            raise self._not_found(exchange_order_id, identity_name="exchange order id")
        return order

    def submit_limit_order(self, command: SubmitLimitOrder) -> ExchangeOrderSnapshot:
        """Phase 2 has no write path, even when recovery is READY."""

        raise TradingDisabledError(_TRADING_DISABLED_MESSAGE)

    def cancel_order(self, command: CancelOrder) -> ExchangeOrderSnapshot:
        """Phase 2 has no write path, even when recovery is READY."""

        raise TradingDisabledError(_TRADING_DISABLED_MESSAGE)

    def _begin_read(self, operation: str) -> Optional[FakeFault]:
        fault = self._backend.take_fault(operation)
        if fault is not None and fault.kind is FakeFaultKind.TIMEOUT_BEFORE_REQUEST:
            return fault
        self._backend.mark_operation(
            operation,
            advance_read_sequence=(
                fault is None
                or fault.kind is not FakeFaultKind.DELAYED_VISIBILITY
            ),
        )
        return fault

    @staticmethod
    def _raise_fault(fault: Optional[FakeFault], operation: str) -> None:
        if fault is None or fault.kind in {
            FakeFaultKind.STALE_SNAPSHOT,
            FakeFaultKind.DELAYED_VISIBILITY,
            FakeFaultKind.DUPLICATE_DATA,
            FakeFaultKind.OUT_OF_ORDER_DATA,
        }:
            return

        message = fault.message or f"fake fault during {operation}"
        kwargs: dict[str, Any] = {
            "code": f"FAKE_{fault.kind.value.upper()}",
            "request_class": RequestClass.READ_ONLY,
        }
        if fault.kind is FakeFaultKind.TIMEOUT_BEFORE_REQUEST:
            raise ExchangeTimeoutError(message, **kwargs)
        if fault.kind in {
            FakeFaultKind.RESPONSE_LOST_AFTER_ACCEPT,
            FakeFaultKind.EXECUTION_UNKNOWN_503,
        }:
            raise ExchangeAmbiguousResultError(message, http_status=503, **kwargs)
        if fault.kind is FakeFaultKind.PAGINATION_INTERRUPTION:
            raise ExchangeUnavailableError(message, **kwargs)
        if fault.kind is FakeFaultKind.SERVICE_UNAVAILABLE_503:
            raise ExchangeUnavailableError(message, http_status=503, **kwargs)
        if fault.kind is FakeFaultKind.RATE_LIMIT_429:
            retry_after = (
                None
                if fault.retry_after_seconds is None
                else float(fault.retry_after_seconds)
            )
            raise ExchangeRateLimitError(
                message,
                http_status=429,
                retry_after_seconds=retry_after,
                **kwargs,
            )
        if fault.kind is FakeFaultKind.BANNED_418:
            raise ExchangeBannedError(message, http_status=418, **kwargs)
        if fault.kind is FakeFaultKind.AUTHENTICATION_ERROR:
            raise ExchangeAuthError(message, http_status=401, **kwargs)
        if fault.kind is FakeFaultKind.PERMANENT_PARAMETER_ERROR:
            raise ExchangePermanentRequestError(message, http_status=400, **kwargs)
        if fault.kind is FakeFaultKind.MISSING_ORDER:
            raise ExchangeNotFoundError(message, http_status=404, **kwargs)
        raise AssertionError(f"unhandled fake fault: {fault.kind}")

    @staticmethod
    def _not_found(
        identity: str,
        *,
        identity_name: str = "client order id",
    ) -> ExchangeNotFoundError:
        return ExchangeNotFoundError(
            f"order not found for {identity_name} {identity!r}",
            code="FAKE_ORDER_NOT_FOUND",
            http_status=404,
            request_class=RequestClass.READ_ONLY,
        )


__all__ = [
    "FakeExchangeAdapter",
    "READ_ONLY_MODE",
    "TRADING_ENABLED",
]
