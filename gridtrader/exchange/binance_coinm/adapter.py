"""ExchangePort adapter backed by Binance's official modular COIN-M SDK."""

from __future__ import annotations

import base64
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, Optional, Sequence

from gridtrader.core.runtime import require_trading_enabled
from gridtrader.exchange.models import (
    CancelOrder,
    ExchangeFill,
    ExchangeMarginBalance,
    ExchangeOrderSnapshot,
    ExchangePosition,
    InstrumentRules,
    MarginAccountSnapshot,
    PositionModeSnapshot,
    SubmitLimitOrder,
    TradePage,
)

from .connector import OfficialCoinMConnector
from .errors import ExchangeInvalidResponseError
from .mapper import (
    map_fill,
    map_instrument_rules,
    map_margin_account,
    map_order,
    map_position,
    map_position_mode,
)
from .metadata import as_items, as_mapping, field, select_perpetual_symbol


_TRADE_CURSOR_PREFIX = "coinm-trades-v2."
_MAX_TRADE_WINDOW_MS = 7 * 24 * 60 * 60 * 1000 - 1
# Binance documents userTrades as queryable only for the last three months.
# Ninety days is a conservative, deterministic floor for proving completeness.
_TRADE_HISTORY_RETENTION_MS = 90 * 24 * 60 * 60 * 1000


@dataclass(frozen=True)
class _TradeWindow:
    start_ms: int
    end_ms: int


@dataclass(frozen=True)
class _TradeCursor:
    account_id: str
    symbol: str
    snapshot_end_ms: int
    pending_windows: tuple[_TradeWindow, ...]
    last_trade_id: int | None = None


class CoinMExchangeAdapter:
    """Phase-2 read adapter; mutation methods fail before touching the SDK."""

    def __init__(
        self,
        connector: OfficialCoinMConnector,
        *,
        account_id: str,
        clock: Callable[[], datetime] | None = None,
        trade_page_limit: int = 1000,
    ) -> None:
        if not account_id or not account_id.strip():
            raise ValueError("account_id must not be empty")
        if trade_page_limit < 1 or trade_page_limit > 1000:
            raise ValueError("trade_page_limit must be between 1 and 1000")
        self._connector = connector
        self.account_id = account_id.strip()
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._trade_page_limit = trade_page_limit

    def _now(self) -> datetime:
        value = self._clock()
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("clock must return an aware datetime")
        return value.astimezone(timezone.utc)

    def get_instrument_rules(self, symbol: str) -> InstrumentRules:
        observed_at = self._now()
        raw = self._connector.exchange_information()
        selected = select_perpetual_symbol(raw, symbol)
        return map_instrument_rules(selected, observed_at=observed_at)

    def get_open_orders(
        self, symbol: Optional[str] = None
    ) -> Sequence[ExchangeOrderSnapshot]:
        observed_at = self._now()
        raw = self._connector.current_all_open_orders(symbol=symbol)
        orders = tuple(map_order(item, observed_at=observed_at) for item in as_items(raw))
        if symbol is not None and any(item.symbol != symbol for item in orders):
            raise ExchangeInvalidResponseError(
                "Binance openOrders returned a different symbol"
            )
        return orders

    def get_positions(self, symbol: Optional[str] = None) -> Sequence[ExchangePosition]:
        observed_at = self._now()
        # Binance's current COIN-M endpoint filters by pair, not by symbol.
        # Reading all and filtering exact symbol avoids conflating quarterly and
        # perpetual contracts that share a pair.
        raw = self._connector.position_information()
        raw_items = as_items(raw)
        exchange_info = self._connector.exchange_information()
        metadata_items = as_items(exchange_info, "symbols")
        margin_assets: dict[str, str] = {}
        for metadata_item in metadata_items:
            metadata = as_mapping(metadata_item)
            metadata_symbol = field(metadata, "symbol")
            margin_asset = field(metadata, "marginAsset", "margin_asset")
            if not isinstance(metadata_symbol, str) or not metadata_symbol.strip():
                raise ExchangeInvalidResponseError(
                    "exchangeInfo contains an invalid symbol"
                )
            if not isinstance(margin_asset, str) or not margin_asset.strip():
                raise ExchangeInvalidResponseError(
                    "exchangeInfo contains an invalid marginAsset"
                )
            previous = margin_assets.get(metadata_symbol)
            if previous is not None and previous != margin_asset:
                raise ExchangeInvalidResponseError(
                    "exchangeInfo contains conflicting marginAsset metadata"
                )
            margin_assets[metadata_symbol] = margin_asset
        if symbol is not None:
            # Validate that a symbol-specific bot request still targets an
            # actively trading COIN-M perpetual contract.
            select_perpetual_symbol(exchange_info, symbol)
            raw_items = [
                item
                for item in raw_items
                if field(as_mapping(item), "symbol") == symbol
            ]
        mapped_positions = []
        for item in raw_items:
            item_symbol = field(as_mapping(item), "symbol")
            mapped_positions.append(
                map_position(
                    item,
                    observed_at=observed_at,
                    margin_asset=margin_assets.get(str(item_symbol)),
                )
            )
        positions = tuple(mapped_positions)
        return (
            positions
            if symbol is None
            else tuple(item for item in positions if item.symbol == symbol)
        )

    def get_position_mode(self) -> PositionModeSnapshot:
        observed_at = self._now()
        return map_position_mode(
            self._connector.get_current_position_mode(), observed_at=observed_at
        )

    def get_margin_account_snapshot(self) -> MarginAccountSnapshot:
        observed_at = self._now()
        return map_margin_account(
            self._connector.account_information(), observed_at=observed_at
        )

    def get_margin_balances(self) -> Sequence[ExchangeMarginBalance]:
        return self.get_margin_account_snapshot().balances

    def get_user_trades(
        self,
        symbol: str,
        *,
        cursor: Optional[str] = None,
        from_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        limit: Optional[int] = None,
    ) -> TradePage:
        page_limit = self._trade_page_limit if limit is None else limit
        if isinstance(page_limit, bool) or page_limit < 1 or page_limit > 1000:
            raise ValueError("limit must be between 1 and 1000")
        if not isinstance(symbol, str) or not symbol.strip():
            raise ValueError("symbol must not be empty")
        symbol = symbol.strip()

        if cursor is not None:
            if from_time is not None or end_time is not None:
                raise ValueError(
                    "from_time and end_time must be omitted when resuming a cursor"
                )
            state = self._decode_trade_cursor(cursor)
            if state.account_id != self.account_id or state.symbol != symbol:
                raise ValueError("trade cursor does not belong to this account and symbol")
            server_now_ms = self._server_time_ms()
            if state.snapshot_end_ms > server_now_ms:
                raise ExchangeInvalidResponseError(
                    "COIN-M trade cursor snapshot is ahead of Binance server time"
                )
            if (
                state.pending_windows[0].start_ms
                < server_now_ms - _TRADE_HISTORY_RETENTION_MS
            ):
                raise ExchangeInvalidResponseError(
                    "COIN-M trade cursor is older than the provable userTrades retention"
                )
        else:
            if from_time is None:
                raise ValueError(
                    "from_time is required for an initial complete trade replay"
                )
            start_ms = self._datetime_ms(from_time, "from_time")
            server_now_ms = self._server_time_ms()
            snapshot_end_ms = (
                server_now_ms
                if end_time is None
                else min(self._datetime_ms(end_time, "end_time"), server_now_ms)
            )
            if start_ms > snapshot_end_ms:
                raise ValueError("from_time must not be after the snapshot end")
            if start_ms < server_now_ms - _TRADE_HISTORY_RETENTION_MS:
                raise ExchangeInvalidResponseError(
                    "from_time is older than the provable COIN-M userTrades retention"
                )
            state = _TradeCursor(
                account_id=self.account_id,
                symbol=symbol,
                snapshot_end_ms=snapshot_end_ms,
                pending_windows=self._partition_trade_windows(
                    start_ms,
                    snapshot_end_ms,
                ),
            )

        # A time-bounded Binance response does not promise that a full page
        # starts with the oldest trade in that interval.  Treating ``limit``
        # rows as a resumable ID page could therefore skip an older prefix.
        # Instead, keep a deterministic chronological queue of disjoint time
        # windows.  A window is complete only after Binance returns fewer than
        # ``limit`` rows.  Full windows are bisected and queried again without
        # ever depending on fromId semantics.
        pending_windows = list(state.pending_windows)
        while True:
            window = pending_windows[0]
            raw = self._connector.account_trade_list(
                symbol=symbol,
                start_time=window.start_ms,
                end_time=window.end_ms,
                limit=page_limit,
            )
            mapped = tuple(
                map_fill(item, account_id=self.account_id)
                for item in as_items(raw)
            )
            if len(mapped) > page_limit:
                raise ExchangeInvalidResponseError(
                    "Binance userTrades returned more rows than requested"
                )

            parsed_items: list[tuple[int, int, ExchangeFill]] = []
            for item in mapped:
                if item.symbol != symbol:
                    raise ExchangeInvalidResponseError(
                        "Binance userTrades returned a different symbol"
                    )
                try:
                    trade_id = int(item.trade_id)
                except ValueError as exc:
                    raise ExchangeInvalidResponseError(
                        "Binance trade ID cannot be used as a pagination watermark"
                    ) from exc
                if trade_id < 0 or str(trade_id) != item.trade_id:
                    raise ExchangeInvalidResponseError(
                        "Binance trade ID must be a canonical non-negative integer"
                    )
                trade_time_ms = self._datetime_ms(item.trade_time, "trade_time")
                if (
                    trade_time_ms < window.start_ms
                    or trade_time_ms > window.end_ms
                ):
                    raise ExchangeInvalidResponseError(
                        "Binance time-bounded userTrades returned an out-of-window trade"
                    )
                parsed_items.append((trade_time_ms, trade_id, item))

            if len({trade_id for _, trade_id, _ in parsed_items}) != len(
                parsed_items
            ):
                raise ExchangeInvalidResponseError(
                    "Binance userTrades returned duplicate trade IDs"
                )

            if len(mapped) == page_limit:
                if window.start_ms == window.end_ms:
                    raise ExchangeInvalidResponseError(
                        "Binance userTrades contains an unpageable full 1ms bucket"
                    )
                midpoint_ms = window.start_ms + (
                    window.end_ms - window.start_ms
                ) // 2
                pending_windows[0:1] = [
                    _TradeWindow(window.start_ms, midpoint_ms),
                    _TradeWindow(midpoint_ms + 1, window.end_ms),
                ]
                continue

            # A short response proves that this exact time window is complete.
            # Do not depend on the SDK/API response ordering: normalize the
            # accepted page chronologically, using the trade ID as the stable
            # tie-breaker for multiple fills in one millisecond.
            parsed_items.sort(key=lambda value: (value[0], value[1]))
            trade_ids = [trade_id for _, trade_id, _ in parsed_items]
            if any(
                right <= left for left, right in zip(trade_ids, trade_ids[1:])
            ):
                raise ExchangeInvalidResponseError(
                    "Binance userTrades IDs are inconsistent with trade time"
                )
            if state.last_trade_id is not None and any(
                trade_id <= state.last_trade_id for trade_id in trade_ids
            ):
                raise ExchangeInvalidResponseError(
                    "Binance userTrades did not advance beyond the cursor watermark"
                )

            last_trade_id = state.last_trade_id
            if trade_ids:
                last_trade_id = trade_ids[-1]
            remaining_windows = tuple(pending_windows[1:])
            next_state = (
                None
                if not remaining_windows
                else _TradeCursor(
                    account_id=state.account_id,
                    symbol=state.symbol,
                    snapshot_end_ms=state.snapshot_end_ms,
                    pending_windows=remaining_windows,
                    last_trade_id=last_trade_id,
                )
            )
            complete = next_state is None
            window_items = tuple(item for _, _, item in parsed_items)
            break

        snapshot_time = datetime.fromtimestamp(
            state.snapshot_end_ms / 1000,
            tz=timezone.utc,
        )
        return TradePage(
            items=window_items,
            next_cursor=(
                None
                if next_state is None
                else self._encode_trade_cursor(next_state)
            ),
            complete=complete,
            snapshot_time=snapshot_time,
            pagination_watermark=(
                None if last_trade_id is None else str(last_trade_id)
            ),
        )

    def get_order_by_client_id(
        self, symbol: str, client_order_id: str
    ) -> ExchangeOrderSnapshot:
        observed_at = self._now()
        raw = self._connector.query_order(
            symbol=symbol, orig_client_order_id=client_order_id
        )
        order = map_order(raw, observed_at=observed_at)
        if order.symbol != symbol or order.client_order_id != client_order_id:
            raise ExchangeInvalidResponseError(
                "Binance queryOrder response identity did not match the request"
            )
        return order

    def get_order_by_exchange_id(
        self, symbol: str, exchange_order_id: str
    ) -> ExchangeOrderSnapshot:
        if not isinstance(exchange_order_id, str) or not exchange_order_id:
            raise ValueError("exchange_order_id must not be empty")
        try:
            numeric_order_id = int(exchange_order_id)
        except ValueError as exc:
            raise ValueError("Binance exchange_order_id must be numeric") from exc
        if numeric_order_id < 0 or str(numeric_order_id) != exchange_order_id:
            raise ValueError("Binance exchange_order_id must be canonical")
        observed_at = self._now()
        raw = self._connector.query_order_by_order_id(
            symbol=symbol,
            order_id=numeric_order_id,
        )
        order = map_order(raw, observed_at=observed_at)
        if order.symbol != symbol or order.exchange_order_id != exchange_order_id:
            raise ExchangeInvalidResponseError(
                "Binance queryOrder response identity did not match the request"
            )
        return order

    @staticmethod
    def _datetime_ms(value: datetime, name: str) -> int:
        if not isinstance(value, datetime):
            raise ValueError(f"{name} must be datetime")
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError(f"{name} must be timezone-aware")
        return int(value.timestamp() * 1000)

    @staticmethod
    def _partition_trade_windows(
        start_ms: int,
        end_ms: int,
    ) -> tuple[_TradeWindow, ...]:
        windows = []
        next_start_ms = start_ms
        while next_start_ms <= end_ms:
            next_end_ms = min(
                next_start_ms + _MAX_TRADE_WINDOW_MS,
                end_ms,
            )
            windows.append(_TradeWindow(next_start_ms, next_end_ms))
            next_start_ms = next_end_ms + 1
        return tuple(windows)

    def _server_time_ms(self) -> int:
        try:
            payload = as_mapping(self._connector.check_server_time())
            value = field(payload, "serverTime", "server_time")
            if isinstance(value, bool):
                raise ValueError
            milliseconds = int(value)
            if milliseconds < 0 or str(milliseconds) != str(value):
                raise ValueError
            return milliseconds
        except (TypeError, ValueError) as exc:
            raise ExchangeInvalidResponseError(
                "Binance server-time response is invalid"
            ) from exc

    @staticmethod
    def _encode_trade_cursor(state: _TradeCursor) -> str:
        payload = {
            "v": 2,
            "account": state.account_id,
            "symbol": state.symbol,
            "snapshot_end_ms": state.snapshot_end_ms,
            "pending_windows": [
                [window.start_ms, window.end_ms]
                for window in state.pending_windows
            ],
            "last_trade_id": state.last_trade_id,
        }
        encoded = base64.urlsafe_b64encode(
            json.dumps(payload, separators=(",", ":"), sort_keys=True).encode(
                "utf-8"
            )
        ).decode("ascii")
        return _TRADE_CURSOR_PREFIX + encoded.rstrip("=")

    @staticmethod
    def _decode_trade_cursor(cursor: str) -> _TradeCursor:
        if not isinstance(cursor, str) or not cursor.startswith(_TRADE_CURSOR_PREFIX):
            raise ValueError("invalid COIN-M trade cursor")
        encoded = cursor.removeprefix(_TRADE_CURSOR_PREFIX)
        try:
            padding = "=" * (-len(encoded) % 4)
            decoded = base64.b64decode(
                encoded + padding,
                altchars=b"-_",
                validate=True,
            )
            payload = json.loads(decoded.decode("utf-8"))
            if (
                not isinstance(payload, dict)
                or isinstance(payload.get("v"), bool)
                or payload.get("v") != 2
            ):
                raise ValueError
            account_id = payload["account"]
            symbol = payload["symbol"]
            if not isinstance(account_id, str) or not account_id:
                raise ValueError
            if not isinstance(symbol, str) or not symbol:
                raise ValueError

            snapshot_end_ms = payload["snapshot_end_ms"]
            if (
                isinstance(snapshot_end_ms, bool)
                or not isinstance(snapshot_end_ms, int)
                or snapshot_end_ms < 0
            ):
                raise ValueError

            raw_windows = payload["pending_windows"]
            if not isinstance(raw_windows, list) or not raw_windows:
                raise ValueError
            windows = []
            previous_end_ms: int | None = None
            for raw_window in raw_windows:
                if not isinstance(raw_window, list) or len(raw_window) != 2:
                    raise ValueError
                start_ms, end_ms = raw_window
                if any(
                    isinstance(value, bool)
                    or not isinstance(value, int)
                    or value < 0
                    for value in (start_ms, end_ms)
                ):
                    raise ValueError
                if not start_ms <= end_ms <= snapshot_end_ms:
                    raise ValueError
                if end_ms - start_ms > _MAX_TRADE_WINDOW_MS:
                    raise ValueError
                if previous_end_ms is not None and start_ms != previous_end_ms + 1:
                    raise ValueError
                windows.append(_TradeWindow(start_ms, end_ms))
                previous_end_ms = end_ms
            if windows[-1].end_ms != snapshot_end_ms:
                raise ValueError

            last_trade_id = payload.get("last_trade_id")
            if last_trade_id is not None and (
                isinstance(last_trade_id, bool)
                or not isinstance(last_trade_id, int)
                or last_trade_id < 0
            ):
                raise ValueError
        except (KeyError, TypeError, ValueError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError("invalid COIN-M trade cursor") from exc
        return _TradeCursor(
            account_id=account_id,
            symbol=symbol,
            snapshot_end_ms=snapshot_end_ms,
            pending_windows=tuple(windows),
            last_trade_id=last_trade_id,
        )

    def submit_limit_order(self, command: SubmitLimitOrder) -> ExchangeOrderSnapshot:
        del command
        require_trading_enabled()
        raise AssertionError("unreachable")

    def cancel_order(self, command: CancelOrder) -> ExchangeOrderSnapshot:
        del command
        require_trading_enabled()
        raise AssertionError("unreachable")


__all__ = ["CoinMExchangeAdapter"]
