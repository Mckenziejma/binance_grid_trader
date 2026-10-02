"""Deterministic, in-memory exchange truth for recovery and adapter tests.

The backend deliberately outlives adapter instances.  Tests can therefore
destroy and rebuild the application-facing adapter while keeping the same
orders, fills, positions, and account observations -- the same boundary that
exists between a restarted process and Binance.

This module is a test control plane, not a trading implementation.  Phase 2
adapters reject every write command before it can reach this backend.
"""

from __future__ import annotations

import base64
import json
from collections import defaultdict, deque
from dataclasses import dataclass, replace
from datetime import datetime
from decimal import Decimal
from enum import Enum
from threading import RLock
from typing import Deque, Optional

from gridtrader.core.clock import Clock, SystemClock, require_utc
from gridtrader.core.enums import ExchangeOrderStatus, PositionSide, Side
from gridtrader.core.errors import DomainValidationError
from gridtrader.core.types import require_contracts, require_non_empty

from .errors import ExchangeAmbiguousResultError, RequestClass
from .models import (
    ExchangeFill,
    ExchangeOrderSnapshot,
    ExchangePosition,
    InstrumentRules,
    MarginAccountSnapshot,
    PositionMode,
    PositionModeSnapshot,
    TradePage,
)


class FakeFaultKind(str, Enum):
    """Faults a scenario can enqueue at a named adapter operation."""

    TIMEOUT_BEFORE_REQUEST = "timeout_before_request"
    RESPONSE_LOST_AFTER_ACCEPT = "response_lost_after_accept"
    STALE_SNAPSHOT = "stale_snapshot"
    MISSING_ORDER = "missing_order"
    PAGINATION_INTERRUPTION = "pagination_interruption"
    DUPLICATE_DATA = "duplicate_data"
    OUT_OF_ORDER_DATA = "out_of_order_data"
    DELAYED_VISIBILITY = "delayed_visibility"
    RATE_LIMIT_429 = "rate_limit_429"
    BANNED_418 = "banned_418"
    EXECUTION_UNKNOWN_503 = "execution_unknown_503"
    SERVICE_UNAVAILABLE_503 = "service_unavailable_503"
    AUTHENTICATION_ERROR = "authentication_error"
    PERMANENT_PARAMETER_ERROR = "permanent_parameter_error"


@dataclass(frozen=True)
class FakeFault:
    """One deterministic fault queued for a specific adapter operation."""

    kind: FakeFaultKind
    message: str = ""
    retry_after_seconds: Optional[Decimal] = None

    def __post_init__(self) -> None:
        if not isinstance(self.kind, FakeFaultKind):
            raise DomainValidationError("kind must be FakeFaultKind")
        if not isinstance(self.message, str):
            raise DomainValidationError("message must be string")
        if self.retry_after_seconds is not None:
            if (
                not isinstance(self.retry_after_seconds, Decimal)
                or not self.retry_after_seconds.is_finite()
                or self.retry_after_seconds < 0
            ):
                raise DomainValidationError(
                    "retry_after_seconds must be a non-negative Decimal"
                )


@dataclass(frozen=True)
class _StoredFill:
    sequence: int
    fill: ExchangeFill
    visible_after_read: int


@dataclass(frozen=True)
class _ReadView:
    rules: dict[str, InstrumentRules]
    orders: dict[tuple[str, str], ExchangeOrderSnapshot]
    positions: dict[tuple[str, PositionSide], ExchangePosition]
    margin: MarginAccountSnapshot
    position_mode: PositionModeSnapshot
    fills: tuple[_StoredFill, ...]


class FakeExchangeBackend:
    """Thread-safe authoritative fake exchange state.

    Adapter-local state is intentionally absent.  Recreating a
    :class:`FakeExchangeAdapter` over the same backend preserves every exchange
    fact and every pagination sequence number.
    """

    _CURSOR_PREFIX = "fake-trades-v4"
    _ORDER_READ_OPERATIONS = frozenset(
        {
            "get_open_orders",
            "get_order_by_client_id",
            "get_order_by_exchange_id",
        }
    )

    def __init__(
        self,
        account_id: str = "fake-coinm-account",
        *,
        clock: Optional[Clock] = None,
        default_page_size: int = 100,
    ) -> None:
        self.account_id = require_non_empty(account_id, "account_id")
        if (
            isinstance(default_page_size, bool)
            or not isinstance(default_page_size, int)
            or default_page_size <= 0
        ):
            raise DomainValidationError("default_page_size must be positive int")
        self._clock = clock or SystemClock()
        self._default_page_size = default_page_size
        self._lock = RLock()
        now = self._clock.now()
        self._rules: dict[str, InstrumentRules] = {}
        self._orders: dict[tuple[str, str], ExchangeOrderSnapshot] = {}
        self._order_id_index: dict[tuple[str, str], tuple[str, str]] = {}
        self._positions: dict[tuple[str, PositionSide], ExchangePosition] = {}
        self._margin = MarginAccountSnapshot(balances=(), observed_at=now)
        self._position_mode = PositionModeSnapshot(
            mode=PositionMode.ONE_WAY,
            observed_at=now,
        )
        self._fills: list[_StoredFill] = []
        self._trade_index: dict[tuple[str, str, str], ExchangeFill] = {}
        self._next_trade_sequence = 1
        self._order_read_sequence = 0
        self._trade_read_sequence = 0
        self._order_visible_after: dict[tuple[str, str], int] = {}
        self._faults: dict[str, Deque[FakeFault]] = defaultdict(deque)
        self._stale_view: Optional[_ReadView] = None
        self._duplicate_trade_pages = False
        self._out_of_order_trade_pages = False
        self._operation_calls: dict[str, int] = defaultdict(int)

    def now(self) -> datetime:
        return require_utc(self._clock.now(), "clock.now()")

    def operation_call_count(self, operation: str) -> int:
        operation = require_non_empty(operation, "operation")
        with self._lock:
            return self._operation_calls[operation]

    def mark_operation(
        self,
        operation: str,
        *,
        advance_read_sequence: bool = True,
    ) -> None:
        operation = require_non_empty(operation, "operation")
        if not isinstance(advance_read_sequence, bool):
            raise DomainValidationError("advance_read_sequence must be bool")
        with self._lock:
            self._operation_calls[operation] += 1
            if advance_read_sequence:
                if operation in self._ORDER_READ_OPERATIONS:
                    self._order_read_sequence += 1
                elif operation == "get_user_trades":
                    self._trade_read_sequence += 1

    def queue_fault(self, operation: str, fault: FakeFault) -> None:
        operation = require_non_empty(operation, "operation")
        if not isinstance(fault, FakeFault):
            raise DomainValidationError("fault must be FakeFault")
        with self._lock:
            if fault.kind is FakeFaultKind.STALE_SNAPSHOT:
                self.enable_stale_snapshots()
            self._faults[operation].append(fault)

    def take_fault(self, operation: str) -> Optional[FakeFault]:
        operation = require_non_empty(operation, "operation")
        with self._lock:
            queue = self._faults.get(operation)
            if not queue:
                return None
            fault = queue.popleft()
            if not queue:
                self._faults.pop(operation, None)
            return fault

    def set_instrument_rules(self, rules: InstrumentRules) -> None:
        if not isinstance(rules, InstrumentRules):
            raise DomainValidationError("rules must be InstrumentRules")
        with self._lock:
            self._rules[rules.symbol] = rules

    def get_instrument_rules(self, symbol: str) -> Optional[InstrumentRules]:
        symbol = require_non_empty(symbol, "symbol")
        with self._lock:
            return self._view().rules.get(symbol)

    def set_position_mode(
        self,
        mode: PositionMode,
        *,
        observed_at: Optional[datetime] = None,
    ) -> None:
        if not isinstance(mode, PositionMode):
            raise DomainValidationError("mode must be PositionMode")
        with self._lock:
            self._position_mode = PositionModeSnapshot(
                mode=mode,
                observed_at=observed_at or self.now(),
            )

    def get_position_mode(self) -> PositionModeSnapshot:
        with self._lock:
            return self._view().position_mode

    def set_margin_snapshot(self, snapshot: MarginAccountSnapshot) -> None:
        if not isinstance(snapshot, MarginAccountSnapshot):
            raise DomainValidationError("snapshot must be MarginAccountSnapshot")
        with self._lock:
            self._margin = snapshot

    def get_margin_snapshot(self) -> MarginAccountSnapshot:
        with self._lock:
            return self._view().margin

    def set_position(self, position: ExchangePosition) -> None:
        if not isinstance(position, ExchangePosition):
            raise DomainValidationError("position must be ExchangePosition")
        with self._lock:
            self._positions[(position.symbol, position.position_side)] = position

    def remove_position(self, symbol: str, position_side: PositionSide) -> None:
        symbol = require_non_empty(symbol, "symbol")
        if not isinstance(position_side, PositionSide):
            raise DomainValidationError("position_side must be PositionSide")
        with self._lock:
            self._positions.pop((symbol, position_side), None)

    def get_positions(self, symbol: Optional[str] = None) -> tuple[ExchangePosition, ...]:
        if symbol is not None:
            symbol = require_non_empty(symbol, "symbol")
        with self._lock:
            values = self._view().positions.values()
            return tuple(
                sorted(
                    (item for item in values if symbol is None or item.symbol == symbol),
                    key=lambda item: (item.symbol, item.position_side.value),
                )
            )

    def seed_order(
        self,
        order: ExchangeOrderSnapshot,
        *,
        visibility_delay_reads: int = 0,
    ) -> None:
        """Insert an exchange fact through the test-only control plane."""

        if not isinstance(order, ExchangeOrderSnapshot):
            raise DomainValidationError("order must be ExchangeOrderSnapshot")
        self._validate_delay(visibility_delay_reads)
        key = (order.symbol, order.client_order_id)
        with self._lock:
            if key in self._orders:
                raise DomainValidationError("duplicate client_order_id for symbol")
            exchange_key = (order.symbol, order.exchange_order_id)
            if exchange_key in self._order_id_index:
                raise DomainValidationError(
                    "duplicate exchange_order_id for symbol"
                )
            self._validate_order_position_mode(order)
            self._orders[key] = order
            self._order_id_index[exchange_key] = key
            self._order_visible_after[key] = self._order_visibility_threshold(
                visibility_delay_reads
            )

    def simulate_accepted_order_fact_then_ambiguous(
        self,
        order: ExchangeOrderSnapshot,
        *,
        fault_kind: FakeFaultKind,
    ) -> None:
        """Test-only control plane for accepted-but-unacknowledged writes.

        This deliberately is not part of ``ExchangePort`` and the Phase-2
        adapter never calls it.  It lets later phases prove that an ambiguous
        submit/cancel response leaves authoritative exchange truth to recover
        by the original identity.
        """

        if not isinstance(order, ExchangeOrderSnapshot):
            raise DomainValidationError("order must be ExchangeOrderSnapshot")
        if fault_kind not in {
            FakeFaultKind.RESPONSE_LOST_AFTER_ACCEPT,
            FakeFaultKind.EXECUTION_UNKNOWN_503,
        }:
            raise DomainValidationError(
                "accepted order simulation requires an ambiguous fault kind"
            )
        key = (order.symbol, order.client_order_id)
        with self._lock:
            existing = self._orders.get(key)
            if existing is None:
                self.seed_order(order)
            elif existing != order:
                if existing.exchange_order_id != order.exchange_order_id:
                    raise DomainValidationError(
                        "accepted order identity conflicts with exchange truth"
                    )
                self.replace_order(order)
            self._operation_calls["test_control_accepted_write"] += 1
        raise ExchangeAmbiguousResultError(
            "fake exchange accepted the write but acknowledgement was lost",
            code=f"FAKE_{fault_kind.value.upper()}",
            http_status=(
                503
                if fault_kind is FakeFaultKind.EXECUTION_UNKNOWN_503
                else None
            ),
            request_class=RequestClass.WRITE,
        )

    def replace_order(self, order: ExchangeOrderSnapshot) -> None:
        """Replace a seeded order while preserving both identity indexes."""

        if not isinstance(order, ExchangeOrderSnapshot):
            raise DomainValidationError("order must be ExchangeOrderSnapshot")
        key = (order.symbol, order.client_order_id)
        with self._lock:
            current = self._orders.get(key)
            if current is None:
                raise DomainValidationError("cannot replace an unknown order")
            if current.exchange_order_id != order.exchange_order_id:
                raise DomainValidationError("exchange_order_id is immutable")
            self._validate_order_position_mode(order)
            self._orders[key] = order

    def set_order_status(
        self,
        symbol: str,
        client_order_id: str,
        status: ExchangeOrderStatus,
        *,
        filled_contracts: Optional[int] = None,
        average_fill_price: Optional[Decimal] = None,
        update_time: Optional[datetime] = None,
    ) -> ExchangeOrderSnapshot:
        key = self._order_key(symbol, client_order_id)
        if not isinstance(status, ExchangeOrderStatus):
            raise DomainValidationError("status must be ExchangeOrderStatus")
        with self._lock:
            current = self._orders.get(key)
            if current is None:
                raise DomainValidationError("cannot update an unknown order")
            new_filled = (
                current.filled_contracts
                if filled_contracts is None
                else filled_contracts
            )
            updated = replace(
                current,
                status=status,
                filled_contracts=new_filled,
                average_fill_price=(
                    current.average_fill_price
                    if average_fill_price is None
                    else average_fill_price
                ),
                update_time=update_time or self.now(),
            )
            self._orders[key] = updated
            return updated

    def delay_order_visibility(
        self,
        symbol: str,
        client_order_id: str,
        read_count: int,
    ) -> None:
        self._validate_delay(read_count)
        key = self._order_key(symbol, client_order_id)
        with self._lock:
            if key not in self._orders:
                raise DomainValidationError("cannot delay an unknown order")
            self._order_visible_after[key] = self._order_visibility_threshold(
                read_count
            )

    def get_order(
        self,
        symbol: str,
        client_order_id: str,
    ) -> Optional[ExchangeOrderSnapshot]:
        key = self._order_key(symbol, client_order_id)
        with self._lock:
            order = self._view().orders.get(key)
            if order is None or not self._is_order_visible(key):
                return None
            return order

    def get_order_by_exchange_id(
        self,
        symbol: str,
        exchange_order_id: str,
    ) -> Optional[ExchangeOrderSnapshot]:
        symbol = require_non_empty(symbol, "symbol")
        exchange_order_id = require_non_empty(
            exchange_order_id, "exchange_order_id"
        )
        with self._lock:
            key = self._order_id_index.get((symbol, exchange_order_id))
            if key is None:
                return None
            order = self._view().orders.get(key)
            if order is None or not self._is_order_visible(key):
                return None
            return order

    def get_open_orders(
        self,
        symbol: Optional[str] = None,
    ) -> tuple[ExchangeOrderSnapshot, ...]:
        if symbol is not None:
            symbol = require_non_empty(symbol, "symbol")
        with self._lock:
            items = (
                order
                for key, order in self._view().orders.items()
                if (symbol is None or order.symbol == symbol)
                and not order.is_terminal
                and self._is_order_visible(key)
            )
            return tuple(
                sorted(
                    items,
                    key=lambda item: (item.symbol, item.client_order_id),
                )
            )

    def record_fill(
        self,
        symbol: str,
        client_order_id: str,
        *,
        contracts: int,
        price: Decimal,
        commission: Decimal = Decimal("0"),
        realized_pnl: Decimal = Decimal("0"),
        commission_asset: Optional[str] = None,
        trade_id: Optional[str] = None,
        trade_time: Optional[datetime] = None,
        visibility_delay_reads: int = 0,
    ) -> ExchangeFill:
        """Apply one real fill and update order and position facts atomically."""

        key = self._order_key(symbol, client_order_id)
        require_contracts(contracts, "contracts", positive=True)
        self._validate_delay(visibility_delay_reads)
        if not isinstance(price, Decimal) or not price.is_finite() or price <= 0:
            raise DomainValidationError("price must be a positive Decimal")
        for value, name, non_negative in (
            (commission, "commission", True),
            (realized_pnl, "realized_pnl", False),
        ):
            if not isinstance(value, Decimal) or not value.is_finite():
                raise DomainValidationError(f"{name} must be a finite Decimal")
            if non_negative and value < 0:
                raise DomainValidationError("commission must not be negative")

        with self._lock:
            order = self._orders.get(key)
            if order is None:
                raise DomainValidationError("cannot fill an unknown order")
            if order.is_terminal:
                raise DomainValidationError("cannot fill a terminal order")
            if contracts > order.remaining_contracts:
                raise DomainValidationError("fill exceeds remaining contracts")
            rules = self._rules.get(symbol)
            asset = commission_asset or (rules.margin_asset if rules else "UNKNOWN")
            sequence = self._next_trade_sequence
            normalized_trade_id = trade_id or f"fake-trade-{sequence:012d}"
            normalized_trade_id = require_non_empty(normalized_trade_id, "trade_id")
            dedupe_key = (self.account_id, symbol, normalized_trade_id)
            existing = self._trade_index.get(dedupe_key)
            if existing is not None:
                return existing

            new_filled = order.filled_contracts + contracts
            average = self._weighted_average_fill(order, contracts, price)
            position = self._project_position(order, contracts, price)
            fill = ExchangeFill(
                account_id=self.account_id,
                symbol=symbol,
                trade_id=normalized_trade_id,
                exchange_order_id=order.exchange_order_id,
                client_order_id=client_order_id,
                side=order.side,
                position_side=order.position_side,
                price=price,
                contracts=contracts,
                realized_pnl=realized_pnl,
                commission=commission,
                commission_asset=asset,
                trade_time=trade_time or self.now(),
            )
            updated_order = replace(
                order,
                status=(
                    ExchangeOrderStatus.FILLED
                    if new_filled == order.original_contracts
                    else ExchangeOrderStatus.PARTIALLY_FILLED
                ),
                filled_contracts=new_filled,
                average_fill_price=average,
                update_time=fill.trade_time,
            )

            self._orders[key] = updated_order
            self._positions[(position.symbol, position.position_side)] = position
            self._fills.append(
                _StoredFill(
                    sequence=sequence,
                    fill=fill,
                    visible_after_read=self._trade_visibility_threshold(
                        visibility_delay_reads
                    ),
                )
            )
            self._trade_index[dedupe_key] = fill
            self._next_trade_sequence += 1
            return fill

    def configure_trade_page_data(
        self,
        *,
        duplicate_items: bool = False,
        out_of_order: bool = False,
    ) -> None:
        """Configure deterministic bad-data delivery for reconciliation tests."""

        if not isinstance(duplicate_items, bool) or not isinstance(out_of_order, bool):
            raise DomainValidationError("trade page flags must be bool")
        with self._lock:
            self._duplicate_trade_pages = duplicate_items
            self._out_of_order_trade_pages = out_of_order

    def get_user_trades(
        self,
        symbol: str,
        *,
        cursor: Optional[str] = None,
        from_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        limit: Optional[int] = None,
    ) -> TradePage:
        symbol = require_non_empty(symbol, "symbol")
        if cursor is not None and (from_time is not None or end_time is not None):
            raise DomainValidationError(
                "cursor cannot be combined with from_time or end_time"
            )
        if from_time is not None:
            from_time = require_utc(from_time, "from_time")
        if end_time is not None:
            end_time = require_utc(end_time, "end_time")
        if from_time is not None and end_time is not None and from_time > end_time:
            raise DomainValidationError("from_time must not be after end_time")
        page_size = self._default_page_size if limit is None else limit
        if (
            isinstance(page_size, bool)
            or not isinstance(page_size, int)
            or page_size <= 0
        ):
            raise DomainValidationError("limit must be a positive int")

        with self._lock:
            view = self._view()
            if cursor is None:
                snapshot_read_sequence = self._trade_read_sequence
                watermark = max(
                    (
                        stored.sequence
                        for stored in view.fills
                        if stored.visible_after_read <= snapshot_read_sequence
                    ),
                    default=0,
                )
                offset = 0
                snapshot_time = self.now()
                cursor_from_time = from_time
                cursor_end_time = end_time
            else:
                (
                    watermark,
                    offset,
                    snapshot_time,
                    snapshot_read_sequence,
                    cursor_from_time,
                    cursor_end_time,
                ) = self._decode_cursor(cursor, expected_symbol=symbol)

            eligible = [
                stored
                for stored in view.fills
                if stored.sequence <= watermark
                and stored.visible_after_read <= snapshot_read_sequence
                and stored.fill.symbol == symbol
                and (
                    cursor_from_time is None
                    or stored.fill.trade_time >= cursor_from_time
                )
                and (
                    cursor_end_time is None
                    or stored.fill.trade_time <= cursor_end_time
                )
            ]
            selected = eligible[offset : offset + page_size]
            next_offset = offset + len(selected)
            complete = next_offset >= len(eligible)
            next_cursor = (
                None
                if complete
                else self._encode_cursor(
                    symbol,
                    watermark,
                    next_offset,
                    snapshot_time,
                    snapshot_read_sequence,
                    cursor_from_time,
                    cursor_end_time,
                )
            )
            items = [stored.fill for stored in selected]
            if self._out_of_order_trade_pages:
                items.reverse()
            if self._duplicate_trade_pages and items:
                items.append(items[0])
            return TradePage(
                items=tuple(items),
                next_cursor=next_cursor,
                complete=complete,
                snapshot_time=snapshot_time,
                pagination_watermark=str(watermark),
            )

    def enable_stale_snapshots(self) -> None:
        """Freeze reads at the current facts until explicitly disabled."""

        with self._lock:
            self._stale_view = _ReadView(
                rules=dict(self._rules),
                orders={
                    key: order
                    for key, order in self._orders.items()
                    if self._is_order_visible(key)
                },
                positions=dict(self._positions),
                margin=self._margin,
                position_mode=self._position_mode,
                fills=tuple(
                    replace(stored, visible_after_read=0)
                    for stored in self._fills
                    if stored.visible_after_read <= self._trade_read_sequence
                ),
            )

    def disable_stale_snapshots(self) -> None:
        with self._lock:
            self._stale_view = None

    def _view(self) -> _ReadView:
        if self._stale_view is not None:
            return self._stale_view
        return _ReadView(
            rules=self._rules,
            orders=self._orders,
            positions=self._positions,
            margin=self._margin,
            position_mode=self._position_mode,
            fills=tuple(self._fills),
        )

    def _is_order_visible(self, key: tuple[str, str]) -> bool:
        return (
            self._order_visible_after.get(key, 0)
            <= self._order_read_sequence
        )

    def _validate_order_position_mode(self, order: ExchangeOrderSnapshot) -> None:
        mode = self._position_mode.mode
        if mode is PositionMode.ONE_WAY and order.position_side is not PositionSide.BOTH:
            raise DomainValidationError("one-way mode requires position_side BOTH")
        if mode is PositionMode.HEDGE and order.position_side is PositionSide.BOTH:
            raise DomainValidationError("hedge mode requires LONG or SHORT position_side")

    def _project_position(
        self,
        order: ExchangeOrderSnapshot,
        fill_contracts: int,
        fill_price: Decimal,
    ) -> ExchangePosition:
        key = (order.symbol, order.position_side)
        current = self._positions.get(key)
        current_contracts = current.contracts if current else 0
        delta = fill_contracts if order.side is Side.BUY else -fill_contracts
        projected = current_contracts + delta
        if order.position_side is PositionSide.LONG and projected < 0:
            raise DomainValidationError("fill would cross a LONG hedge position")
        if order.position_side is PositionSide.SHORT and projected > 0:
            raise DomainValidationError("fill would cross a SHORT hedge position")

        if projected == 0:
            entry_price = Decimal("0")
        elif current is None or current_contracts == 0:
            entry_price = fill_price
        elif (current_contracts > 0) == (delta > 0):
            entry_price = (
                current.entry_price * abs(current_contracts)
                + fill_price * abs(delta)
            ) / Decimal(abs(projected))
        elif (current_contracts > 0) == (projected > 0):
            entry_price = current.entry_price
        else:
            entry_price = fill_price

        rules = self._rules.get(order.symbol)
        margin_asset = (
            current.margin_asset
            if current is not None
            else (rules.margin_asset if rules is not None else "UNKNOWN")
        )
        return ExchangePosition(
            symbol=order.symbol,
            position_side=order.position_side,
            contracts=projected,
            entry_price=entry_price,
            mark_price=fill_price,
            unrealized_pnl=Decimal("0"),
            leverage=current.leverage if current is not None else 1,
            margin_asset=margin_asset,
            isolated=current.isolated if current is not None else False,
            liquidation_price=(
                current.liquidation_price if current is not None else None
            ),
            update_time=self.now(),
        )

    @staticmethod
    def _weighted_average_fill(
        order: ExchangeOrderSnapshot,
        contracts: int,
        price: Decimal,
    ) -> Decimal:
        if order.filled_contracts == 0 or order.average_fill_price is None:
            return price
        return (
            order.average_fill_price * order.filled_contracts + price * contracts
        ) / Decimal(order.filled_contracts + contracts)

    @staticmethod
    def _order_key(symbol: str, client_order_id: str) -> tuple[str, str]:
        return (
            require_non_empty(symbol, "symbol"),
            require_non_empty(client_order_id, "client_order_id"),
        )

    @staticmethod
    def _validate_delay(value: int) -> None:
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise DomainValidationError("visibility delay must be a non-negative int")

    def _order_visibility_threshold(self, delay_reads: int) -> int:
        if delay_reads == 0:
            return self._order_read_sequence
        return self._order_read_sequence + delay_reads + 1

    def _trade_visibility_threshold(self, delay_reads: int) -> int:
        if delay_reads == 0:
            return self._trade_read_sequence
        return self._trade_read_sequence + delay_reads + 1

    def _encode_cursor(
        self,
        symbol: str,
        watermark: int,
        offset: int,
        snapshot_time: datetime,
        snapshot_read_sequence: int,
        from_time: Optional[datetime],
        end_time: Optional[datetime],
    ) -> str:
        payload = json.dumps(
            [
                self.account_id,
                require_non_empty(symbol, "symbol"),
                watermark,
                offset,
                int(snapshot_time.timestamp() * 1000),
                snapshot_read_sequence,
                None if from_time is None else int(from_time.timestamp() * 1000),
                None if end_time is None else int(end_time.timestamp() * 1000),
            ],
            separators=(",", ":"),
        ).encode("utf-8")
        token = base64.urlsafe_b64encode(payload).decode("ascii").rstrip("=")
        return f"{self._CURSOR_PREFIX}:{token}"

    def _decode_cursor(
        self,
        cursor: str,
        *,
        expected_symbol: str,
    ) -> tuple[
        int,
        int,
        datetime,
        int,
        Optional[datetime],
        Optional[datetime],
    ]:
        cursor = require_non_empty(cursor, "cursor")
        expected_symbol = require_non_empty(expected_symbol, "expected_symbol")
        try:
            prefix, token = cursor.split(":", 1)
            if prefix != self._CURSOR_PREFIX:
                raise ValueError
            padding = "=" * (-len(token) % 4)
            decoded = base64.b64decode(
                token + padding,
                altchars=b"-_",
                validate=True,
            )
            payload = json.loads(decoded.decode("utf-8"))
            if not isinstance(payload, list) or len(payload) != 8:
                raise ValueError
            (
                account_id,
                symbol,
                watermark,
                offset,
                milliseconds,
                snapshot_read_sequence,
                from_milliseconds,
                end_milliseconds,
            ) = payload
            if account_id != self.account_id or symbol != expected_symbol:
                raise ValueError
            if any(
                isinstance(value, bool) or not isinstance(value, int)
                for value in (
                    watermark,
                    offset,
                    milliseconds,
                    snapshot_read_sequence,
                )
            ):
                raise ValueError
            if min(watermark, offset, milliseconds, snapshot_read_sequence) < 0:
                raise ValueError
            for boundary in (from_milliseconds, end_milliseconds):
                if boundary is not None and (
                    isinstance(boundary, bool)
                    or not isinstance(boundary, int)
                    or boundary < 0
                ):
                    raise ValueError
            if (
                from_milliseconds is not None
                and end_milliseconds is not None
                and from_milliseconds > end_milliseconds
            ):
                raise ValueError
            snapshot_time = datetime.fromtimestamp(milliseconds / 1000).astimezone()
            snapshot_time = require_utc(snapshot_time, "cursor snapshot_time")
            decoded_from_time = (
                None
                if from_milliseconds is None
                else require_utc(
                    datetime.fromtimestamp(from_milliseconds / 1000).astimezone(),
                    "cursor from_time",
                )
            )
            decoded_end_time = (
                None
                if end_milliseconds is None
                else require_utc(
                    datetime.fromtimestamp(end_milliseconds / 1000).astimezone(),
                    "cursor end_time",
                )
            )
        except (
            TypeError,
            ValueError,
            UnicodeDecodeError,
            json.JSONDecodeError,
        ) as error:
            raise DomainValidationError("invalid fake trade cursor") from error
        return (
            watermark,
            offset,
            snapshot_time,
            snapshot_read_sequence,
            decoded_from_time,
            decoded_end_time,
        )


__all__ = [
    "FakeExchangeBackend",
    "FakeFault",
    "FakeFaultKind",
]
