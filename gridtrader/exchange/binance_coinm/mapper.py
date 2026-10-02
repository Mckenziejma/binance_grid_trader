"""Map official Binance COIN-M payloads into exchange-neutral models."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping

from gridtrader.core.enums import ExchangeOrderStatus, PositionSide, Side
from gridtrader.exchange.models import (
    ExchangeFill,
    ExchangeMarginBalance,
    ExchangeOrderSnapshot,
    ExchangePosition,
    InstrumentRules,
    MarginAccountSnapshot,
    PositionMode,
    PositionModeSnapshot,
)

from .errors import ExchangeInvalidResponseError
from .metadata import as_items, as_mapping, field, filter_by_type, rules_hash


_MISSING = object()


def _required_field(payload: Mapping[str, Any], *names: str) -> Any:
    value = field(payload, *names, default=_MISSING)
    if value is _MISSING or value is None:
        raise ExchangeInvalidResponseError(f"missing required field {names[0]}")
    return value


def _decimal(value: Any, name: str) -> Decimal:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ExchangeInvalidResponseError(f"invalid decimal field {name}") from exc
    if not result.is_finite():
        raise ExchangeInvalidResponseError(f"non-finite decimal field {name}")
    return result


def _integer(value: Any, name: str) -> int:
    number = _decimal(value, name)
    integral = number.to_integral_value()
    if number != integral:
        raise ExchangeInvalidResponseError(
            f"COIN-M {name} must be an integer contract count"
        )
    return int(integral)


def _text(value: Any, name: str) -> str:
    if value is None:
        raise ExchangeInvalidResponseError(f"missing text field {name}")
    result = str(value)
    if not result.strip():
        raise ExchangeInvalidResponseError(f"empty text field {name}")
    return result


def _boolean(value: Any, name: str) -> bool:
    if isinstance(value, bool):
        return value
    raise ExchangeInvalidResponseError(f"{name} must be boolean")


def _margin_isolated(value: Any) -> bool:
    normalized = _text(value, "marginType").lower()
    if normalized == "isolated":
        return True
    if normalized in {"cross", "crossed"}:
        return False
    raise ExchangeInvalidResponseError(f"unknown marginType {value!r}")


def _timestamp(value: Any, name: str) -> datetime:
    try:
        milliseconds = int(value)
        return datetime.fromtimestamp(milliseconds / 1000, tz=timezone.utc)
    except (TypeError, ValueError, OSError, OverflowError) as exc:
        raise ExchangeInvalidResponseError(f"invalid timestamp field {name}") from exc


def _side(value: Any) -> Side:
    try:
        return Side(str(value).lower())
    except ValueError as exc:
        raise ExchangeInvalidResponseError(f"unknown order side {value!r}") from exc


def _position_side(value: Any) -> PositionSide:
    try:
        return PositionSide(str(value).lower())
    except ValueError as exc:
        raise ExchangeInvalidResponseError(f"unknown position side {value!r}") from exc


def _order_status(value: Any) -> ExchangeOrderStatus:
    normalized = str(value).lower()
    try:
        return ExchangeOrderStatus(normalized)
    except ValueError as exc:
        raise ExchangeInvalidResponseError(f"unknown order status {value!r}") from exc


def _optional_positive(value: Any, name: str) -> Decimal | None:
    if value is None or not str(value).strip():
        return None
    number = _decimal(value, name)
    if number == 0:
        return None
    if number < 0:
        raise ExchangeInvalidResponseError(f"{name} must be positive when present")
    return number


def map_instrument_rules(payload: Any, *, observed_at: datetime) -> InstrumentRules:
    item = as_mapping(payload)
    price_filter = filter_by_type(item, "PRICE_FILTER")
    lot_filter = filter_by_type(item, "LOT_SIZE")
    raw_order_types = _required_field(item, "orderTypes", "order_types")
    if not isinstance(raw_order_types, (list, tuple)) or not raw_order_types:
        raise ExchangeInvalidResponseError(
            "orderTypes must be a non-empty sequence"
        )
    supported_order_types = tuple(
        _text(value, "orderTypes item").upper() for value in raw_order_types
    )
    if "LIMIT" not in supported_order_types:
        raise ExchangeInvalidResponseError(
            "COIN-M grid trading requires LIMIT order support"
        )
    return InstrumentRules(
        symbol=_text(field(item, "symbol"), "symbol"),
        pair=_text(field(item, "pair"), "pair"),
        base_asset=_text(field(item, "baseAsset", "base_asset"), "baseAsset"),
        quote_asset=_text(field(item, "quoteAsset", "quote_asset"), "quoteAsset"),
        margin_asset=_text(
            field(item, "marginAsset", "margin_asset"), "marginAsset"
        ),
        contract_type=_text(
            field(item, "contractType", "contract_type"), "contractType"
        ),
        status=_text(
            field(item, "contractStatus", "contract_status", "status"),
            "contractStatus",
        ),
        price_tick=_decimal(field(price_filter, "tickSize", "tick_size"), "tickSize"),
        contract_size=_decimal(
            field(item, "contractSize", "contract_size"), "contractSize"
        ),
        contract_step=_integer(
            field(lot_filter, "stepSize", "step_size"), "stepSize"
        ),
        min_contracts=_integer(field(lot_filter, "minQty", "min_qty"), "minQty"),
        max_contracts=_integer(field(lot_filter, "maxQty", "max_qty"), "maxQty"),
        min_price=_decimal(field(price_filter, "minPrice", "min_price"), "minPrice"),
        max_price=_decimal(field(price_filter, "maxPrice", "max_price"), "maxPrice"),
        supported_order_types=supported_order_types,
        observed_at=observed_at,
        rules_hash=rules_hash(item),
    )


def map_order(payload: Any, *, observed_at: datetime | None = None) -> ExchangeOrderSnapshot:
    item = as_mapping(payload)
    order_type = _text(
        _required_field(item, "type", "order_type"),
        "type",
    ).upper()
    original_type = field(item, "origType", "orig_type")
    if original_type is not None and _text(original_type, "origType").upper() != order_type:
        raise ExchangeInvalidResponseError(
            "Binance order type conflicts with origType"
        )
    return ExchangeOrderSnapshot(
        symbol=_text(_required_field(item, "symbol"), "symbol"),
        client_order_id=_text(
            _required_field(
                item,
                "clientOrderId",
                "client_order_id",
                "origClientOrderId",
                "orig_client_order_id",
            ),
            "clientOrderId",
        ),
        exchange_order_id=_text(
            _required_field(item, "orderId", "order_id"), "orderId"
        ),
        status=_order_status(_required_field(item, "status")),
        side=_side(_required_field(item, "side")),
        position_side=_position_side(
            _required_field(item, "positionSide", "position_side")
        ),
        original_contracts=_integer(
            _required_field(item, "origQty", "orig_qty", "quantity"),
            "origQty",
        ),
        filled_contracts=_integer(
            _required_field(item, "executedQty", "executed_qty"),
            "executedQty",
        ),
        update_time=_timestamp(
            _required_field(item, "updateTime", "update_time", "time"),
            "updateTime",
        ),
        price=_optional_positive(field(item, "price"), "price"),
        average_fill_price=_optional_positive(
            field(item, "avgPrice", "avg_price", "average_fill_price"),
            "avgPrice",
        ),
        reduce_only=_boolean(
            _required_field(item, "reduceOnly", "reduce_only"),
            "reduceOnly",
        ),
        order_type=order_type,
        time_in_force=_text(
            _required_field(item, "timeInForce", "time_in_force"),
            "timeInForce",
        ),
    )


def map_fill(payload: Any, *, account_id: str) -> ExchangeFill:
    item = as_mapping(payload)
    return ExchangeFill(
        account_id=account_id,
        symbol=_text(_required_field(item, "symbol"), "symbol"),
        trade_id=_text(
            _required_field(item, "id", "tradeId", "trade_id"), "tradeId"
        ),
        exchange_order_id=_text(
            _required_field(item, "orderId", "order_id"), "orderId"
        ),
        client_order_id=(
            None
            if field(item, "clientOrderId", "client_order_id") is None
            else str(field(item, "clientOrderId", "client_order_id"))
        ),
        side=_side(_required_field(item, "side")),
        position_side=_position_side(
            _required_field(item, "positionSide", "position_side")
        ),
        price=_decimal(_required_field(item, "price"), "price"),
        contracts=_integer(_required_field(item, "qty", "quantity"), "qty"),
        realized_pnl=_decimal(
            _required_field(item, "realizedPnl", "realized_pnl"),
            "realizedPnl",
        ),
        commission=_decimal(
            _required_field(item, "commission"), "commission"
        ),
        commission_asset=_text(
            _required_field(item, "commissionAsset", "commission_asset"),
            "commissionAsset",
        ),
        trade_time=_timestamp(_required_field(item, "time"), "time"),
        base_amount=(
            None
            if field(item, "baseQty", "base_qty") is None
            else _decimal(field(item, "baseQty", "base_qty"), "baseQty")
        ),
    )


def map_position(
    payload: Any,
    *,
    observed_at: datetime,
    margin_asset: str | None = None,
) -> ExchangePosition:
    item = as_mapping(payload)
    payload_margin_asset = field(item, "marginAsset", "margin_asset")
    if payload_margin_asset is None:
        resolved_margin_asset = _text(margin_asset, "marginAsset")
    else:
        resolved_margin_asset = _text(payload_margin_asset, "marginAsset")
        if (
            margin_asset is not None
            and resolved_margin_asset != _text(margin_asset, "marginAsset")
        ):
            raise ExchangeInvalidResponseError(
                "position marginAsset conflicts with exchangeInfo"
            )
    return ExchangePosition(
        symbol=_text(_required_field(item, "symbol"), "symbol"),
        position_side=_position_side(
            _required_field(item, "positionSide", "position_side")
        ),
        contracts=_integer(
            _required_field(item, "positionAmt", "position_amt"),
            "positionAmt",
        ),
        entry_price=_decimal(
            _required_field(item, "entryPrice", "entry_price"), "entryPrice"
        ),
        mark_price=_decimal(
            _required_field(item, "markPrice", "mark_price"), "markPrice"
        ),
        unrealized_pnl=_decimal(
            _required_field(
                item,
                "unRealizedProfit",
                "unrealizedProfit",
                "un_realized_profit",
                "unrealized_profit",
                "unrealized_pnl",
            ),
            "unRealizedProfit",
        ),
        leverage=_integer(_required_field(item, "leverage"), "leverage"),
        margin_asset=resolved_margin_asset,
        isolated=_margin_isolated(
            _required_field(item, "marginType", "margin_type")
        ),
        liquidation_price=_optional_positive(
            field(item, "liquidationPrice", "liquidation_price"),
            "liquidationPrice",
        ),
        update_time=_timestamp(
            _required_field(item, "updateTime", "update_time"),
            "updateTime",
        ),
    )


def map_position_mode(payload: Any, *, observed_at: datetime) -> PositionModeSnapshot:
    item = as_mapping(payload)
    value = _required_field(item, "dualSidePosition", "dual_side_position")
    value = _boolean(value, "dualSidePosition")
    return PositionModeSnapshot(
        PositionMode.HEDGE if value else PositionMode.ONE_WAY,
        observed_at,
    )


def map_margin_account(payload: Any, *, observed_at: datetime) -> MarginAccountSnapshot:
    account = as_mapping(payload)
    raw_assets = _required_field(account, "assets")
    assets = as_items(raw_assets) if not isinstance(raw_assets, list) else raw_assets
    balances = []
    for raw in assets:
        item = as_mapping(raw)
        balances.append(
            ExchangeMarginBalance(
                asset=_text(_required_field(item, "asset"), "asset"),
                wallet_balance=_decimal(
                    _required_field(item, "walletBalance", "wallet_balance"),
                    "walletBalance",
                ),
                available_balance=_decimal(
                    _required_field(
                        item,
                        "availableBalance",
                        "available_balance",
                    ),
                    "availableBalance",
                ),
                unrealized_pnl=_decimal(
                    _required_field(
                        item,
                        "unrealizedProfit",
                        "unRealizedProfit",
                        "unrealized_profit",
                        "un_realized_profit",
                    ),
                    "unrealizedProfit",
                ),
                update_time=_timestamp(
                    _required_field(item, "updateTime", "update_time"),
                    "updateTime",
                ),
            )
        )

    def optional_decimal(*names: str) -> Decimal | None:
        value = field(account, *names)
        return None if value is None else _decimal(value, names[0])

    return MarginAccountSnapshot(
        balances=tuple(balances),
        observed_at=observed_at,
        total_wallet_balance=optional_decimal("totalWalletBalance", "total_wallet_balance"),
        total_unrealized_pnl=optional_decimal(
            "totalUnrealizedProfit",
            "total_unrealized_profit",
            "total_unrealized_pnl",
        ),
        available_balance=optional_decimal("availableBalance", "available_balance"),
    )


__all__ = [
    "map_fill",
    "map_instrument_rules",
    "map_margin_account",
    "map_order",
    "map_position",
    "map_position_mode",
]
