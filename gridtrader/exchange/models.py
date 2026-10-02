"""Exchange-boundary models with COIN-M-safe numeric semantics.

Adapters translate provider payloads into these models.  No Binance field
names, SDK classes, transport exceptions, or persistence concerns belong in
this module.
"""

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Optional

from gridtrader.core.clock import require_utc
from gridtrader.core.enums import (
    ExchangeOrderStatus,
    PositionSide,
    Side,
    TERMINAL_EXCHANGE_ORDER_STATUSES,
)
from gridtrader.core.errors import DomainValidationError
from gridtrader.core.types import (
    require_contracts,
    require_finite_decimal,
    require_non_empty,
)


def _normalize_optional_decimal(
    value: Optional[Decimal],
    field_name: str,
    *,
    positive: bool = False,
    non_negative: bool = False,
) -> Optional[Decimal]:
    if value is None:
        return None
    return require_finite_decimal(
        value,
        field_name,
        positive=positive,
        non_negative=non_negative,
    )


def _require_enum(value: object, enum_type: type[Enum], field_name: str) -> None:
    if not isinstance(value, enum_type):
        raise DomainValidationError(
            f"{field_name} must be {enum_type.__name__}"
        )


def _require_bool(value: object, field_name: str) -> None:
    if not isinstance(value, bool):
        raise DomainValidationError(f"{field_name} must be bool")


@dataclass(frozen=True)
class InstrumentRules:
    """Tradable contract metadata returned by the exchange.

    Prices and monetary values are Decimal.  Order quantities and steps are
    integer contract counts; an adapter must never expose base-asset floats as
    COIN-M order quantity.
    """

    symbol: str
    base_asset: str
    quote_asset: str
    margin_asset: str
    contract_type: str
    status: str
    price_tick: Decimal
    contract_size: Decimal
    contract_step: int
    min_contracts: int
    max_contracts: Optional[int]
    min_price: Optional[Decimal] = None
    max_price: Optional[Decimal] = None
    pair: Optional[str] = None
    supported_order_types: tuple[str, ...] = ()
    observed_at: Optional[datetime] = None
    rules_hash: Optional[str] = None

    def __post_init__(self) -> None:
        for field_name in (
            "symbol",
            "base_asset",
            "quote_asset",
            "margin_asset",
            "contract_type",
            "status",
        ):
            object.__setattr__(
                self,
                field_name,
                require_non_empty(getattr(self, field_name), field_name),
            )
        require_finite_decimal(self.price_tick, "price_tick", positive=True)
        require_finite_decimal(self.contract_size, "contract_size", positive=True)
        require_contracts(self.contract_step, "contract_step", positive=True)
        require_contracts(self.min_contracts, "min_contracts", positive=True)
        if self.max_contracts is not None:
            require_contracts(self.max_contracts, "max_contracts", positive=True)
            if self.max_contracts < self.min_contracts:
                raise DomainValidationError(
                    "max_contracts must be greater than or equal to min_contracts"
                )
        _normalize_optional_decimal(
            self.min_price, "min_price", non_negative=True
        )
        _normalize_optional_decimal(
            self.max_price, "max_price", positive=True
        )
        if (
            self.min_price is not None
            and self.max_price is not None
            and self.min_price >= self.max_price
        ):
            raise DomainValidationError("min_price must be less than max_price")
        if self.pair is not None:
            object.__setattr__(self, "pair", require_non_empty(self.pair, "pair"))
        if not isinstance(self.supported_order_types, tuple):
            raise DomainValidationError("supported_order_types must be tuple")
        normalized_order_types = tuple(
            require_non_empty(value, "supported_order_types item")
            for value in self.supported_order_types
        )
        if len(normalized_order_types) != len(set(normalized_order_types)):
            raise DomainValidationError("supported_order_types must not contain duplicates")
        object.__setattr__(self, "supported_order_types", normalized_order_types)
        if self.observed_at is not None:
            object.__setattr__(
                self,
                "observed_at",
                require_utc(self.observed_at, "observed_at"),
            )
        if self.rules_hash is not None:
            object.__setattr__(
                self,
                "rules_hash",
                require_non_empty(self.rules_hash, "rules_hash"),
            )

    @property
    def tick_size(self) -> Decimal:
        """Binance terminology alias for the exchange-neutral price tick."""

        return self.price_tick

    @property
    def quantity_step(self) -> int:
        """COIN-M quantity step expressed as an integer contract count."""

        return self.contract_step

    @property
    def min_qty(self) -> int:
        return self.min_contracts

    @property
    def max_qty(self) -> Optional[int]:
        return self.max_contracts

    def validate_order(self, price: Decimal, contracts: int) -> None:
        """Validate domain-level price and contract increments."""

        require_finite_decimal(price, "price", positive=True)
        require_contracts(contracts, "contracts", positive=True)
        if price % self.price_tick != 0:
            raise DomainValidationError("price is not aligned to price_tick")
        if contracts % self.contract_step != 0:
            raise DomainValidationError(
                "contracts is not aligned to contract_step"
            )
        if contracts < self.min_contracts:
            raise DomainValidationError("contracts is below min_contracts")
        if self.max_contracts is not None and contracts > self.max_contracts:
            raise DomainValidationError("contracts exceeds max_contracts")
        if self.min_price is not None and price < self.min_price:
            raise DomainValidationError("price is below min_price")
        if self.max_price is not None and price > self.max_price:
            raise DomainValidationError("price exceeds max_price")


@dataclass(frozen=True)
class ExchangeOrderSnapshot:
    """Authoritative exchange view of one order at a point in time."""

    symbol: str
    client_order_id: str
    exchange_order_id: str
    status: ExchangeOrderStatus
    side: Side
    position_side: PositionSide
    original_contracts: int
    filled_contracts: int
    update_time: datetime
    price: Optional[Decimal] = None
    average_fill_price: Optional[Decimal] = None
    reduce_only: bool = False
    order_type: str = "LIMIT"
    time_in_force: str = "GTC"

    def __post_init__(self) -> None:
        for field_name in ("symbol", "client_order_id", "exchange_order_id"):
            object.__setattr__(
                self,
                field_name,
                require_non_empty(getattr(self, field_name), field_name),
            )
        require_contracts(
            self.original_contracts, "original_contracts", positive=True
        )
        _require_enum(self.status, ExchangeOrderStatus, "status")
        _require_enum(self.side, Side, "side")
        _require_enum(self.position_side, PositionSide, "position_side")
        _require_bool(self.reduce_only, "reduce_only")
        for field_name in ("order_type", "time_in_force"):
            value = require_non_empty(getattr(self, field_name), field_name).upper()
            object.__setattr__(self, field_name, value)
        require_contracts(
            self.filled_contracts, "filled_contracts", non_negative=True
        )
        if self.filled_contracts > self.original_contracts:
            raise DomainValidationError(
                "filled_contracts cannot exceed original_contracts"
            )
        if (
            self.status in {ExchangeOrderStatus.UNKNOWN, ExchangeOrderStatus.NEW}
            and self.filled_contracts != 0
        ):
            raise DomainValidationError(
                "UNKNOWN and NEW orders cannot carry filled contracts"
            )
        if (
            self.status is ExchangeOrderStatus.PARTIALLY_FILLED
            and not 0 < self.filled_contracts < self.original_contracts
        ):
            raise DomainValidationError(
                "PARTIALLY_FILLED requires a strict partial contract count"
            )
        if (
            self.status is ExchangeOrderStatus.FILLED
            and self.filled_contracts != self.original_contracts
        ):
            raise DomainValidationError(
                "FILLED requires filled_contracts == original_contracts"
            )
        if (
            self.status is ExchangeOrderStatus.REJECTED
            and self.filled_contracts != 0
        ):
            raise DomainValidationError(
                "REJECTED orders cannot carry filled contracts"
            )
        _normalize_optional_decimal(self.price, "price", positive=True)
        _normalize_optional_decimal(
            self.average_fill_price,
            "average_fill_price",
            positive=True,
        )
        object.__setattr__(
            self, "update_time", require_utc(self.update_time, "update_time")
        )

    @property
    def remaining_contracts(self) -> int:
        return self.original_contracts - self.filled_contracts

    @property
    def is_terminal(self) -> bool:
        return self.status in TERMINAL_EXCHANGE_ORDER_STATUSES


@dataclass(frozen=True)
class ExchangeFill:
    """One immutable exchange fill, identified by the exchange trade ID."""

    account_id: str
    symbol: str
    trade_id: str
    exchange_order_id: str
    side: Side
    position_side: PositionSide
    price: Decimal
    contracts: int
    realized_pnl: Decimal
    commission: Decimal
    commission_asset: str
    trade_time: datetime
    client_order_id: Optional[str] = None
    base_amount: Optional[Decimal] = None
    quote_amount: Optional[Decimal] = None

    def __post_init__(self) -> None:
        for field_name in (
            "account_id",
            "symbol",
            "trade_id",
            "exchange_order_id",
            "commission_asset",
        ):
            object.__setattr__(
                self,
                field_name,
                require_non_empty(getattr(self, field_name), field_name),
            )
        if self.client_order_id is not None:
            object.__setattr__(
                self,
                "client_order_id",
                require_non_empty(self.client_order_id, "client_order_id"),
            )
        _require_enum(self.side, Side, "side")
        _require_enum(self.position_side, PositionSide, "position_side")
        require_finite_decimal(self.price, "price", positive=True)
        require_contracts(self.contracts, "contracts", positive=True)
        require_finite_decimal(self.realized_pnl, "realized_pnl")
        require_finite_decimal(self.commission, "commission", non_negative=True)
        _normalize_optional_decimal(
            self.base_amount, "base_amount", non_negative=True
        )
        _normalize_optional_decimal(
            self.quote_amount, "quote_amount", non_negative=True
        )
        object.__setattr__(
            self, "trade_time", require_utc(self.trade_time, "trade_time")
        )

    @property
    def deduplication_key(self) -> tuple[str, str, str]:
        """Binance trade IDs are scoped by account and symbol."""

        return self.account_id, self.symbol, self.trade_id


@dataclass(frozen=True)
class ExchangePosition:
    """Authoritative exchange position snapshot.

    ``contracts`` preserves the signed exchange position amount and remains an
    int.  Consumers may use ``absolute_contracts`` for size-only comparisons.
    """

    symbol: str
    position_side: PositionSide
    contracts: int
    entry_price: Decimal
    mark_price: Decimal
    unrealized_pnl: Decimal
    leverage: int
    margin_asset: str
    isolated: bool
    update_time: datetime
    liquidation_price: Optional[Decimal] = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "symbol", require_non_empty(self.symbol, "symbol"))
        object.__setattr__(
            self,
            "margin_asset",
            require_non_empty(self.margin_asset, "margin_asset"),
        )
        _require_enum(self.position_side, PositionSide, "position_side")
        _require_bool(self.isolated, "isolated")
        require_contracts(self.contracts, "contracts")
        require_finite_decimal(self.entry_price, "entry_price", non_negative=True)
        require_finite_decimal(self.mark_price, "mark_price", non_negative=True)
        require_finite_decimal(self.unrealized_pnl, "unrealized_pnl")
        require_contracts(self.leverage, "leverage", positive=True)
        _normalize_optional_decimal(
            self.liquidation_price,
            "liquidation_price",
            non_negative=True,
        )
        object.__setattr__(
            self, "update_time", require_utc(self.update_time, "update_time")
        )

    @property
    def absolute_contracts(self) -> int:
        return abs(self.contracts)


@dataclass(frozen=True)
class ExchangeMarginBalance:
    """Authoritative margin-asset balance snapshot."""

    asset: str
    wallet_balance: Decimal
    available_balance: Decimal
    unrealized_pnl: Decimal
    update_time: datetime

    def __post_init__(self) -> None:
        object.__setattr__(self, "asset", require_non_empty(self.asset, "asset"))
        require_finite_decimal(self.wallet_balance, "wallet_balance")
        require_finite_decimal(self.available_balance, "available_balance")
        require_finite_decimal(self.unrealized_pnl, "unrealized_pnl")
        object.__setattr__(
            self, "update_time", require_utc(self.update_time, "update_time")
        )


class PositionMode(str, Enum):
    """Account-level Binance position mode observed through the read port."""

    ONE_WAY = "one_way"
    HEDGE = "hedge"


@dataclass(frozen=True)
class PositionModeSnapshot:
    """An immutable observation of account position mode."""

    mode: PositionMode
    observed_at: datetime

    def __post_init__(self) -> None:
        _require_enum(self.mode, PositionMode, "mode")
        object.__setattr__(
            self,
            "observed_at",
            require_utc(self.observed_at, "observed_at"),
        )


@dataclass(frozen=True)
class MarginAccountSnapshot:
    """Authoritative COIN-M margin account observation.

    The per-asset balances remain available even when an exchange response does
    not expose every account-wide total. Missing optional totals are distinct
    from zero.
    """

    balances: tuple[ExchangeMarginBalance, ...]
    observed_at: datetime
    total_wallet_balance: Optional[Decimal] = None
    total_unrealized_pnl: Optional[Decimal] = None
    available_balance: Optional[Decimal] = None

    def __post_init__(self) -> None:
        if not isinstance(self.balances, tuple):
            raise DomainValidationError("balances must be tuple")
        if any(not isinstance(item, ExchangeMarginBalance) for item in self.balances):
            raise DomainValidationError(
                "balances must contain ExchangeMarginBalance values"
            )
        assets = tuple(item.asset for item in self.balances)
        if len(assets) != len(set(assets)):
            raise DomainValidationError("balances must not contain duplicate assets")
        object.__setattr__(
            self,
            "observed_at",
            require_utc(self.observed_at, "observed_at"),
        )
        for field_name in (
            "total_wallet_balance",
            "total_unrealized_pnl",
            "available_balance",
        ):
            _normalize_optional_decimal(getattr(self, field_name), field_name)


@dataclass(frozen=True)
class TradePage:
    """One replay page together with evidence about pagination completeness."""

    items: tuple[ExchangeFill, ...]
    next_cursor: Optional[str]
    complete: bool
    snapshot_time: datetime
    pagination_watermark: Optional[str]

    def __post_init__(self) -> None:
        if not isinstance(self.items, tuple):
            raise DomainValidationError("items must be tuple")
        if any(not isinstance(item, ExchangeFill) for item in self.items):
            raise DomainValidationError("items must contain ExchangeFill values")
        _require_bool(self.complete, "complete")
        if self.next_cursor is not None:
            object.__setattr__(
                self,
                "next_cursor",
                require_non_empty(self.next_cursor, "next_cursor"),
            )
        if self.complete and self.next_cursor is not None:
            raise DomainValidationError(
                "a complete trade page must not expose next_cursor"
            )
        if not self.complete and self.next_cursor is None:
            raise DomainValidationError(
                "an incomplete trade page must expose next_cursor"
            )
        object.__setattr__(
            self,
            "snapshot_time",
            require_utc(self.snapshot_time, "snapshot_time"),
        )
        if self.pagination_watermark is not None:
            object.__setattr__(
                self,
                "pagination_watermark",
                require_non_empty(
                    self.pagination_watermark,
                    "pagination_watermark",
                ),
            )


@dataclass(frozen=True)
class SubmitLimitOrder:
    """Exchange command DTO; ownership is carried by client_order_id."""

    symbol: str
    client_order_id: str
    side: Side
    position_side: PositionSide
    price: Decimal
    contracts: int
    reduce_only: bool = False
    time_in_force: str = "GTC"

    def __post_init__(self) -> None:
        for field_name in ("symbol", "client_order_id", "time_in_force"):
            object.__setattr__(
                self,
                field_name,
                require_non_empty(getattr(self, field_name), field_name),
            )
        _require_enum(self.side, Side, "side")
        _require_enum(self.position_side, PositionSide, "position_side")
        _require_bool(self.reduce_only, "reduce_only")
        require_finite_decimal(self.price, "price", positive=True)
        require_contracts(self.contracts, "contracts", positive=True)


@dataclass(frozen=True)
class CancelOrder:
    symbol: str
    client_order_id: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "symbol", require_non_empty(self.symbol, "symbol"))
        object.__setattr__(
            self,
            "client_order_id",
            require_non_empty(self.client_order_id, "client_order_id"),
        )


__all__ = [
    "CancelOrder",
    "ExchangeFill",
    "ExchangeMarginBalance",
    "ExchangeOrderSnapshot",
    "ExchangePosition",
    "InstrumentRules",
    "MarginAccountSnapshot",
    "PositionMode",
    "PositionModeSnapshot",
    "SubmitLimitOrder",
    "TradePage",
]
