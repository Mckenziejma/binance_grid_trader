"""Validated immutable order and fill domain records."""

from dataclasses import dataclass
from decimal import Decimal

from gridtrader.core.enums import (
    ExchangeOrderStatus,
    OrderLocalState,
    PositionSide,
    Side,
    TERMINAL_EXCHANGE_ORDER_STATUSES,
)

from .idempotency import (
    LogicalSlotKey,
    is_valid_client_order_id,
    logical_slot_key,
    logical_slot_key_text,
    make_client_order_id,
)


def _validate_text(value: str, field_name: str) -> None:
    if not isinstance(value, str):
        raise TypeError(f"{field_name} must be a string")
    if not value or not value.strip():
        raise ValueError(f"{field_name} must not be empty")
    if value != value.strip():
        raise ValueError(f"{field_name} must not have surrounding whitespace")


def _validate_optional_text(value: str | None, field_name: str) -> None:
    if value is not None:
        _validate_text(value, field_name)


def _validate_non_negative_int(value: int, field_name: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{field_name} must be an integer")
    if value < 0:
        raise ValueError(f"{field_name} must be non-negative")


def _validate_positive_int(value: int, field_name: str) -> None:
    _validate_non_negative_int(value, field_name)
    if value == 0:
        raise ValueError(f"{field_name} must be greater than zero")


def _validate_positive_decimal(value: Decimal, field_name: str) -> None:
    if not isinstance(value, Decimal):
        raise TypeError(f"{field_name} must be Decimal")
    if not value.is_finite() or value <= 0:
        raise ValueError(f"{field_name} must be finite and greater than zero")


@dataclass(frozen=True)
class OrderIntent:
    """The immutable intent for one logical grid-order slot."""

    strategy_id: str
    generation_id: str
    level_id: str
    cycle_no: int
    leg_role: str
    symbol: str
    side: Side
    position_side: PositionSide
    price: Decimal
    quantity_contracts: int
    reduce_only: bool = False

    def __post_init__(self) -> None:
        # logical_slot_key performs exact validation of all semantic-key fields.
        logical_slot_key(
            self.strategy_id,
            self.generation_id,
            self.level_id,
            self.cycle_no,
            self.leg_role,
        )
        _validate_text(self.symbol, "symbol")
        if not isinstance(self.side, Side):
            raise TypeError("side must be Side")
        if not isinstance(self.position_side, PositionSide):
            raise TypeError("position_side must be PositionSide")
        _validate_positive_decimal(self.price, "price")
        _validate_positive_int(self.quantity_contracts, "quantity_contracts")
        if not isinstance(self.reduce_only, bool):
            raise TypeError("reduce_only must be bool")

    @property
    def slot_key(self) -> LogicalSlotKey:
        return logical_slot_key(
            self.strategy_id,
            self.generation_id,
            self.level_id,
            self.cycle_no,
            self.leg_role,
        )

    @property
    def logical_slot_key(self) -> str:
        """Stable, versioned TEXT key suitable for persistence and uniqueness."""
        return logical_slot_key_text(
            self.strategy_id,
            self.generation_id,
            self.level_id,
            self.cycle_no,
            self.leg_role,
        )

    @property
    def client_order_id(self) -> str:
        return make_client_order_id(
            self.strategy_id,
            self.generation_id,
            self.level_id,
            self.cycle_no,
            self.leg_role,
        )


@dataclass(frozen=True)
class OrderRecord:
    """Current local and exchange state for an :class:`OrderIntent`."""

    intent: OrderIntent
    client_order_id: str = ""
    local_state: OrderLocalState = OrderLocalState.PLANNED
    exchange_status: ExchangeOrderStatus = ExchangeOrderStatus.UNKNOWN
    cumulative_filled_contracts: int = 0
    exchange_order_id: str | None = None
    last_error: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.intent, OrderIntent):
            raise TypeError("intent must be OrderIntent")

        expected_id = self.intent.client_order_id
        if not self.client_order_id:
            object.__setattr__(self, "client_order_id", expected_id)
        elif self.client_order_id != expected_id:
            raise ValueError("client_order_id does not match the intent semantic key")
        if not is_valid_client_order_id(self.client_order_id):
            raise ValueError("client_order_id is not Binance-safe")

        if not isinstance(self.local_state, OrderLocalState):
            raise TypeError("local_state must be OrderLocalState")
        if not isinstance(self.exchange_status, ExchangeOrderStatus):
            raise TypeError("exchange_status must be ExchangeOrderStatus")
        _validate_non_negative_int(
            self.cumulative_filled_contracts,
            "cumulative_filled_contracts",
        )
        if self.cumulative_filled_contracts > self.intent.quantity_contracts:
            raise ValueError("cumulative fill cannot exceed order quantity")

        local_is_terminal = self.local_state is OrderLocalState.TERMINAL
        exchange_is_terminal = (
            self.exchange_status in TERMINAL_EXCHANGE_ORDER_STATUSES
        )
        if local_is_terminal and not exchange_is_terminal:
            raise ValueError("TERMINAL local state requires a terminal exchange status")
        if not local_is_terminal and exchange_is_terminal:
            raise ValueError("terminal exchange status requires TERMINAL local state")

        if self.local_state in {
            OrderLocalState.PLANNED,
            OrderLocalState.SUBMITTING,
            OrderLocalState.ACK_UNKNOWN,
        }:
            if (
                self.exchange_status is not ExchangeOrderStatus.UNKNOWN
                or self.cumulative_filled_contracts != 0
            ):
                raise ValueError(
                    f"{self.local_state.value} requires UNKNOWN with zero fills"
                )
        elif self.local_state in {
            OrderLocalState.ACTIVE,
            OrderLocalState.CANCEL_PENDING,
        } and self.exchange_status not in {
            ExchangeOrderStatus.NEW,
            ExchangeOrderStatus.PARTIALLY_FILLED,
        }:
            raise ValueError(
                f"{self.local_state.value} requires NEW or PARTIALLY_FILLED"
            )

        if (
            self.exchange_status in {
                ExchangeOrderStatus.UNKNOWN,
                ExchangeOrderStatus.NEW,
            }
            and self.cumulative_filled_contracts != 0
        ):
            raise ValueError("UNKNOWN and NEW cannot carry a cumulative fill")

        if (
            self.exchange_status is ExchangeOrderStatus.PARTIALLY_FILLED
            and not 0 < self.cumulative_filled_contracts < self.intent.quantity_contracts
        ):
            raise ValueError("PARTIALLY_FILLED requires a strict partial fill")
        if (
            self.exchange_status is ExchangeOrderStatus.FILLED
            and self.cumulative_filled_contracts != self.intent.quantity_contracts
        ):
            raise ValueError("FILLED requires cumulative fill equal to order quantity")

        _validate_optional_text(self.exchange_order_id, "exchange_order_id")
        _validate_optional_text(self.last_error, "last_error")

    @property
    def remaining_contracts(self) -> int:
        return self.intent.quantity_contracts - self.cumulative_filled_contracts

    @property
    def is_terminal(self) -> bool:
        return self.local_state is OrderLocalState.TERMINAL


@dataclass(frozen=True)
class FillRecord:
    """One exchange fill, identified by a mandatory exchange trade ID."""

    trade_id: str
    client_order_id: str
    symbol: str
    side: Side
    position_side: PositionSide
    price: Decimal
    fill_contracts: int
    cumulative_filled_contracts: int
    exchange_order_id: str | None = None

    def __post_init__(self) -> None:
        _validate_text(self.trade_id, "trade_id")
        _validate_text(self.client_order_id, "client_order_id")
        if not is_valid_client_order_id(self.client_order_id):
            raise ValueError("client_order_id is not Binance-safe")
        _validate_text(self.symbol, "symbol")
        if not isinstance(self.side, Side):
            raise TypeError("side must be Side")
        if not isinstance(self.position_side, PositionSide):
            raise TypeError("position_side must be PositionSide")
        _validate_positive_decimal(self.price, "price")
        _validate_positive_int(self.fill_contracts, "fill_contracts")
        _validate_positive_int(
            self.cumulative_filled_contracts,
            "cumulative_filled_contracts",
        )
        if self.cumulative_filled_contracts < self.fill_contracts:
            raise ValueError("cumulative fill cannot be smaller than this fill")
        _validate_optional_text(self.exchange_order_id, "exchange_order_id")
