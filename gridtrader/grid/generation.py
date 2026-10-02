"""Grid-generation metadata without price-calculation behavior."""

from dataclasses import dataclass
from decimal import Decimal

from gridtrader.core.enums import GridGenerationStatus, SpacingMode, StrategyMode


def _validate_text(value: str, field_name: str) -> None:
    if not isinstance(value, str):
        raise TypeError(f"{field_name} must be a string")
    if not value or not value.strip():
        raise ValueError(f"{field_name} must not be empty")
    if value != value.strip():
        raise ValueError(f"{field_name} must not have surrounding whitespace")


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
class GridGeneration:
    """Persistable configuration identity for one version of a grid."""

    generation_id: str
    strategy_id: str
    generation_no: int
    lower_price: Decimal
    upper_price: Decimal
    logical_level_count: int
    strategy_mode: StrategyMode
    spacing_mode: SpacingMode
    order_contracts: int
    max_active_orders: int
    status: GridGenerationStatus
    arithmetic_step: Decimal | None = None
    geometric_ratio: Decimal | None = None

    def __post_init__(self) -> None:
        _validate_text(self.generation_id, "generation_id")
        _validate_text(self.strategy_id, "strategy_id")
        _validate_positive_int(self.generation_no, "generation_no")
        if not isinstance(self.lower_price, Decimal):
            raise TypeError("lower_price must be Decimal")
        if not isinstance(self.upper_price, Decimal):
            raise TypeError("upper_price must be Decimal")
        if not self.lower_price.is_finite() or self.lower_price <= 0:
            raise ValueError("lower_price must be finite and greater than zero")
        if not self.upper_price.is_finite() or self.upper_price <= self.lower_price:
            raise ValueError("upper_price must be finite and greater than lower_price")
        _validate_positive_int(self.logical_level_count, "logical_level_count")
        _validate_positive_int(self.order_contracts, "order_contracts")
        _validate_positive_int(self.max_active_orders, "max_active_orders")
        if self.max_active_orders > self.logical_level_count:
            raise ValueError("max_active_orders cannot exceed logical_level_count")
        if not isinstance(self.strategy_mode, StrategyMode):
            raise TypeError("strategy_mode must be StrategyMode")
        if not isinstance(self.spacing_mode, SpacingMode):
            raise TypeError("spacing_mode must be SpacingMode")
        if self.spacing_mode is SpacingMode.ARITHMETIC:
            if self.geometric_ratio is not None:
                raise ValueError("arithmetic generation cannot have geometric_ratio")
            if self.arithmetic_step is None:
                raise ValueError("arithmetic generation requires arithmetic_step")
            _validate_positive_decimal(self.arithmetic_step, "arithmetic_step")
        else:
            if self.arithmetic_step is not None:
                raise ValueError("geometric generation cannot have arithmetic_step")
            if self.geometric_ratio is None:
                raise ValueError("geometric generation requires geometric_ratio")
            _validate_positive_decimal(self.geometric_ratio, "geometric_ratio")
            if self.geometric_ratio <= 1:
                raise ValueError("geometric_ratio must be greater than one")
        if not isinstance(self.status, GridGenerationStatus):
            raise TypeError("status must be GridGenerationStatus")
