"""Logical grid-level model; price generation belongs to a later phase."""

from dataclasses import dataclass
from decimal import Decimal

from gridtrader.core.enums import GridLevelState


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


@dataclass(frozen=True)
class GridLevel:
    """One persisted logical level within a grid generation."""

    level_id: str
    generation_id: str
    level_index: int
    price: Decimal
    planned_contracts: int
    cycle_no: int
    state: GridLevelState

    def __post_init__(self) -> None:
        _validate_text(self.level_id, "level_id")
        _validate_text(self.generation_id, "generation_id")
        _validate_non_negative_int(self.level_index, "level_index")
        if not isinstance(self.price, Decimal):
            raise TypeError("price must be Decimal")
        if not self.price.is_finite() or self.price <= 0:
            raise ValueError("price must be finite and greater than zero")
        _validate_non_negative_int(self.planned_contracts, "planned_contracts")
        if self.planned_contracts == 0:
            raise ValueError("planned_contracts must be greater than zero")
        _validate_non_negative_int(self.cycle_no, "cycle_no")
        if not isinstance(self.state, GridLevelState):
            raise TypeError("state must be GridLevelState")
