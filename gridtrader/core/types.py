"""Small, exchange-independent value types for the new trading core."""

from dataclasses import dataclass
from decimal import Decimal
from typing import NewType

from .errors import DomainValidationError


StrategyId = NewType("StrategyId", str)
GridGenerationId = NewType("GridGenerationId", str)
LogicalLevelId = NewType("LogicalLevelId", str)
ClientOrderId = NewType("ClientOrderId", str)
ExchangeOrderId = NewType("ExchangeOrderId", str)
TradeId = NewType("TradeId", str)
Symbol = NewType("Symbol", str)

# Monetary and price values must remain Decimal at every domain boundary.
Price = Decimal
Amount = Decimal

# COIN-M order quantity is an integer number of contracts.
Contracts = int


def require_non_empty(value: str, field_name: str) -> str:
    """Return a stripped identifier or raise a domain validation error."""

    if not isinstance(value, str):
        raise DomainValidationError(f"{field_name} must be a string")
    normalized = value.strip()
    if not normalized:
        raise DomainValidationError(f"{field_name} must not be empty")
    return normalized


def require_finite_decimal(
    value: Decimal,
    field_name: str,
    *,
    positive: bool = False,
    non_negative: bool = False,
) -> Decimal:
    """Validate a Decimal without silently accepting binary floats."""

    if not isinstance(value, Decimal):
        raise DomainValidationError(f"{field_name} must be Decimal")
    if not value.is_finite():
        raise DomainValidationError(f"{field_name} must be finite")
    if positive and value <= 0:
        raise DomainValidationError(f"{field_name} must be greater than zero")
    if non_negative and value < 0:
        raise DomainValidationError(f"{field_name} must not be negative")
    return value


def require_contracts(
    value: int,
    field_name: str,
    *,
    positive: bool = False,
    non_negative: bool = False,
) -> int:
    """Validate integer contract counts and reject bool-as-int values."""

    if isinstance(value, bool) or not isinstance(value, int):
        raise DomainValidationError(f"{field_name} must be an integer contract count")
    if positive and value <= 0:
        raise DomainValidationError(f"{field_name} must be greater than zero")
    if non_negative and value < 0:
        raise DomainValidationError(f"{field_name} must not be negative")
    return value


@dataclass(frozen=True)
class GridBounds:
    """Validated price boundaries for a grid generation."""

    lower_price: Price
    upper_price: Price

    def __post_init__(self) -> None:
        require_finite_decimal(self.lower_price, "lower_price", positive=True)
        require_finite_decimal(self.upper_price, "upper_price", positive=True)
        if self.lower_price >= self.upper_price:
            raise DomainValidationError("lower_price must be less than upper_price")


__all__ = [
    "Amount",
    "ClientOrderId",
    "Contracts",
    "ExchangeOrderId",
    "GridBounds",
    "GridGenerationId",
    "LogicalLevelId",
    "Price",
    "StrategyId",
    "Symbol",
    "TradeId",
    "require_contracts",
    "require_finite_decimal",
    "require_non_empty",
]
