"""COIN-M exchangeInfo selection and deterministic rules hashing."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from typing import Any

from .errors import ExchangeInvalidResponseError, ExchangeNotFoundError


def field(payload: Mapping[str, Any], *names: str, default: Any = None) -> Any:
    for name in names:
        if name in payload:
            return payload[name]
    return default


def as_mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    for method_name in ("to_dict", "model_dump"):
        method = getattr(value, method_name, None)
        if callable(method):
            converted = method()
            if isinstance(converted, Mapping):
                return dict(converted)
    if hasattr(value, "__dict__"):
        return {
            key: item
            for key, item in vars(value).items()
            if not key.startswith("_")
        }
    raise ExchangeInvalidResponseError(
        f"expected mapping-like SDK value, got {type(value).__name__}"
    )


def as_items(value: Any, *container_names: str) -> list[Any]:
    if isinstance(value, (list, tuple)):
        return list(value)
    actual = getattr(value, "actual_instance", None)
    if isinstance(actual, (list, tuple)):
        return list(actual)
    payload = as_mapping(value)
    for name in container_names:
        candidate = field(payload, name, _snake_to_camel(name))
        if isinstance(candidate, (list, tuple)):
            return list(candidate)
    # Generated root models can serialize under root/__root__.
    for name in ("root", "__root__", "items"):
        candidate = payload.get(name)
        if isinstance(candidate, (list, tuple)):
            return list(candidate)
    raise ExchangeInvalidResponseError("expected a list-like SDK response")


def _snake_to_camel(value: str) -> str:
    first, *rest = value.split("_")
    return first + "".join(item.capitalize() for item in rest)


def select_perpetual_symbol(exchange_info: Any, symbol: str) -> dict[str, Any]:
    info = as_mapping(exchange_info)
    symbols = field(info, "symbols")
    if not isinstance(symbols, Sequence) or isinstance(symbols, (str, bytes)):
        raise ExchangeInvalidResponseError("exchangeInfo.symbols is not a list")
    selected = None
    for item in symbols:
        mapped = as_mapping(item)
        if field(mapped, "symbol") == symbol:
            selected = mapped
            break
    if selected is None:
        raise ExchangeNotFoundError(f"COIN-M symbol {symbol!r} was not found")
    status = str(
        field(
            selected,
            "contractStatus",
            "contract_status",
            "status",
            default="",
        )
    )
    contract_type = str(
        field(selected, "contractType", "contract_type", default="")
    )
    if status != "TRADING" or contract_type != "PERPETUAL":
        raise ExchangeNotFoundError(
            f"symbol {symbol!r} is not a TRADING COIN-M PERPETUAL"
        )
    return selected


def filter_by_type(symbol_payload: Mapping[str, Any], filter_type: str) -> dict[str, Any]:
    filters = field(symbol_payload, "filters")
    if not isinstance(filters, Sequence) or isinstance(filters, (str, bytes)):
        raise ExchangeInvalidResponseError("exchangeInfo symbol filters are missing")
    for item in filters:
        mapped = as_mapping(item)
        if field(mapped, "filterType", "filter_type") == filter_type:
            return mapped
    raise ExchangeInvalidResponseError(f"required {filter_type} filter is missing")


def rules_hash(symbol_payload: Mapping[str, Any]) -> str:
    canonical = json.dumps(
        dict(symbol_payload),
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


__all__ = [
    "as_items",
    "as_mapping",
    "field",
    "filter_by_type",
    "rules_hash",
    "select_perpetual_symbol",
]
