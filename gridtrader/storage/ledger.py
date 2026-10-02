"""Public facade and validation helpers for the SQLite strategy ledger."""

from __future__ import annotations

import json
import re
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Optional, Union

from .ports import SensitiveDataError
from .sqlite.connection import SQLiteDatabase
from .sqlite.unit_of_work import SQLiteUnitOfWork


_SENSITIVE_KEY = re.compile(
    r"(?:api[_-]?secret|api[_-]?key|secret[_-]?key|listen[_-]?key|listenkey|signature)",
    re.IGNORECASE,
)
_SIGNED_URL = re.compile(r"(?:[?&]signature=|wss?://[^\s]*listenkey)", re.IGNORECASE)


def decimal_text(value: Union[str, int, Decimal]) -> str:
    """Return a finite, canonical decimal string without accepting floats.

    SQLite REAL is intentionally not used for exchange prices, PnL or amounts.
    Rejecting float input prevents a binary approximation from silently entering
    the durable ledger before it is converted to text.
    """

    if isinstance(value, bool) or isinstance(value, float):
        raise TypeError("financial values must be Decimal, int, or decimal text")
    try:
        number = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError("invalid decimal value: {!r}".format(value)) from exc
    if not number.is_finite():
        raise ValueError("financial values must be finite")
    normalized = format(number, "f")
    if "." in normalized:
        normalized = normalized.rstrip("0").rstrip(".")
    if normalized in {"", "-0"}:
        normalized = "0"
    return normalized


def ensure_safe_text(value: Optional[str], field_name: str = "text") -> Optional[str]:
    """Reject credentials, listen keys and signed URLs at the storage boundary."""

    if value is None:
        return None
    text = str(value)
    if _SIGNED_URL.search(text):
        raise SensitiveDataError("{} contains a signed or private URL".format(field_name))
    return text


def safe_json(payload: Any) -> str:
    """Serialize an event payload after recursively rejecting secret-bearing keys."""

    def validate(value: Any, path: str) -> None:
        if isinstance(value, dict):
            for key, child in value.items():
                key_text = str(key)
                if _SENSITIVE_KEY.search(key_text):
                    raise SensitiveDataError("sensitive event key at {}.{}".format(path, key_text))
                validate(child, "{}.{}".format(path, key_text))
        elif isinstance(value, (list, tuple)):
            for index, child in enumerate(value):
                validate(child, "{}[{}]".format(path, index))
        elif isinstance(value, str):
            ensure_safe_text(value, path)

    validate(payload, "payload")
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


class SQLiteLedger:
    """Small facade that owns database configuration and transaction creation."""

    def __init__(
        self,
        path: Union[str, Path],
        *,
        busy_timeout_ms: int = 5_000,
    ) -> None:
        self.database = SQLiteDatabase(path, busy_timeout_ms=busy_timeout_ms)

    def initialize(self) -> None:
        self.database.migrate()

    def unit_of_work(self, immediate: bool = True) -> SQLiteUnitOfWork:
        return SQLiteUnitOfWork(self.database, immediate=immediate)

    transaction = unit_of_work


# Concise public name used by application services.
Ledger = SQLiteLedger


__all__ = [
    "Ledger",
    "SQLiteLedger",
    "decimal_text",
    "ensure_safe_text",
    "safe_json",
]
