"""Compatibility exports for the provider-neutral exchange error taxonomy."""

from gridtrader.exchange.errors import (
    ExchangeAmbiguousResultError,
    ExchangeAuthError,
    ExchangeBannedError,
    ExchangeError,
    ExchangeInsufficientMarginError,
    ExchangeInvalidResponseError,
    ExchangeNotFoundError,
    ExchangePermanentRequestError,
    ExchangePermissionError,
    ExchangeRateLimitError,
    ExchangeRulesChangedError,
    ExchangeTimeoutError,
    ExchangeUnavailableError,
    RequestClass,
    classify_sdk_exception,
    redact_message,
)

__all__ = [
    "ExchangeAmbiguousResultError",
    "ExchangeAuthError",
    "ExchangeBannedError",
    "ExchangeError",
    "ExchangeInsufficientMarginError",
    "ExchangeInvalidResponseError",
    "ExchangeNotFoundError",
    "ExchangePermanentRequestError",
    "ExchangePermissionError",
    "ExchangeRateLimitError",
    "ExchangeRulesChangedError",
    "ExchangeTimeoutError",
    "ExchangeUnavailableError",
    "RequestClass",
    "classify_sdk_exception",
    "redact_message",
]
