"""Provider-neutral exchange error taxonomy.

Only redacted, structured details cross into application/recovery code. Raw
SDK exceptions may be chained as ``__cause__`` for local diagnostics, but are
never interpolated into persisted messages.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from enum import Enum
from typing import Any, Optional

from gridtrader.core.errors import (
    ExchangePortError,
    ExchangeUnavailableError as CoreExchangeUnavailableError,
)


class RequestClass(str, Enum):
    READ_ONLY = "read_only"
    WRITE = "write"


_SECRET_PATTERNS = (
    re.compile(r"(?i)(signature[\"']?\s*[=:]\s*[\"']?)[^&\s,}\"']+"),
    re.compile(r"(?i)(listen[_-]?key[\"']?\s*[=:]\s*[\"']?)[^&\s,}\"']+"),
    re.compile(r"(?i)(api[_-]?(?:key|secret)[\"']?\s*[=:]\s*[\"']?)[^&\s,}\"']+"),
    re.compile(r"(?i)(x-mbx-apikey[\"']?\s*[=:]\s*[\"']?)[^&\s,}\"']+"),
)


def redact_message(value: object) -> str:
    text = str(value)
    for pattern in _SECRET_PATTERNS:
        text = pattern.sub(r"\1<redacted>", text)
    return text


class ExchangeError(ExchangePortError):
    """Base structured exchange failure."""

    default_retryable = False

    def __init__(
        self,
        message: str,
        *,
        code: Optional[int | str] = None,
        http_status: Optional[int] = None,
        request_class: RequestClass = RequestClass.READ_ONLY,
        retryable: Optional[bool] = None,
        retry_after_seconds: Optional[float] = None,
    ) -> None:
        self.message = redact_message(message)
        self.code = code
        self.http_status = http_status
        self.request_class = request_class
        self.retryable = self.default_retryable if retryable is None else retryable
        self.retry_after_seconds = retry_after_seconds
        super().__init__(self.message)


class ExchangeAuthError(ExchangeError):
    pass


class ExchangePermissionError(ExchangeError):
    pass


class ExchangeRateLimitError(ExchangeError):
    default_retryable = True


class ExchangeBannedError(ExchangeError):
    pass


class ExchangeTimeoutError(ExchangeError):
    default_retryable = True


class ExchangeUnavailableError(ExchangeError, CoreExchangeUnavailableError):
    default_retryable = True


class ExchangePermanentRequestError(ExchangeError):
    pass


class ExchangeInsufficientMarginError(ExchangeError):
    pass


class ExchangeAmbiguousResultError(ExchangeError):
    pass


class ExchangeRulesChangedError(ExchangeError):
    pass


class ExchangeNotFoundError(ExchangeError):
    pass


class ExchangeInvalidResponseError(ExchangeError):
    pass


def _first_attr(error: BaseException, *names: str) -> Any:
    for name in names:
        value = getattr(error, name, None)
        if value is not None:
            return value
    return None


def _coerce_status(value: Any) -> Optional[int]:
    try:
        return None if value is None else int(value)
    except (TypeError, ValueError):
        return None


def _coerce_code(value: Any) -> Optional[int | str]:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return redact_message(value)


def classify_sdk_exception(
    error: BaseException,
    *,
    request_class: RequestClass = RequestClass.READ_ONLY,
) -> ExchangeError:
    """Classify an SDK/transport exception by stable public attributes."""

    name = type(error).__name__
    raw_status = _coerce_status(
        _first_attr(error, "status_code", "status", "http_status")
    )
    code = _coerce_code(_first_attr(error, "error_code", "code"))

    # ``binance-common`` uses ``status_code`` for Binance's negative JSON
    # error code on several 4xx exception types.  Recover the HTTP category
    # from the public exception class name while preserving the provider code.
    official_http_status = {
        "BadRequestError": 400,
        "UnauthorizedError": 401,
        "ForbiddenError": 403,
        "NotFoundError": 404,
        "ConflictError": 409,
        "RateLimitBanError": 418,
        "TooManyRequestsError": 429,
    }.get(name)
    if raw_status is not None and raw_status < 0:
        if code is None:
            code = raw_status
        status = official_http_status
    else:
        status = raw_status if raw_status is not None else official_http_status
    raw_message = _first_attr(error, "error_message", "message", "detail")
    message = redact_message(raw_message if raw_message is not None else error)
    lowered = message.lower()
    kwargs: dict[str, Any] = {
        "code": code,
        "http_status": status,
        "request_class": request_class,
    }

    if code == -1003 and status not in {418, 429} and name not in {
        "RateLimitBanError",
        "TooManyRequestsError",
    }:
        return ExchangeRateLimitError(
            message or "exchange rate limit exceeded", **kwargs
        )
    if code == -1006:
        return ExchangeAmbiguousResultError(
            message or "exchange response was unexpected", **kwargs
        )
    if code == -1007 or isinstance(error, TimeoutError) or "timeout" in name.lower():
        if request_class is RequestClass.WRITE:
            return ExchangeAmbiguousResultError(
                message or "exchange write result is unknown after timeout", **kwargs
            )
        return ExchangeTimeoutError(message or "exchange request timed out", **kwargs)
    # The current official modular SDK wraps ``requests.Timeout`` as the
    # generic ``NetworkError`` class and preserves the distinction only in the
    # public error message.  Recover that transport category before the generic
    # network branch below.
    if (
        (name == "NetworkError" or isinstance(error, ConnectionError))
        and ("timeout" in lowered or "timed out" in lowered)
    ):
        if request_class is RequestClass.WRITE:
            return ExchangeAmbiguousResultError(
                message or "exchange write result is unknown after timeout", **kwargs
            )
        return ExchangeTimeoutError(message or "exchange request timed out", **kwargs)
    if code in {-1000, -1001, -1008}:
        if request_class is RequestClass.WRITE:
            return ExchangeAmbiguousResultError(
                message or "exchange write result is unknown", **kwargs
            )
        return ExchangeUnavailableError(
            message or "exchange service unavailable", **kwargs
        )
    if status == 418 or name == "RateLimitBanError":
        return ExchangeBannedError(message or "exchange rate-limit ban", **kwargs)
    if status == 429 or name == "TooManyRequestsError":
        retry_after = _first_attr(error, "retry_after", "retry_after_seconds")
        if retry_after is None:
            headers = _first_attr(error, "headers", "response_headers")
            if isinstance(headers, Mapping):
                retry_after = next(
                    (
                        value
                        for key, value in headers.items()
                        if str(key).lower() == "retry-after"
                    ),
                    None,
                )
        try:
            retry_after_value = None if retry_after is None else float(retry_after)
        except (TypeError, ValueError):
            retry_after_value = None
        return ExchangeRateLimitError(
            message or "exchange rate limit exceeded",
            retry_after_seconds=retry_after_value,
            **kwargs,
        )
    if status == 401 or name == "UnauthorizedError" or code in {-2014, -2015}:
        return ExchangeAuthError(message or "exchange authentication failed", **kwargs)
    if status == 403 or name == "ForbiddenError":
        return ExchangePermissionError(message or "exchange permission denied", **kwargs)
    if status == 503 and (
        "execution status unknown" in lowered
        or "unknown error" in lowered
        or "please check your request" in lowered
    ):
        return ExchangeAmbiguousResultError(
            message or "exchange execution status unknown", **kwargs
        )
    if code == -2019 or "insufficient margin" in lowered:
        return ExchangeInsufficientMarginError(message or "insufficient margin", **kwargs)
    if code == -2013 or name == "NotFoundError" or status == 404:
        return ExchangeNotFoundError(message or "exchange object was not found", **kwargs)
    if code in {-1013, -1111, -1116, -1117}:
        return ExchangeRulesChangedError(message or "exchange rules rejected request", **kwargs)
    if status is not None and status >= 500:
        return ExchangeUnavailableError(message or "exchange service unavailable", **kwargs)
    if name == "NetworkError" or isinstance(error, ConnectionError):
        if request_class is RequestClass.WRITE:
            return ExchangeAmbiguousResultError(
                message or "exchange write result is unknown", **kwargs
            )
        return ExchangeUnavailableError(message or "exchange network unavailable", **kwargs)
    if status is not None and 400 <= status < 500:
        return ExchangePermanentRequestError(
            message or "permanent exchange request error", **kwargs
        )
    if name in {"BadRequestError", "RequiredError", "ClientError"}:
        return ExchangePermanentRequestError(message or "invalid exchange request", **kwargs)
    return ExchangeInvalidResponseError(message or "unclassified exchange failure", **kwargs)


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
