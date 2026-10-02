"""Operation-aware bounded retry for read-only exchange observations."""

from __future__ import annotations

import random
import time
from dataclasses import dataclass
from threading import Lock
from typing import Callable, TypeVar

from .errors import (
    ExchangeAmbiguousResultError,
    ExchangeBannedError,
    ExchangeError,
    ExchangeTimeoutError,
    RequestClass,
    classify_sdk_exception,
)


T = TypeVar("T")


@dataclass(frozen=True)
class RetrySettings:
    max_attempts: int = 4
    base_delay_seconds: float = 0.1
    max_delay_seconds: float = 2.0
    total_deadline_seconds: float = 8.0

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be positive")
        if min(
            self.base_delay_seconds,
            self.max_delay_seconds,
            self.total_deadline_seconds,
        ) < 0:
            raise ValueError("retry delays and deadline must be non-negative")


class RetryPolicy:
    """Retry only classified transient reads; writes are always single-shot."""

    def __init__(
        self,
        settings: RetrySettings = RetrySettings(),
        *,
        sleep: Callable[[float], None] = time.sleep,
        monotonic: Callable[[], float] = time.monotonic,
        random_value: Callable[[], float] = random.random,
    ) -> None:
        self.settings = settings
        self._sleep = sleep
        self._monotonic = monotonic
        self._random = random_value
        self._circuit_open = False
        # A connector deliberately serializes Phase-2 account snapshots.  In
        # addition to producing coherent recovery evidence, this prevents a
        # burst of concurrent retries after a 429 response.
        self._operation_lock = Lock()

    @property
    def circuit_open(self) -> bool:
        return self._circuit_open

    def execute(
        self,
        operation: Callable[[], T],
        *,
        request_class: RequestClass = RequestClass.READ_ONLY,
    ) -> T:
        with self._operation_lock:
            return self._execute_locked(operation, request_class=request_class)

    def _execute_locked(
        self,
        operation: Callable[[], T],
        *,
        request_class: RequestClass,
    ) -> T:
        if self._circuit_open:
            raise ExchangeBannedError(
                "exchange request circuit is open after HTTP 418",
                http_status=418,
                request_class=request_class,
            )

        started = self._monotonic()
        attempt = 0
        while True:
            attempt += 1
            try:
                result = operation()
            except ExchangeError as exc:
                mapped = exc
            except Exception as exc:  # SDK types are isolated at this boundary.
                mapped = classify_sdk_exception(exc, request_class=request_class)
            else:
                if self._monotonic() - started > self.settings.total_deadline_seconds:
                    if request_class is RequestClass.WRITE:
                        raise ExchangeAmbiguousResultError(
                            "exchange write completed after retry deadline",
                            request_class=request_class,
                        )
                    raise ExchangeTimeoutError(
                        "exchange read completed after retry deadline",
                        request_class=request_class,
                    )
                return result

            if isinstance(mapped, ExchangeBannedError):
                self._circuit_open = True
            if (
                request_class is not RequestClass.READ_ONLY
                or not mapped.retryable
                or attempt >= self.settings.max_attempts
            ):
                raise mapped from None

            ceiling = min(
                self.settings.max_delay_seconds,
                self.settings.base_delay_seconds * (2 ** (attempt - 1)),
            )
            delay = ceiling * max(0.0, min(1.0, self._random()))
            if mapped.retry_after_seconds is not None:
                delay = max(delay, mapped.retry_after_seconds)
            elapsed = self._monotonic() - started
            if (
                elapsed >= self.settings.total_deadline_seconds
                or elapsed + delay > self.settings.total_deadline_seconds
            ):
                raise mapped from None
            self._sleep(delay)


__all__ = ["RetryPolicy", "RetrySettings"]
