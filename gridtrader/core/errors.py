"""Domain and port errors for the new architecture."""

from typing import Optional


class GridTraderError(Exception):
    """Base class for errors deliberately exposed by the new core."""


class DomainValidationError(GridTraderError, ValueError):
    """A domain value violates a construction-time invariant."""


class InvariantViolation(GridTraderError):
    """Persisted or observed state violates a cross-record invariant."""


class InvalidStateTransition(GridTraderError):
    """A state transition is not legal from the current state."""


class NotReadyError(GridTraderError):
    """An action requiring READY was attempted in another readiness state."""


class ExchangePortError(GridTraderError):
    """Base error raised by an exchange adapter at the port boundary."""


class ExchangeUnavailableError(ExchangePortError):
    """The exchange could not be reached before an operation was accepted."""


class AmbiguousOrderResultError(ExchangePortError):
    """An order command may have reached the exchange but no ACK was received.

    The application must persist ``ACK_UNKNOWN`` and reconcile by the exact
    client order ID.  It must not create a replacement client order ID.
    """

    def __init__(
        self,
        client_order_id: str,
        message: str = "order acknowledgement is unknown",
        *,
        cause: Optional[BaseException] = None,
    ) -> None:
        super().__init__(message)
        self.client_order_id = client_order_id
        self.cause = cause


__all__ = [
    "AmbiguousOrderResultError",
    "DomainValidationError",
    "ExchangePortError",
    "ExchangeUnavailableError",
    "GridTraderError",
    "InvalidStateTransition",
    "InvariantViolation",
    "NotReadyError",
]
