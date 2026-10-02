"""Clock abstractions used by domain and application services."""

from abc import ABC, abstractmethod
from datetime import datetime, timezone

from .errors import DomainValidationError


def require_utc(value: datetime, field_name: str = "timestamp") -> datetime:
    """Require a timezone-aware timestamp and normalize it to UTC."""

    if not isinstance(value, datetime):
        raise DomainValidationError(f"{field_name} must be datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise DomainValidationError(f"{field_name} must be timezone-aware")
    return value.astimezone(timezone.utc)


class Clock(ABC):
    """Port for obtaining current UTC time."""

    @abstractmethod
    def now(self) -> datetime:
        """Return a timezone-aware UTC timestamp."""

        raise NotImplementedError


class SystemClock(Clock):
    """Production wall clock."""

    def now(self) -> datetime:
        return datetime.now(timezone.utc)


__all__ = ["Clock", "SystemClock", "require_utc"]
