"""Storage interfaces and storage-specific errors.

The protocols intentionally use mappings instead of legacy data classes.  This
keeps the persistence boundary independent from the old event engine and from
the future Binance adapter.
"""

from __future__ import annotations

from typing import Any, Mapping, Optional, Protocol, runtime_checkable


Record = Mapping[str, Any]


class StorageError(RuntimeError):
    """Base class for durable-ledger failures."""


class MigrationError(StorageError):
    """The database cannot be migrated safely."""


class ConstraintViolation(StorageError):
    """A durable uniqueness or foreign-key invariant was violated."""


class InvariantViolation(StorageError):
    """An update would regress or contradict an established exchange fact."""


class NotFound(StorageError):
    """A requested ledger record does not exist."""


class SensitiveDataError(StorageError):
    """Content that must never be persisted was presented to the ledger."""


class LeaseUnavailable(StorageError):
    """Another live process owns the requested strategy lease."""


@runtime_checkable
class Repository(Protocol):
    """Minimal repository contract shared by concrete repositories."""

    def get(self, record_id: str) -> Optional[Record]:
        ...


@runtime_checkable
class UnitOfWork(Protocol):
    """Transaction boundary used by services and recovery code."""

    strategies: Any
    grid_generations: Any
    grid_levels: Any
    orders: Any
    fills: Any
    positions: Any
    recovery_checkpoints: Any
    events: Any
    bot_runs: Any
    strategy_leases: Any
    instrument_rules: Any
    position_mode_observations: Any
    exchange_observations: Any
    exchange_trade_observations: Any

    def __enter__(self) -> "UnitOfWork":
        ...

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        ...

    def commit(self) -> None:
        ...

    def rollback(self) -> None:
        ...


@runtime_checkable
class LedgerPort(Protocol):
    """Top-level durable ledger contract."""

    def initialize(self) -> None:
        ...

    def unit_of_work(self, immediate: bool = True) -> UnitOfWork:
        ...
