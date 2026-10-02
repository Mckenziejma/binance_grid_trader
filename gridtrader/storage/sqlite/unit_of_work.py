"""Explicit SQLite transaction boundary for ledger operations."""

from __future__ import annotations

import sqlite3
from types import TracebackType
from typing import Optional, Type

from .connection import SQLiteDatabase
from .repositories import SQLiteRepositories


class SQLiteUnitOfWork:
    """One connection and one explicit transaction.

    Transactions never auto-commit.  Calling :meth:`commit` is mandatory;
    otherwise context exit rolls back, including normal context exit.  This
    makes partial recovery checkpoints and order intents fail closed.
    """

    def __init__(self, database: SQLiteDatabase, *, immediate: bool = True) -> None:
        self.database = database
        self.immediate = immediate
        self.connection: Optional[sqlite3.Connection] = None
        self.repositories: Optional[SQLiteRepositories] = None
        self._finished = False

    def __enter__(self) -> "SQLiteUnitOfWork":
        if self.connection is not None or self._finished:
            raise RuntimeError("unit of work instances are single-use")
        self.connection = self.database.connect()
        try:
            self.connection.execute("BEGIN IMMEDIATE" if self.immediate else "BEGIN")
            self.repositories = SQLiteRepositories(self.connection)
            self._bind_repositories(self.repositories)
        except BaseException:
            self.connection.close()
            self.connection = None
            self.repositories = None
            self._finished = True
            raise
        return self

    def _bind_repositories(self, repositories: SQLiteRepositories) -> None:
        self.strategies = repositories.strategies
        self.grid_generations = repositories.grid_generations
        self.grid_levels = repositories.grid_levels
        self.orders = repositories.orders
        self.fills = repositories.fills
        self.positions = repositories.positions
        self.recovery_checkpoints = repositories.recovery_checkpoints
        self.events = repositories.events
        self.bot_runs = repositories.bot_runs
        self.strategy_leases = repositories.strategy_leases
        self.instrument_rules = repositories.instrument_rules
        self.position_mode_observations = repositories.position_mode_observations
        self.exchange_observations = repositories.exchange_observations
        self.exchange_trade_observations = repositories.exchange_trade_observations

    def commit(self) -> None:
        connection = self._require_connection()
        if self._finished:
            raise RuntimeError("transaction has already finished")
        connection.commit()
        self._finished = True
        connection.close()
        self.connection = None
        self.repositories = None

    def rollback(self) -> None:
        connection = self._require_connection()
        if not self._finished:
            connection.rollback()
            self._finished = True
            connection.close()
            self.connection = None
            self.repositories = None

    def _require_connection(self) -> sqlite3.Connection:
        if self.connection is None:
            raise RuntimeError("unit of work is not active")
        return self.connection

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[TracebackType],
    ) -> None:
        if self.connection is None:
            return
        try:
            if not self._finished:
                self.connection.rollback()
                self._finished = True
        finally:
            self.connection.close()
            self.connection = None
            self.repositories = None


__all__ = ["SQLiteUnitOfWork"]
