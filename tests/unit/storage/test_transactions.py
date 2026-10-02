from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from gridtrader.storage.sqlite.unit_of_work import SQLiteUnitOfWork

from .support import new_ledger, strategy_record


class TransactionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "ledger.sqlite3"
        self.ledger = new_ledger(self.db_path)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_normal_exit_without_commit_rolls_back(self) -> None:
        with self.ledger.unit_of_work() as uow:
            uow.strategies.add(strategy_record())
        with self.ledger.unit_of_work(immediate=False) as uow:
            self.assertIsNone(uow.strategies.get("strategy-1"))

    def test_exception_rolls_back_all_records(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "boom"):
            with self.ledger.unit_of_work() as uow:
                uow.strategies.add(strategy_record())
                raise RuntimeError("boom")
        with self.ledger.unit_of_work(immediate=False) as uow:
            self.assertIsNone(uow.strategies.get("strategy-1"))

    def test_explicit_commit_persists(self) -> None:
        unit_of_work = self.ledger.unit_of_work()
        with unit_of_work as uow:
            uow.strategies.add(strategy_record())
            uow.commit()
            with self.assertRaises(sqlite3.ProgrammingError):
                uow.strategies.add(strategy_record("strategy-after-commit"))
        with self.assertRaises(RuntimeError):
            unit_of_work.__enter__()
        with self.ledger.unit_of_work(immediate=False) as uow:
            stored = uow.strategies.require("strategy-1")
            self.assertEqual("BTCUSD_PERP", stored["symbol"])
            self.assertIsNone(uow.strategies.get("strategy-after-commit"))

    def test_begin_failure_closes_connection_and_invalidates_unit_of_work(self) -> None:
        class BeginFailingConnection:
            def __init__(self) -> None:
                self.closed = False

            def execute(self, statement: str) -> None:
                self.assert_not_closed()
                raise sqlite3.OperationalError("database is locked")

            def close(self) -> None:
                self.closed = True

            def assert_not_closed(self) -> None:
                if self.closed:
                    raise AssertionError("connection was already closed")

        class StubDatabase:
            def __init__(self, connection: BeginFailingConnection) -> None:
                self.connection = connection

            def connect(self) -> BeginFailingConnection:
                return self.connection

        connection = BeginFailingConnection()
        unit_of_work = SQLiteUnitOfWork(StubDatabase(connection))  # type: ignore[arg-type]

        with self.assertRaisesRegex(sqlite3.OperationalError, "database is locked"):
            unit_of_work.__enter__()

        self.assertTrue(connection.closed)
        self.assertIsNone(unit_of_work.connection)
        self.assertIsNone(unit_of_work.repositories)
        with self.assertRaisesRegex(RuntimeError, "single-use"):
            unit_of_work.__enter__()


if __name__ == "__main__":
    unittest.main()
