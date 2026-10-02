from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

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


if __name__ == "__main__":
    unittest.main()
