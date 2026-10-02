from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from .support import new_ledger, strategy_record


class TransactionRollbackContractTests(unittest.TestCase):
    def test_exception_rolls_back_the_entire_unit_of_work(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            ledger = new_ledger(Path(directory) / "ledger.sqlite3")
            with self.assertRaises(RuntimeError):
                with ledger.unit_of_work() as uow:
                    uow.strategies.add(strategy_record())
                    raise RuntimeError("force rollback")

            with ledger.unit_of_work(immediate=False) as uow:
                self.assertIsNone(uow.strategies.get("strategy-1"))


if __name__ == "__main__":
    unittest.main()
