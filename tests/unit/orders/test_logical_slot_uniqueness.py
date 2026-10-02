from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from tests.unit.storage.support import new_ledger, order_record, seed_grid


class LogicalSlotUniquenessContractTests(unittest.TestCase):
    def test_database_tuple_constraint_cannot_be_bypassed_by_alternate_text(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            ledger = new_ledger(Path(directory) / "ledger.sqlite3")
            with ledger.unit_of_work() as uow:
                seed_grid(uow)
                uow.orders.add(order_record())
                bypass = order_record("order-2", attempt_no=2)
                bypass["logical_slot_key"] = "alternate-serialization"
                bypass["client_order_id"] = "different-safe-client-id"
                columns = list(bypass)
                with self.assertRaises(sqlite3.IntegrityError):
                    uow.connection.execute(
                        "INSERT INTO orders ({}) VALUES ({})".format(
                            ", ".join(columns),
                            ", ".join("?" for _ in columns),
                        ),
                        tuple(bypass[column] for column in columns),
                    )


if __name__ == "__main__":
    unittest.main()
