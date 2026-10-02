from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from tests.unit.storage.support import fill_record, new_ledger, order_record, seed_grid


class FillDeduplicationContractTests(unittest.TestCase):
    def test_same_account_symbol_trade_id_is_committed_once(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            ledger = new_ledger(Path(directory) / "ledger.sqlite3")
            with ledger.unit_of_work() as uow:
                seed_grid(uow)
                uow.orders.add(order_record())
                websocket_fill = fill_record()
                websocket_fill["commission"] = None
                websocket_fill["commission_asset"] = None
                _, inserted_first = uow.fills.add_idempotent(websocket_fill)
                replayed, inserted_again = uow.fills.add_idempotent(
                    fill_record("fill-replay", "trade-100")
                )
                self.assertTrue(inserted_first)
                self.assertFalse(inserted_again)
                self.assertEqual("0.000001", replayed["commission"])
                self.assertEqual("BTC", replayed["commission_asset"])
                self.assertEqual(1, len(uow.fills.list_all()))


if __name__ == "__main__":
    unittest.main()
