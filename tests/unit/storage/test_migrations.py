from __future__ import annotations

import sqlite3
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path

from gridtrader.storage import InvariantViolation, SensitiveDataError
from gridtrader.storage.ledger import decimal_text

from .support import NOW, new_ledger


class MigrationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "ledger.sqlite3"

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_empty_database_gets_all_phase2_tables_and_pragmas(self) -> None:
        ledger = new_ledger(self.db_path)
        expected = {
            "strategies", "grid_generations", "grid_levels", "orders", "fills",
            "positions", "recovery_checkpoints", "events", "schema_migrations",
            "bot_runs", "strategy_leases",
            "instrument_rules", "position_mode_observations",
            "exchange_observations", "exchange_trade_observations",
        }
        connection = ledger.database.connect()
        try:
            actual = {
                row["name"]
                for row in connection.execute(
                    "SELECT name FROM sqlite_master "
                    "WHERE type = 'table' AND name NOT LIKE 'sqlite_%'"
                )
            }
            self.assertEqual(expected, actual)
            self.assertEqual(1, connection.execute("PRAGMA foreign_keys").fetchone()[0])
            self.assertEqual("wal", connection.execute("PRAGMA journal_mode").fetchone()[0].lower())
            self.assertEqual(1_234, connection.execute("PRAGMA busy_timeout").fetchone()[0])
            versions = [
                row[0]
                for row in connection.execute(
                    "SELECT version FROM schema_migrations ORDER BY version"
                )
            ]
            self.assertEqual([1, 2, 3], versions)
        finally:
            connection.close()

    def test_migrations_are_idempotent(self) -> None:
        ledger = new_ledger(self.db_path)
        ledger.initialize()
        connection = ledger.database.connect()
        try:
            count = connection.execute("SELECT COUNT(*) FROM schema_migrations").fetchone()[0]
            self.assertEqual(3, count)
        finally:
            connection.close()

    def test_financial_columns_are_text_and_contracts_are_integer(self) -> None:
        ledger = new_ledger(self.db_path)
        connection = ledger.database.connect()
        try:
            order_types = {
                row["name"]: row["type"]
                for row in connection.execute("PRAGMA table_info(orders)")
            }
            self.assertEqual("TEXT", order_types["price"])
            self.assertEqual("TEXT", order_types["avg_fill_price"])
            self.assertEqual("INTEGER", order_types["quantity_contracts"])
            fill_types = {
                row["name"]: row["type"]
                for row in connection.execute("PRAGMA table_info(fills)")
            }
            self.assertEqual("TEXT", fill_types["commission"])
            self.assertEqual("TEXT", fill_types["realized_pnl"])
        finally:
            connection.close()

    def test_decimal_serialization_rejects_float(self) -> None:
        self.assertEqual("123.45", decimal_text(Decimal("123.4500")))
        with self.assertRaises(TypeError):
            decimal_text(123.45)

    def test_event_repository_rejects_sensitive_material(self) -> None:
        ledger = new_ledger(self.db_path)
        with ledger.unit_of_work() as uow:
            with self.assertRaises(SensitiveDataError):
                uow.events.append({
                    "source": "LOCAL",
                    "dedupe_key": "sensitive-1",
                    "event_type": "bad_payload",
                    "received_at_ms": NOW,
                    "payload_json": {"listenKey": "must-not-persist"},
                })

    def test_event_repository_does_not_trust_a_caller_supplied_hash(self) -> None:
        ledger = new_ledger(self.db_path)
        with ledger.unit_of_work() as uow:
            with self.assertRaises(InvariantViolation):
                uow.events.append({
                    "source": "LOCAL",
                    "dedupe_key": "event-1",
                    "event_type": "safe_payload",
                    "received_at_ms": NOW,
                    "payload_json": {"status": "new"},
                    "payload_hash": "not-the-canonical-hash",
                })


if __name__ == "__main__":
    unittest.main()
