from __future__ import annotations

import shutil
import sqlite3
import tempfile
import unittest
from pathlib import Path

from gridtrader.storage import (
    InvariantViolation,
    MigrationError,
    SQLiteDatabase,
    SQLiteLedger,
)

from .support import NOW, strategy_record


MIGRATIONS = (
    Path(__file__).resolve().parents[3]
    / "gridtrader"
    / "storage"
    / "sqlite"
    / "migrations"
)


class Phase2MigrationTests(unittest.TestCase):
    def test_phase1_database_upgrades_without_losing_rows(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            phase1_migrations = root / "phase1"
            phase1_migrations.mkdir()
            for name in ("0001_initial.sql", "0002_runtime_safety.sql"):
                shutil.copyfile(MIGRATIONS / name, phase1_migrations / name)
            path = root / "ledger.sqlite3"
            phase1 = SQLiteDatabase(path, migrations_path=phase1_migrations)
            phase1.migrate()
            connection = phase1.connect()
            try:
                columns = list(strategy_record())
                connection.execute("BEGIN IMMEDIATE")
                connection.execute(
                    "INSERT INTO strategies ({}) VALUES ({})".format(
                        ", ".join(columns), ", ".join("?" for _ in columns)
                    ),
                    tuple(strategy_record()[column] for column in columns),
                )
                connection.commit()
            finally:
                connection.close()

            upgraded = SQLiteLedger(path)
            upgraded.initialize()
            with upgraded.unit_of_work(immediate=False) as uow:
                self.assertEqual(
                    "BTCUSD_PERP", uow.strategies.require("strategy-1")["symbol"]
                )
                self.assertEqual([], uow.instrument_rules.list_all())
                self.assertEqual([], uow.exchange_trade_observations.list_all())

    def test_migration_checksum_change_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            copied = root / "migrations"
            shutil.copytree(MIGRATIONS, copied)
            database = SQLiteDatabase(root / "ledger.sqlite3", migrations_path=copied)
            database.migrate()
            target = copied / "0003_phase2_recovery_observations.sql"
            target.write_text(
                target.read_text(encoding="utf-8") + "\n-- changed\n",
                encoding="utf-8",
            )
            with self.assertRaisesRegex(MigrationError, "applied migration 3 has changed"):
                database.migrate()

    def test_failed_migration_rolls_back_schema_and_version(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            migrations = root / "migrations"
            migrations.mkdir()
            (migrations / "0001_base.sql").write_text(
                "CREATE TABLE stable (id INTEGER PRIMARY KEY) STRICT;\n",
                encoding="utf-8",
            )
            (migrations / "0002_broken.sql").write_text(
                "CREATE TABLE should_rollback (id INTEGER);\n"
                "INSERT INTO table_that_does_not_exist VALUES (1);\n",
                encoding="utf-8",
            )
            database = SQLiteDatabase(root / "ledger.sqlite3", migrations_path=migrations)
            with self.assertRaises(MigrationError):
                database.migrate()
            connection = database.connect()
            try:
                self.assertIsNone(
                    connection.execute(
                        "SELECT name FROM sqlite_master WHERE name = 'should_rollback'"
                    ).fetchone()
                )
                versions = connection.execute(
                    "SELECT version FROM schema_migrations ORDER BY version"
                ).fetchall()
                self.assertEqual([1], [row[0] for row in versions])
            finally:
                connection.close()

    def test_rules_and_position_mode_observations_are_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            ledger = SQLiteLedger(Path(directory) / "ledger.sqlite3")
            ledger.initialize()
            with ledger.unit_of_work() as uow:
                uow.strategies.add(strategy_record())
                checkpoint = uow.recovery_checkpoints.add(
                    {
                        "run_id": "run-phase2",
                        "strategy_id": "strategy-1",
                        "account_id": "coin-m-main",
                        "symbol": "BTCUSD_PERP",
                        "reason": "STARTUP",
                        "status": "STARTED",
                        "recovery_epoch": 1,
                        "started_at_ms": NOW,
                    }
                )
                rules = {
                    "symbol": "BTCUSD_PERP",
                    "pair": "BTCUSD",
                    "contract_type": "PERPETUAL",
                    "status": "TRADING",
                    "contract_size": "100",
                    "margin_asset": "BTC",
                    "tick_size": "0.1",
                    "quantity_step": 1,
                    "min_qty": 1,
                    "max_qty": 1000,
                    "min_price": "0.1",
                    "max_price": "1000000",
                    "supported_order_types_json": '["LIMIT"]',
                    "observed_at_ms": NOW,
                    "payload_hash": "payload-a",
                    "rules_hash": "rules-a",
                }
                first, first_inserted = uow.instrument_rules.record_observation(rules)
                second, second_inserted = uow.instrument_rules.record_observation(rules)
                self.assertTrue(first_inserted)
                self.assertFalse(second_inserted)
                self.assertEqual(first["instrument_rule_id"], second["instrument_rule_id"])
                mode = {
                    "account_id": "coin-m-main",
                    "mode": "one_way",
                    "observed_at_ms": NOW,
                    "checkpoint_id": checkpoint["checkpoint_id"],
                    "source": "REST",
                }
                _, inserted = uow.position_mode_observations.add_idempotent(mode)
                _, repeated = uow.position_mode_observations.add_idempotent(mode)
                self.assertTrue(inserted)
                self.assertFalse(repeated)
                trade = {
                    "account_id": "coin-m-main",
                    "symbol": "BTCUSD_PERP",
                    "binance_trade_id": "trade-1",
                    "exchange_order_id": "exchange-order-1",
                    "client_order_id": "manual-order-1",
                    "side": "buy",
                    "position_side": "both",
                    "price": "50000",
                    "fill_contracts": 1,
                    "commission": "0.000001",
                    "commission_asset": "BTC",
                    "realized_pnl": "0",
                    "is_maker": None,
                    "trade_time_ms": NOW,
                    "first_checkpoint_id": checkpoint["checkpoint_id"],
                    "last_checkpoint_id": checkpoint["checkpoint_id"],
                    "first_observed_at_ms": NOW,
                    "last_observed_at_ms": NOW,
                }
                first_trade, first_trade_inserted = (
                    uow.exchange_trade_observations.record_observation(trade)
                )
                second_trade, second_trade_inserted = (
                    uow.exchange_trade_observations.record_observation(trade)
                )
                self.assertTrue(first_trade_inserted)
                self.assertFalse(second_trade_inserted)
                self.assertEqual(first_trade, second_trade)
                conflicting_trade = dict(trade, fill_contracts=2)
                with self.assertRaises(InvariantViolation):
                    uow.exchange_trade_observations.record_observation(
                        conflicting_trade
                    )
                eth_trade = dict(
                    trade,
                    symbol="ETHUSD_PERP",
                    exchange_order_id="exchange-order-eth",
                    commission_asset="ETH",
                )
                uow.exchange_trade_observations.record_observation(eth_trade)
                self.assertEqual(
                    uow.exchange_trade_observations.require(
                        ("coin-m-main", "BTCUSD_PERP", "trade-1")
                    )["exchange_order_id"],
                    "exchange-order-1",
                )
                self.assertEqual(
                    uow.exchange_trade_observations.require(
                        ("coin-m-main", "ETHUSD_PERP", "trade-1")
                    )["exchange_order_id"],
                    "exchange-order-eth",
                )
                with self.assertRaises(ValueError):
                    uow.exchange_trade_observations.get("trade-1")
                uow.commit()


if __name__ == "__main__":
    unittest.main()
