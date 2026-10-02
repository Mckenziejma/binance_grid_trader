from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from gridtrader.storage import ConstraintViolation, InvariantViolation

from .support import (
    NOW,
    fill_record,
    generation_record,
    level_record,
    new_ledger,
    order_record,
    seed_grid,
    strategy_record,
)


class ConstraintTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "ledger.sqlite3"
        self.ledger = new_ledger(self.db_path)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_only_one_active_generation_per_strategy(self) -> None:
        with self.ledger.unit_of_work() as uow:
            seed_grid(uow)
            uow.grid_generations.add(generation_record(
                "generation-2",
                generation_no=2,
                logical_level_count=1,
                max_active_orders=1,
            ))
            uow.grid_levels.add(level_record(
                "level-2",
                generation_id="generation-2",
                level_index=0,
            ))
            with self.assertRaises(ConstraintViolation):
                uow.grid_generations.activate("generation-2", NOW + 1)

    def test_generation_rejects_invalid_bounds_and_incomplete_activation(self) -> None:
        with self.ledger.unit_of_work() as uow:
            uow.strategies.add(strategy_record())
            invalid = generation_record(status="draft")
            invalid["lower_price"] = "70000"
            invalid["upper_price"] = "50000"
            with self.assertRaises(InvariantViolation):
                uow.grid_generations.add(invalid)

            generation = generation_record(status="draft")
            uow.grid_generations.add(generation)
            with self.assertRaises(InvariantViolation):
                uow.grid_generations.activate("generation-1", NOW + 1)

    def test_unfinished_states_reserve_logical_slot_until_terminal(self) -> None:
        with self.ledger.unit_of_work() as uow:
            seed_grid(uow)
            uow.orders.add(order_record())
            with self.assertRaises(ConstraintViolation):
                uow.orders.add(order_record("order-2", local_state="blocked"))
            terminal = uow.orders.record_exchange_update(
                "order-1",
                exchange_status="canceled",
                cumulative_filled_contracts=0,
                exchange_update_ms=NOW + 10,
                updated_at_ms=NOW + 11,
            )
            self.assertEqual("terminal", terminal["local_state"])
            replacement = uow.orders.add(order_record(
                "order-2",
                local_state="blocked",
                cycle_no=1,
            ))
            self.assertEqual("blocked", replacement["local_state"])

    def test_client_order_id_is_unique(self) -> None:
        with self.ledger.unit_of_work() as uow:
            seed_grid(uow)
            uow.grid_levels.add(level_record("level-2", level_index=1))
            uow.orders.add(order_record())
            duplicate = order_record("order-2", level_id="level-2")
            duplicate["client_order_id"] = order_record()["client_order_id"]
            with self.assertRaises(InvariantViolation):
                uow.orders.add(duplicate)

    def test_noncanonical_slot_key_is_rejected_for_bot_order(self) -> None:
        with self.ledger.unit_of_work() as uow:
            seed_grid(uow)
            record = order_record()
            record["logical_slot_key"] = "same-fields-but-different-text"
            with self.assertRaises(InvariantViolation):
                uow.orders.add(record)

    def test_fill_is_idempotent_by_account_symbol_and_trade_id(self) -> None:
        with self.ledger.unit_of_work() as uow:
            seed_grid(uow)
            uow.orders.add(order_record())
            first, inserted = uow.fills.add_idempotent(fill_record())
            self.assertTrue(inserted)
            duplicate = fill_record("fill-from-rest", "trade-100")
            second, inserted = uow.fills.add_idempotent(duplicate)
            self.assertFalse(inserted)
            self.assertEqual(first["fill_id"], second["fill_id"])
            self.assertEqual(1, len(uow.fills.list_all()))

    def test_conflicting_duplicate_fill_is_rejected(self) -> None:
        with self.ledger.unit_of_work() as uow:
            seed_grid(uow)
            uow.orders.add(order_record())
            uow.fills.add_idempotent(fill_record())
            with self.assertRaises(InvariantViolation):
                uow.fills.add_idempotent(fill_record(
                    "fill-conflict", "trade-100", fill_contracts=2
                ))

    def test_exchange_update_is_monotonic_and_terminal_cannot_regress(self) -> None:
        with self.ledger.unit_of_work() as uow:
            seed_grid(uow)
            uow.orders.add(order_record())
            partial = uow.orders.record_exchange_update(
                "order-1",
                exchange_status="partially_filled",
                cumulative_filled_contracts=1,
                exchange_update_ms=NOW + 10,
                updated_at_ms=NOW + 11,
                exchange_order_id="90001",
                avg_fill_price="50000",
                last_source="BINANCE_WS",
            )
            self.assertEqual(1, partial["cumulative_filled_contracts"])
            with self.assertRaises(InvariantViolation):
                uow.orders.record_exchange_update(
                    "order-1",
                    exchange_status="filled",
                    cumulative_filled_contracts=1,
                    exchange_update_ms=NOW + 12,
                    updated_at_ms=NOW + 13,
                )
            with self.assertRaises(InvariantViolation):
                uow.orders.record_exchange_update(
                    "order-1",
                    exchange_status="partially_filled",
                    cumulative_filled_contracts=1,
                    exchange_update_ms=NOW + 12,
                    updated_at_ms=NOW + 13,
                    exchange_order_id="different-order-id",
                )
            with self.assertRaises(InvariantViolation):
                uow.orders.record_exchange_update(
                    "order-1",
                    exchange_status="new",
                    cumulative_filled_contracts=0,
                    exchange_update_ms=NOW + 12,
                    updated_at_ms=NOW + 13,
                )
            filled = uow.orders.record_exchange_update(
                "order-1",
                exchange_status="filled",
                cumulative_filled_contracts=2,
                exchange_update_ms=NOW + 14,
                updated_at_ms=NOW + 15,
            )
            self.assertEqual("terminal", filled["local_state"])
            with self.assertRaises(InvariantViolation):
                uow.orders.record_exchange_update(
                    "order-1",
                    exchange_status="new",
                    cumulative_filled_contracts=2,
                    exchange_update_ms=NOW + 16,
                    updated_at_ms=NOW + 17,
                )

    def test_recovery_checkpoint_cannot_skip_reconciliation(self) -> None:
        with self.ledger.unit_of_work() as uow:
            seed_grid(uow)
            with self.assertRaises(InvariantViolation):
                uow.recovery_checkpoints.add({
                    "run_id": "run-1",
                    "strategy_id": "strategy-1",
                    "account_id": "coin-m-main",
                    "symbol": "BTCUSD_PERP",
                    "reason": "STARTUP",
                    "status": "COMPLETE",
                    "started_at_ms": NOW,
                })


if __name__ == "__main__":
    unittest.main()
