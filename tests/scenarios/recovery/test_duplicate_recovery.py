from __future__ import annotations

import gc
import tempfile
import unittest
import weakref
from decimal import Decimal
from pathlib import Path

from gridtrader.core.enums import ReadinessState
from gridtrader.exchange.fake_backend import FakeFault, FakeFaultKind

from tests.scenarios.support import (
    CLIENT_ORDER_ID,
    LOCAL_ORDER_ID,
    FixedClock,
    build_runtime,
    make_backend,
    make_exchange_order,
    make_request,
    register_run,
    rows,
    seed_local_position,
    seed_strategy_and_order,
)


class DuplicateRecoveryScenarioTests(unittest.TestCase):
    def test_repeated_recovery_and_duplicate_rest_fill_are_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            db_path = Path(temporary_directory) / "duplicate-recovery.sqlite3"
            clock = FixedClock()
            backend = make_backend(clock)
            backend.seed_order(make_exchange_order(contracts=4))
            backend.record_fill(
                "BTCUSD_PERP",
                CLIENT_ORDER_ID,
                contracts=1,
                price=Decimal("65000"),
                trade_id="stable-trade-id",
            )

            first_runtime = build_runtime(db_path, backend, clock)
            seed_strategy_and_order(first_runtime.ledger, contracts=4)
            seed_local_position(first_runtime.ledger, contracts=1)
            register_run(first_runtime.ledger, "run-first")
            first = first_runtime.manager.recover(make_request("run-first"))
            self.assertTrue(first.complete)
            stale_refs = (
                weakref.ref(first_runtime.ledger),
                weakref.ref(first_runtime.adapter),
                weakref.ref(first_runtime.manager),
            )
            del first_runtime
            gc.collect()
            self.assertTrue(all(reference() is None for reference in stale_refs))

            backend.queue_fault(
                "get_user_trades",
                FakeFault(FakeFaultKind.DUPLICATE_DATA),
            )
            second_runtime = build_runtime(db_path, backend, clock)
            register_run(second_runtime.ledger, "run-second")
            second = second_runtime.manager.recover(make_request("run-second"))

            self.assertTrue(second.complete)
            self.assertIs(second.state, ReadinessState.READY)
            with second_runtime.ledger.unit_of_work(immediate=False) as uow:
                order = uow.orders.require(LOCAL_ORDER_ID)
                fills = rows(uow.fills)
                checkpoints = rows(uow.recovery_checkpoints)
            self.assertEqual(order["exchange_status"], "partially_filled")
            self.assertEqual(order["cumulative_filled_contracts"], 1)
            self.assertEqual(len(fills), 1)
            self.assertEqual(fills[0]["binance_trade_id"], "stable-trade-id")
            self.assertEqual(fills[0]["source"], "REST_BACKFILL")
            self.assertEqual(
                [item["status"] for item in checkpoints],
                ["COMPLETE", "COMPLETE"],
            )
