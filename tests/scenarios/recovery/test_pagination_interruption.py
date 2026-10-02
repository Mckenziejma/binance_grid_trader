from __future__ import annotations

import gc
import tempfile
import unittest
import weakref
from decimal import Decimal
from pathlib import Path

from gridtrader.core.enums import ReadinessState
from gridtrader.exchange.errors import ExchangeUnavailableError
from gridtrader.exchange.fake_backend import FakeFault, FakeFaultKind

from tests.scenarios.support import (
    CLIENT_ORDER_ID,
    STRATEGY_ID,
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


class PaginationInterruptionScenarioTests(unittest.TestCase):
    def test_interrupted_trade_pagination_resumes_from_durable_cursor(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            db_path = Path(temporary_directory) / "pagination-resume.sqlite3"
            clock = FixedClock()
            backend = make_backend(clock, default_page_size=2)
            backend.seed_order(make_exchange_order(contracts=8))
            for number in range(1, 4):
                backend.record_fill(
                    "BTCUSD_PERP",
                    CLIENT_ORDER_ID,
                    contracts=1,
                    price=Decimal(65_000 + number),
                    trade_id=f"paged-trade-{number}",
                )

            interrupted_runtime = build_runtime(db_path, backend, clock)
            seed_strategy_and_order(interrupted_runtime.ledger, contracts=8)
            seed_local_position(interrupted_runtime.ledger, contracts=3)
            register_run(interrupted_runtime.ledger, "run-interrupted")
            backend.queue_fault(
                "get_user_trades",
                FakeFault(FakeFaultKind.DUPLICATE_DATA),
            )
            backend.queue_fault(
                "get_user_trades",
                FakeFault(
                    FakeFaultKind.PAGINATION_INTERRUPTION,
                    "second page unavailable",
                ),
            )

            with self.assertRaisesRegex(
                ExchangeUnavailableError,
                "second page unavailable",
            ):
                interrupted_runtime.manager.recover(make_request("run-interrupted"))

            self.assertIs(interrupted_runtime.readiness.state, ReadinessState.DEGRADED)
            with interrupted_runtime.ledger.unit_of_work(immediate=False) as uow:
                interrupted = uow.recovery_checkpoints.latest_for_strategy(STRATEGY_ID)
                partial_fills = rows(uow.fills)
            self.assertIsNotNone(interrupted)
            self.assertEqual(interrupted["status"], "BLOCKED")
            self.assertEqual(interrupted["trades_complete"], 0)
            self.assertIsNotNone(interrupted["next_trade_cursor"])
            self.assertEqual(
                {item["binance_trade_id"] for item in partial_fills},
                {"paged-trade-1", "paged-trade-2"},
            )
            self.assertEqual(len(partial_fills), 2)

            stale_refs = (
                weakref.ref(interrupted_runtime.ledger),
                weakref.ref(interrupted_runtime.adapter),
                weakref.ref(interrupted_runtime.manager),
            )
            del interrupted_runtime
            gc.collect()
            self.assertTrue(all(reference() is None for reference in stale_refs))

            resumed_runtime = build_runtime(db_path, backend, clock)
            register_run(resumed_runtime.ledger, "run-resumed")
            resumed = resumed_runtime.manager.recover(make_request("run-resumed"))

            self.assertTrue(resumed.complete)
            self.assertIs(resumed.state, ReadinessState.READY)
            with resumed_runtime.ledger.unit_of_work(immediate=False) as uow:
                final_fills = rows(uow.fills)
                latest = uow.recovery_checkpoints.latest_for_strategy(STRATEGY_ID)
            self.assertEqual(
                {item["binance_trade_id"] for item in final_fills},
                {"paged-trade-1", "paged-trade-2", "paged-trade-3"},
            )
            self.assertEqual(len(final_fills), 3)
            self.assertIsNotNone(latest)
            self.assertEqual(latest["status"], "COMPLETE")
            self.assertEqual(latest["trades_complete"], 1)
            # Two calls before interruption, one to finish the old fixed
            # snapshot, two fresh catch-up pages, two overlapping tail pages,
            # and two final pages after account-fact confirmation.
            self.assertEqual(backend.operation_call_count("get_user_trades"), 9)
