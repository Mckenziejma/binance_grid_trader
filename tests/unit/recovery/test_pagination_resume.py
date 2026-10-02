from __future__ import annotations

import unittest
from datetime import timedelta
from decimal import Decimal

from gridtrader.core.enums import ReadinessState, Side
from gridtrader.exchange.errors import ExchangeUnavailableError
from gridtrader.exchange.fake_backend import FakeFault, FakeFaultKind
from gridtrader.recovery.manager import RecoveryBlockedError
from tests.unit.storage.support import order_record

from .support import NOW, make_exchange_order, make_harness, make_position


class PaginationResumeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.harness = make_harness(default_page_size=1)

    def tearDown(self) -> None:
        self.harness.close()

    def test_interrupted_page_cursor_and_fills_are_resumed_idempotently(self) -> None:
        local = self.harness.add_local_order()
        self.harness.backend.seed_order(
            make_exchange_order(client_order_id=local["client_order_id"])
        )
        self.harness.backend.record_fill(
            local["symbol"],
            local["client_order_id"],
            contracts=1,
            price=Decimal("50000"),
            trade_id="trade-1",
        )
        self.harness.backend.record_fill(
            local["symbol"],
            local["client_order_id"],
            contracts=1,
            price=Decimal("50001"),
            trade_id="trade-2",
        )
        self.harness.add_local_position(2)

        # The first harmless data fault lets page one return; page two then fails.
        self.harness.backend.queue_fault(
            "get_user_trades",
            FakeFault(FakeFaultKind.DUPLICATE_DATA),
        )
        self.harness.backend.queue_fault(
            "get_user_trades",
            FakeFault(FakeFaultKind.PAGINATION_INTERRUPTION),
        )

        with self.assertRaises(ExchangeUnavailableError):
            self.harness.manager.recover(self.harness.request(run_id="run-page-1"))

        with self.harness.ledger.unit_of_work(immediate=False) as uow:
            interrupted = uow.recovery_checkpoints.latest_for_strategy("strategy-1")
            first_pass_fills = uow.fills.list_all()
        self.assertEqual(interrupted["status"], "BLOCKED")
        self.assertEqual(interrupted["trades_complete"], 0)
        self.assertIsNotNone(interrupted["next_trade_cursor"])
        self.assertEqual(
            [item["binance_trade_id"] for item in first_pass_fills],
            ["trade-1"],
        )

        result = self.harness.manager.recover(
            self.harness.request(run_id="run-page-2")
        )

        self.assertTrue(result.complete)
        self.assertIs(result.state, ReadinessState.READY)
        with self.harness.ledger.unit_of_work(immediate=False) as uow:
            all_fills = uow.fills.list_all()
            completed = uow.recovery_checkpoints.latest_complete("strategy-1")
        self.assertEqual(
            {item["binance_trade_id"] for item in all_fills},
            {"trade-1", "trade-2"},
        )
        self.assertEqual(completed["trades_complete"], 1)
        self.assertIsNone(completed["next_trade_cursor"])

    def test_resume_finishes_old_snapshot_then_catches_up_net_zero_fills(self) -> None:
        with self.harness.ledger.unit_of_work() as uow:
            uow.connection.execute(
                "UPDATE strategies SET initial_position_contracts = 2 "
                "WHERE strategy_id = 'strategy-1'"
            )
            uow.connection.execute(
                "UPDATE grid_levels SET planned_contracts = 3 "
                "WHERE level_id = 'level-1'"
            )
            uow.commit()
        self.harness.backend.set_position(make_position(2))
        buy = self.harness.add_local_order(quantity_contracts=3)
        sell = order_record(local_order_id="order-2", leg_role="grid_sell")
        sell.update(side="sell", intent="GRID_SELL", quantity_contracts=3)
        with self.harness.ledger.unit_of_work() as uow:
            sell = uow.orders.add(sell)
            uow.commit()

        self.harness.backend.seed_order(
            make_exchange_order(
                client_order_id=buy["client_order_id"],
                exchange_order_id="90001",
                original_contracts=3,
                side=Side.BUY,
            )
        )
        self.harness.backend.seed_order(
            make_exchange_order(
                client_order_id=sell["client_order_id"],
                exchange_order_id="90002",
                original_contracts=3,
                side=Side.SELL,
            )
        )
        for trade_id in ("old-1", "old-2"):
            self.harness.backend.record_fill(
                buy["symbol"],
                buy["client_order_id"],
                contracts=1,
                price=Decimal("50000"),
                trade_id=trade_id,
            )
        self.harness.add_local_position(2)
        self.harness.backend.queue_fault(
            "get_user_trades",
            FakeFault(FakeFaultKind.DUPLICATE_DATA),
        )
        self.harness.backend.queue_fault(
            "get_user_trades",
            FakeFault(FakeFaultKind.PAGINATION_INTERRUPTION),
        )

        with self.assertRaises(ExchangeUnavailableError):
            self.harness.manager.recover(
                self.harness.request(run_id="run-old-snapshot")
            )

        # These offsetting fills happen after the interrupted cursor's fixed
        # snapshot.  Current position remains 2, so positions alone cannot
        # prove that trade replay is current.
        self.harness.clock.value = NOW + timedelta(seconds=10)
        self.harness.backend.record_fill(
            buy["symbol"],
            buy["client_order_id"],
            contracts=1,
            price=Decimal("50000"),
            trade_id="new-buy",
        )
        self.harness.backend.record_fill(
            sell["symbol"],
            sell["client_order_id"],
            contracts=3,
            price=Decimal("50000"),
            trade_id="new-sell",
        )

        result = self.harness.manager.recover(
            self.harness.request(run_id="run-catch-up")
        )

        self.assertTrue(result.complete)
        self.assertIs(result.state, ReadinessState.READY)
        with self.harness.ledger.unit_of_work(immediate=False) as uow:
            trade_ids = {
                item["binance_trade_id"] for item in uow.fills.list_all()
            }
        self.assertEqual(
            {"old-1", "old-2", "new-buy", "new-sell"},
            trade_ids,
        )

    def test_request_cannot_skip_durable_trade_history(self) -> None:
        with self.assertRaisesRegex(
            RecoveryBlockedError,
            "from_time cannot skip",
        ):
            self.harness.manager.recover(
                self.harness.request(
                    run_id="run-late-start",
                    from_time=NOW + timedelta(seconds=1),
                )
            )
        self.assertIs(self.harness.readiness.state, ReadinessState.DEGRADED)

        with self.assertRaisesRegex(
            RecoveryBlockedError,
            "cursor does not match",
        ):
            self.harness.manager.recover(
                self.harness.request(
                    run_id="run-forged-cursor",
                    cursor="forged-cursor",
                )
            )

        earlier = self.harness.manager.recover(
            self.harness.request(
                run_id="run-earlier-start",
                from_time=NOW - timedelta(days=1),
            )
        )
        self.assertTrue(earlier.complete)
        self.assertIs(earlier.state, ReadinessState.READY)


if __name__ == "__main__":
    unittest.main()
