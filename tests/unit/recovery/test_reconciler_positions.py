from __future__ import annotations

import unittest
from decimal import Decimal

from gridtrader.core.enums import PositionSide, ReadinessState
from gridtrader.recovery.models import ReconciliationKind
from gridtrader.recovery.reconciler import Reconciler

from .support import (
    STRATEGY_ID,
    SYMBOL,
    make_exchange_order,
    make_harness,
    make_position,
)


class ReconcilerPositionTests(unittest.TestCase):
    def test_equal_position_is_matched(self) -> None:
        result = Reconciler.reconcile_positions(
            (
                {
                    "symbol": SYMBOL,
                    "position_side": PositionSide.BOTH.value,
                    "quantity_contracts": 3,
                },
            ),
            (make_position(3),),
        )

        self.assertEqual(len(result), 1)
        self.assertIs(result[0].kind, ReconciliationKind.MATCHED)
        self.assertFalse(result[0].blocks_ready)

    def test_local_exchange_quantity_mismatch_blocks_readiness(self) -> None:
        result = Reconciler.reconcile_positions(
            (
                {
                    "symbol": SYMBOL,
                    "position_side": PositionSide.BOTH.value,
                    "quantity_contracts": 1,
                },
            ),
            (make_position(2),),
        )

        self.assertIs(result[0].kind, ReconciliationKind.POSITION_MISMATCH)
        self.assertEqual(result[0].local_contracts, 1)
        self.assertEqual(result[0].exchange_contracts, 2)
        self.assertTrue(result[0].blocks_ready)

    def test_exchange_only_position_is_compared_against_zero(self) -> None:
        result = Reconciler.reconcile_positions((), (make_position(-2),))

        self.assertEqual(result[0].local_contracts, 0)
        self.assertEqual(result[0].exchange_contracts, -2)
        self.assertIs(result[0].kind, ReconciliationKind.POSITION_MISMATCH)

    def test_recovery_manager_position_mismatch_blocks_ready(self) -> None:
        harness = make_harness()
        try:
            harness.add_local_position(1)
            harness.backend.set_position(make_position(2))

            result = harness.manager.recover(
                harness.request(run_id="run-position-mismatch")
            )

            self.assertFalse(result.complete)
            self.assertIs(result.state, ReadinessState.DEGRADED)
            self.assertTrue(
                any("POSITION_MISMATCH" in blocker for blocker in result.blockers)
            )
            with harness.ledger.unit_of_work(immediate=False) as uow:
                checkpoint = uow.recovery_checkpoints.require(result.checkpoint_id)
            self.assertEqual(checkpoint["status"], "BLOCKED")
        finally:
            harness.close()

    def test_position_observation_never_becomes_next_recovery_expectation(self) -> None:
        harness = make_harness()
        try:
            harness.backend.set_position(make_position(2))

            first = harness.manager.recover(
                harness.request(run_id="run-position-mismatch-first")
            )
            second = harness.manager.recover(
                harness.request(run_id="run-position-mismatch-second")
            )

            self.assertFalse(first.complete)
            self.assertFalse(second.complete)
            self.assertIs(second.state, ReadinessState.DEGRADED)
            self.assertTrue(
                any("POSITION_MISMATCH" in blocker for blocker in second.blockers)
            )
            self.assertEqual(second.report.positions[0].local_contracts, 0)
            self.assertEqual(second.report.positions[0].exchange_contracts, 2)
        finally:
            harness.close()

    def test_initial_position_and_owned_fills_rebuild_strategy_projection(self) -> None:
        harness = make_harness()
        try:
            with harness.ledger.unit_of_work() as uow:
                uow.connection.execute(
                    "UPDATE strategies SET initial_position_contracts = 3 "
                    "WHERE strategy_id = ?",
                    (STRATEGY_ID,),
                )
                uow.commit()
            local = harness.add_local_order()
            harness.backend.seed_order(
                make_exchange_order(client_order_id=local["client_order_id"])
            )
            harness.backend.record_fill(
                SYMBOL,
                local["client_order_id"],
                contracts=2,
                price=Decimal("50000"),
                trade_id="position-projection-fill",
            )
            # The configured baseline existed before the bot-owned fill.
            harness.backend.set_position(make_position(5))

            result = harness.manager.recover(
                harness.request(run_id="run-position-projection")
            )

            self.assertTrue(result.complete)
            self.assertIs(result.state, ReadinessState.READY)
            self.assertEqual(result.report.positions[0].local_contracts, 5)
            self.assertEqual(result.report.positions[0].exchange_contracts, 5)
        finally:
            harness.close()


if __name__ == "__main__":
    unittest.main()
