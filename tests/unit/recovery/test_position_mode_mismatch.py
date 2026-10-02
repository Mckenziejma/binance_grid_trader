from __future__ import annotations

import unittest

from gridtrader.core.enums import ReadinessState
from gridtrader.exchange.models import PositionMode

from .support import NOW, make_harness


class PositionModeMismatchTests(unittest.TestCase):
    def setUp(self) -> None:
        self.harness = make_harness(position_mode=PositionMode.HEDGE)

    def tearDown(self) -> None:
        self.harness.close()

    def test_account_mode_mismatch_persists_blocked_checkpoint(self) -> None:
        result = self.harness.manager.recover(
            self.harness.request(expected_position_mode=PositionMode.ONE_WAY)
        )

        expected = "POSITION_MODE_MISMATCH:expected=one_way observed=hedge"
        self.assertFalse(result.complete)
        self.assertIs(result.state, ReadinessState.DEGRADED)
        self.assertIn(expected, result.blockers)
        with self.harness.ledger.unit_of_work(immediate=False) as uow:
            checkpoint = uow.recovery_checkpoints.require(result.checkpoint_id)
        self.assertEqual(checkpoint["status"], "BLOCKED")
        self.assertIn(expected, checkpoint["error"])

    def test_matching_hedge_mode_stays_blocked_without_per_side_baseline(self) -> None:
        result = self.harness.manager.recover(
            self.harness.request(
                run_id="run-hedge-baseline-unavailable",
                expected_position_mode=PositionMode.HEDGE,
            )
        )

        self.assertFalse(result.complete)
        self.assertIs(result.state, ReadinessState.DEGRADED)
        self.assertIn(
            "HEDGE_POSITION_BASELINE_UNSUPPORTED_PHASE2",
            result.blockers,
        )

    def test_expected_hedge_but_observed_one_way_is_also_blocked(self) -> None:
        self.harness.backend.set_position_mode(
            PositionMode.ONE_WAY,
            observed_at=NOW,
        )

        result = self.harness.manager.recover(
            self.harness.request(
                run_id="run-reverse-position-mode-mismatch",
                expected_position_mode=PositionMode.HEDGE,
            )
        )

        self.assertFalse(result.complete)
        self.assertIn(
            "POSITION_MODE_MISMATCH:expected=hedge observed=one_way",
            result.blockers,
        )


if __name__ == "__main__":
    unittest.main()
