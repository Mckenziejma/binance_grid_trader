from __future__ import annotations

import unittest
from threading import Thread

from gridtrader.core.enums import ReadinessState
from gridtrader.core.readiness import ReadinessGate
from gridtrader.recovery.manager import RecoveryManager, StaleRecoveryEpochError

from .support import BlockingFakeExchangeAdapter, NOW, make_harness


class StaleEpochRejectedTests(unittest.TestCase):
    def setUp(self) -> None:
        self.harness = make_harness()

    def tearDown(self) -> None:
        self.harness.close()

    def test_shutdown_fences_in_flight_recovery_before_commit(self) -> None:
        # Recovery re-reads account mode immediately before its epoch fence.
        blocking_exchange = BlockingFakeExchangeAdapter(
            self.harness.backend,
            block_on_call=2,
        )
        readiness = ReadinessGate()
        manager = RecoveryManager(
            exchange=blocking_exchange,
            ledger=self.harness.ledger,
            readiness=readiness,
            clock=lambda: NOW,
        )
        outcome: dict[str, object] = {}

        def recover() -> None:
            try:
                outcome["result"] = manager.recover(
                    self.harness.request(run_id="run-stale-epoch")
                )
            except BaseException as error:  # pragma: no cover - asserted below
                outcome["error"] = error

        thread = Thread(target=recover, daemon=True)
        thread.start()
        try:
            self.assertTrue(blocking_exchange.entered.wait(timeout=3))
            manager.begin_shutdown()
        finally:
            blocking_exchange.release.set()
            thread.join(timeout=5)

        self.assertFalse(thread.is_alive())
        self.assertIsInstance(outcome.get("error"), StaleRecoveryEpochError)
        self.assertNotIn("result", outcome)
        self.assertIs(readiness.state, ReadinessState.STOPPING)
        self.assertTrue(manager.wait_for_shutdown_drain(timeout=1))
        self.assertTrue(manager.finish_shutdown(timeout=1))
        self.assertIs(readiness.state, ReadinessState.STOPPED)
        with self.harness.ledger.unit_of_work(immediate=False) as uow:
            checkpoint = uow.recovery_checkpoints.latest_for_strategy("strategy-1")
        self.assertEqual(checkpoint["status"], "BLOCKED")
        self.assertIn("StaleRecoveryEpochError", checkpoint["error"])


if __name__ == "__main__":
    unittest.main()
