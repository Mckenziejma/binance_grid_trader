from __future__ import annotations

import unittest
from datetime import timedelta
from threading import Lock, Thread

from gridtrader.core.readiness import ReadinessGate
from gridtrader.recovery.manager import (
    RecoveryBlockedError,
    RecoveryInProgressError,
    RecoveryManager,
    StaleRecoveryLeaseError,
)

from .support import BlockingFakeExchangeAdapter, NOW, make_harness


class FirstProcessRecoveryManager(RecoveryManager):
    _registry_guard = Lock()
    _singleflight: dict[tuple[str, str], Lock] = {}


class SecondProcessRecoveryManager(RecoveryManager):
    _registry_guard = Lock()
    _singleflight: dict[tuple[str, str], Lock] = {}


class RecoverySingleFlightTests(unittest.TestCase):
    def setUp(self) -> None:
        self.harness = make_harness()

    def tearDown(self) -> None:
        self.harness.close()

    def test_same_account_and_strategy_cannot_recover_concurrently(self) -> None:
        blocking_exchange = BlockingFakeExchangeAdapter(self.harness.backend)
        first_manager = RecoveryManager(
            exchange=blocking_exchange,
            ledger=self.harness.ledger,
            readiness=ReadinessGate(),
            clock=lambda: NOW,
        )
        second_manager = RecoveryManager(
            exchange=self.harness.exchange,
            ledger=self.harness.ledger,
            readiness=ReadinessGate(),
            clock=lambda: NOW,
        )
        outcome: dict[str, object] = {}

        def run_first() -> None:
            try:
                outcome["result"] = first_manager.recover(
                    self.harness.request(run_id="run-flight-1")
                )
            except BaseException as error:  # pragma: no cover - asserted below
                outcome["error"] = error

        thread = Thread(target=run_first, daemon=True)
        thread.start()
        try:
            self.assertTrue(blocking_exchange.entered.wait(timeout=3))
            with self.assertRaises(RecoveryInProgressError):
                second_manager.recover(
                    self.harness.request(run_id="run-flight-2")
                )
        finally:
            blocking_exchange.release.set()
            thread.join(timeout=5)

        self.assertFalse(thread.is_alive())
        self.assertNotIn("error", outcome)
        self.assertTrue(outcome["result"].complete)

    def test_sqlite_lease_blocks_an_independent_process_registry(self) -> None:
        blocking_exchange = BlockingFakeExchangeAdapter(self.harness.backend)
        first = FirstProcessRecoveryManager(
            exchange=blocking_exchange,
            ledger=self.harness.ledger,
            readiness=ReadinessGate(),
            clock=lambda: NOW,
        )
        second = SecondProcessRecoveryManager(
            exchange=self.harness.exchange,
            ledger=self.harness.ledger,
            readiness=ReadinessGate(),
            clock=lambda: NOW,
        )
        outcome: dict[str, object] = {}

        def run_first() -> None:
            try:
                outcome["result"] = first.recover(
                    self.harness.request(run_id="run-process-1")
                )
            except BaseException as error:  # pragma: no cover - asserted below
                outcome["error"] = error

        thread = Thread(target=run_first, daemon=True)
        thread.start()
        try:
            self.assertTrue(blocking_exchange.entered.wait(timeout=3))
            with self.assertRaises(RecoveryInProgressError):
                second.recover(self.harness.request(run_id="run-process-2"))
        finally:
            blocking_exchange.release.set()
            thread.join(timeout=5)

        self.assertFalse(thread.is_alive())
        self.assertNotIn("error", outcome)

    def test_expired_owner_is_fenced_after_another_process_takes_over(self) -> None:
        now = [NOW]
        blocking_exchange = BlockingFakeExchangeAdapter(self.harness.backend)
        first = FirstProcessRecoveryManager(
            exchange=blocking_exchange,
            ledger=self.harness.ledger,
            readiness=ReadinessGate(),
            clock=lambda: now[0],
            lease_ttl=timedelta(seconds=1),
        )
        second = SecondProcessRecoveryManager(
            exchange=self.harness.exchange,
            ledger=self.harness.ledger,
            readiness=ReadinessGate(),
            clock=lambda: now[0],
            lease_ttl=timedelta(seconds=1),
        )
        outcome: dict[str, object] = {}

        def run_first() -> None:
            try:
                outcome["result"] = first.recover(
                    self.harness.request(run_id="run-expired-owner")
                )
            except BaseException as error:  # pragma: no cover - asserted below
                outcome["error"] = error

        thread = Thread(target=run_first, daemon=True)
        thread.start()
        try:
            self.assertTrue(blocking_exchange.entered.wait(timeout=3))
            now[0] = NOW + timedelta(seconds=2)
            takeover = second.recover(
                self.harness.request(run_id="run-takeover-owner")
            )
            self.assertTrue(takeover.complete)
        finally:
            blocking_exchange.release.set()
            thread.join(timeout=5)

        self.assertFalse(thread.is_alive())
        self.assertIsInstance(outcome.get("error"), StaleRecoveryLeaseError)
        self.assertNotIn("result", outcome)
        with self.harness.ledger.unit_of_work(immediate=False) as uow:
            checkpoints = uow.recovery_checkpoints.list_all()
        self.assertEqual(
            [record["status"] for record in checkpoints],
            ["STARTED", "COMPLETE"],
        )
        self.assertLess(
            checkpoints[0]["recovery_epoch"],
            checkpoints[1]["recovery_epoch"],
        )

    def test_missing_bot_run_fails_before_checkpoint_or_exchange_read(self) -> None:
        request = self.harness.request(
            run_id="run-not-registered",
            register_run=False,
        )

        with self.assertRaises(RecoveryBlockedError):
            self.harness.manager.recover(request)

        with self.harness.ledger.unit_of_work(immediate=False) as uow:
            self.assertEqual(uow.recovery_checkpoints.list_all(), [])
        self.assertEqual(
            self.harness.backend.operation_call_count("get_position_mode"),
            0,
        )


if __name__ == "__main__":
    unittest.main()
