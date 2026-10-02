from __future__ import annotations

import gc
import tempfile
import unittest
import weakref
from pathlib import Path

from gridtrader.core.enums import ReadinessState

from tests.scenarios.support import (
    CLIENT_ORDER_ID,
    LOCAL_ORDER_ID,
    STRATEGY_ID,
    FixedClock,
    build_runtime,
    make_backend,
    make_exchange_order,
    make_request,
    register_run,
    seed_strategy_and_order,
)


class RestartRecoveryScenarioTests(unittest.TestCase):
    def test_restart_rebuilds_runtime_and_resolves_ack_unknown_without_a_write(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            db_path = Path(temporary_directory) / "restart.sqlite3"
            clock = FixedClock()
            backend = make_backend(clock)
            backend.seed_order(make_exchange_order(contracts=2))

            crashed = build_runtime(db_path, backend, clock)
            seed_strategy_and_order(crashed.ledger, contracts=2, local_state="ack_unknown")
            old_refs = (
                weakref.ref(crashed.ledger),
                weakref.ref(crashed.adapter),
                weakref.ref(crashed.manager),
            )
            del crashed
            gc.collect()
            self.assertTrue(all(reference() is None for reference in old_refs))

            restarted = build_runtime(db_path, backend, clock)
            register_run(restarted.ledger, "run-after-restart")
            result = restarted.manager.recover(make_request("run-after-restart"))

            self.assertTrue(result.complete)
            self.assertIs(result.state, ReadinessState.READY)
            with restarted.ledger.unit_of_work(immediate=False) as uow:
                order = uow.orders.require(LOCAL_ORDER_ID)
                checkpoint = uow.recovery_checkpoints.latest_complete(STRATEGY_ID)
            self.assertEqual(order["client_order_id"], CLIENT_ORDER_ID)
            self.assertEqual(order["exchange_order_id"], "90001")
            self.assertEqual(order["local_state"], "active")
            self.assertEqual(order["exchange_status"], "new")
            self.assertIsNotNone(checkpoint)
            self.assertEqual(checkpoint["status"], "COMPLETE")
            self.assertEqual(backend.operation_call_count("submit_limit_order"), 0)
            self.assertEqual(backend.operation_call_count("cancel_order"), 0)
