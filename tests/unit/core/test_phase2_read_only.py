from __future__ import annotations

import unittest
from decimal import Decimal
from unittest.mock import patch

import gridtrader.core.runtime as runtime
from gridtrader.core.enums import PositionSide, Side
from gridtrader.core.errors import TradingDisabledError
from gridtrader.core.readiness import DEFAULT_RECOVERY_CHECKS, ReadinessGate
from gridtrader.core.runtime import READ_ONLY_MODE, TRADING_ENABLED
from gridtrader.exchange.fake import FakeExchangeAdapter
from gridtrader.exchange.fake_backend import FakeExchangeBackend
from gridtrader.exchange.models import SubmitLimitOrder
from gridtrader.recovery.lifecycle import ShutdownPlan, ShutdownPolicy


class Phase2ReadOnlyTests(unittest.TestCase):
    def test_ready_does_not_enable_exchange_writes(self) -> None:
        gate = ReadinessGate()
        epoch = gate.begin_recovery()
        for check in DEFAULT_RECOVERY_CHECKS:
            gate.record_check(check, recovery_epoch=epoch)
        gate.mark_ready(recovery_epoch=epoch)
        self.assertTrue(gate.can_submit_orders)
        self.assertTrue(READ_ONLY_MODE)
        self.assertFalse(TRADING_ENABLED)

        adapter = FakeExchangeAdapter(FakeExchangeBackend())
        command = SubmitLimitOrder(
            symbol="BTCUSD_PERP",
            client_order_id="dg1_phase2-write-disabled",
            side=Side.BUY,
            position_side=PositionSide.BOTH,
            price=Decimal("60000"),
            contracts=1,
        )
        with self.assertRaisesRegex(
            TradingDisabledError, "^Trading disabled in Phase 2$"
        ):
            adapter.submit_limit_order(command)
        self.assertEqual(0, adapter.backend.operation_call_count("submit_limit_order"))

    def test_phase2_write_guard_is_not_a_mutable_feature_flag(self) -> None:
        with patch.object(runtime, "READ_ONLY_MODE", False), patch.object(
            runtime, "TRADING_ENABLED", True
        ):
            with self.assertRaisesRegex(
                TradingDisabledError, "^Trading disabled in Phase 2$"
            ):
                runtime.require_trading_enabled()

    def test_shutdown_policies_are_declared_but_cannot_enable_writes(self) -> None:
        for policy in ShutdownPolicy:
            plan = ShutdownPlan(policy)
            self.assertFalse(plan.execute_exchange_writes)
        with self.assertRaisesRegex(ValueError, "cannot execute exchange writes"):
            ShutdownPlan(
                ShutdownPolicy.CANCEL_BOT_ORDERS,
                execute_exchange_writes=True,
            )


if __name__ == "__main__":
    unittest.main()
