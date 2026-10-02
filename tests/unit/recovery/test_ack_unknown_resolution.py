from __future__ import annotations

import unittest

from gridtrader.core.enums import ExchangeOrderStatus, ReadinessState
from gridtrader.recovery.models import ReconciliationKind

from .support import make_exchange_order, make_harness, make_position


class AckUnknownResolutionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.harness = make_harness()

    def tearDown(self) -> None:
        self.harness.close()

    def test_exact_terminal_order_resolves_ack_unknown(self) -> None:
        local = self.harness.add_local_order(
            local_state="ack_unknown",
            exchange_status="unknown",
        )
        self.harness.backend.seed_order(
            make_exchange_order(
                client_order_id=local["client_order_id"],
            )
        )
        self.harness.backend.record_fill(
            local["symbol"],
            local["client_order_id"],
            contracts=2,
            price=make_exchange_order().price,
            trade_id="trade-ack-resolved",
        )
        self.harness.add_local_position(2)
        exchange_position = make_position(2)

        result = self.harness.manager.recover(self.harness.request())

        self.assertTrue(result.complete)
        self.assertIs(result.state, ReadinessState.READY)
        self.assertIs(result.report.orders[0].kind, ReconciliationKind.TERMINAL)
        self.assertEqual(
            self.harness.backend.operation_call_count("get_order_by_client_id"),
            1,
        )
        self.assertEqual(exchange_position.contracts, 2)
        with self.harness.ledger.unit_of_work(immediate=False) as uow:
            stored = uow.orders.require(local["local_order_id"])
        self.assertEqual(stored["local_state"], "terminal")
        self.assertEqual(stored["exchange_status"], "filled")
        self.assertEqual(stored["cumulative_filled_contracts"], 2)

    def test_missing_ack_unknown_remains_ambiguous_and_blocks_ready(self) -> None:
        self.harness.add_local_order(
            local_state="ack_unknown",
            exchange_status="unknown",
        )

        result = self.harness.manager.recover(self.harness.request())

        self.assertFalse(result.complete)
        self.assertIs(result.state, ReadinessState.DEGRADED)
        self.assertIs(result.report.orders[0].kind, ReconciliationKind.AMBIGUOUS)
        self.assertIn("ACK_UNKNOWN_OR_ORDER_HISTORY_UNRESOLVED", result.blockers)

if __name__ == "__main__":
    unittest.main()
