from __future__ import annotations

import unittest

from gridtrader.recovery.models import OrderOwnership, ReconciliationKind
from gridtrader.recovery.reconciler import Reconciler

from .support import make_exchange_order


class UnclaimedBotOrderTests(unittest.TestCase):
    def test_bot_shaped_order_without_a_known_slot_blocks_ready(self) -> None:
        orphan = make_exchange_order(
            client_order_id="dg1_orphaned_order",
            exchange_order_id="orphan-1",
        )

        report = Reconciler().reconcile(
            local_orders=(),
            open_orders=(orphan,),
            exact_orders={},
            fills=(),
            local_positions=(),
            exchange_positions=(),
        )

        resolution = report.orders[0]
        self.assertIs(resolution.kind, ReconciliationKind.UNCLAIMED)
        self.assertIs(resolution.ownership, OrderOwnership.UNCLAIMED)
        self.assertTrue(resolution.blocks_ready)
        self.assertFalse(report.ready_safe)

    def test_transient_deterministic_claim_is_not_ownership_proof(self) -> None:
        client_id = "dg1_claimable_order"
        claimed = make_exchange_order(
            client_order_id=client_id,
            exchange_order_id="claimable-1",
        )
        claim = {"local_order_id": "claimed-local-order"}

        report = Reconciler().reconcile(
            local_orders=(),
            open_orders=(claimed,),
            exact_orders={},
            fills=(),
            local_positions=(),
            exchange_positions=(),
            claimable_orders={client_id: claim},
        )

        resolution = report.orders[0]
        self.assertIs(resolution.kind, ReconciliationKind.UNCLAIMED)
        self.assertIs(resolution.ownership, OrderOwnership.UNCLAIMED)
        self.assertIsNone(resolution.claim)
        self.assertTrue(resolution.blocks_ready)
        self.assertFalse(report.ready_safe)


if __name__ == "__main__":
    unittest.main()
