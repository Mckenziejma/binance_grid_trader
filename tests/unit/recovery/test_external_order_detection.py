from __future__ import annotations

import unittest

from gridtrader.recovery.models import OrderOwnership, ReconciliationKind
from gridtrader.recovery.reconciler import Reconciler

from .support import make_exchange_order


class ExternalOrderDetectionTests(unittest.TestCase):
    def test_non_bot_namespace_order_is_reported_but_does_not_block(self) -> None:
        external = make_exchange_order(
            client_order_id="manual-desk-order-1",
            exchange_order_id="external-1",
        )

        report = Reconciler().reconcile(
            local_orders=(),
            open_orders=(external,),
            exact_orders={},
            fills=(),
            local_positions=(),
            exchange_positions=(),
        )

        resolution = report.orders[0]
        self.assertIs(resolution.kind, ReconciliationKind.EXTERNAL)
        self.assertIs(resolution.ownership, OrderOwnership.EXTERNAL)
        self.assertFalse(resolution.blocks_ready)
        self.assertTrue(report.ready_safe)


if __name__ == "__main__":
    unittest.main()
