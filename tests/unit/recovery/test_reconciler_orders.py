from __future__ import annotations

import unittest

from gridtrader.core.enums import ExchangeOrderStatus
from gridtrader.recovery.models import ReconciliationKind
from gridtrader.recovery.reconciler import Reconciler

from .support import make_exchange_order, make_fill, make_local_order


class ReconcilerOrderTests(unittest.TestCase):
    def setUp(self) -> None:
        self.reconciler = Reconciler()
        self.local = make_local_order()

    def reconcile(self, *, open_orders=(), exact_orders=None, fills=()):
        return self.reconciler.reconcile(
            local_orders=(self.local,),
            open_orders=open_orders,
            exact_orders=exact_orders or {},
            fills=fills,
            local_positions=(),
            exchange_positions=(),
        )

    def test_open_exchange_order_is_authoritative_and_matched(self) -> None:
        exchange = make_exchange_order()

        report = self.reconcile(open_orders=(exchange,))

        resolution = report.orders[0]
        self.assertIs(resolution.kind, ReconciliationKind.MATCHED)
        self.assertIs(resolution.effective_status, ExchangeOrderStatus.NEW)
        self.assertFalse(resolution.blocks_ready)
        self.assertTrue(report.ready_safe)

    def test_terminal_exact_lookup_is_not_treated_as_missing(self) -> None:
        exchange = make_exchange_order(
            status=ExchangeOrderStatus.CANCELED,
        )

        report = self.reconcile(
            exact_orders={self.local["client_order_id"]: exchange},
        )

        resolution = report.orders[0]
        self.assertIs(resolution.kind, ReconciliationKind.TERMINAL)
        self.assertIs(resolution.effective_status, ExchangeOrderStatus.CANCELED)
        self.assertTrue(report.ready_safe)

    def test_active_exact_lookup_absent_from_open_snapshot_is_ambiguous(self) -> None:
        exchange = make_exchange_order(status=ExchangeOrderStatus.NEW)

        report = self.reconcile(
            exact_orders={self.local["client_order_id"]: exchange},
        )

        resolution = report.orders[0]
        self.assertIs(resolution.kind, ReconciliationKind.AMBIGUOUS)
        self.assertIn("absent from", resolution.reason)
        self.assertFalse(report.ready_safe)

    def test_partial_exact_lookup_absent_from_open_snapshot_is_ambiguous(self) -> None:
        exchange = make_exchange_order(
            status=ExchangeOrderStatus.PARTIALLY_FILLED,
            filled_contracts=1,
        )

        report = self.reconcile(
            exact_orders={self.local["client_order_id"]: exchange},
            fills=(make_fill("trade-partial-exact"),),
        )

        resolution = report.orders[0]
        self.assertIs(resolution.kind, ReconciliationKind.AMBIGUOUS)
        self.assertFalse(report.ready_safe)

    def test_exact_not_found_remains_a_blocking_local_only_order(self) -> None:
        report = self.reconcile(
            exact_orders={self.local["client_order_id"]: None},
        )

        resolution = report.orders[0]
        self.assertIs(resolution.kind, ReconciliationKind.LOCAL_ONLY)
        self.assertTrue(resolution.blocks_ready)
        self.assertFalse(report.ready_safe)


if __name__ == "__main__":
    unittest.main()
