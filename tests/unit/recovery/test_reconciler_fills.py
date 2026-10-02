from __future__ import annotations

import unittest
from dataclasses import replace
from decimal import Decimal

from gridtrader.core.enums import ExchangeOrderStatus
from gridtrader.recovery.models import ReconciliationKind
from gridtrader.recovery.reconciler import Reconciler

from .support import make_exchange_order, make_fill, make_local_order


class ReconcilerFillTests(unittest.TestCase):
    def setUp(self) -> None:
        self.reconciler = Reconciler()

    def test_identical_fill_replay_is_deduplicated(self) -> None:
        fill = make_fill("trade-1")

        report = self.reconciler.reconcile(
            local_orders=(),
            open_orders=(),
            exact_orders={},
            fills=(fill, fill),
            local_positions=(),
            exchange_positions=(),
        )

        self.assertEqual(report.fills, (fill,))
        self.assertEqual(report.unmatched_fills, (fill,))

    def test_complete_fill_history_proves_a_missing_order_filled(self) -> None:
        local = make_local_order(quantity_contracts=2)
        fills = (
            make_fill("trade-1", contracts=1, seconds=1),
            make_fill("trade-2", contracts=1, seconds=2),
        )

        report = self.reconciler.reconcile(
            local_orders=(local,),
            open_orders=(),
            exact_orders={local["client_order_id"]: None},
            fills=fills,
            local_positions=(),
            exchange_positions=(),
        )

        resolution = report.orders[0]
        self.assertIs(resolution.kind, ReconciliationKind.TERMINAL)
        self.assertIs(resolution.effective_status, ExchangeOrderStatus.FILLED)
        self.assertEqual(resolution.cumulative_filled_contracts, 2)
        self.assertFalse(report.unmatched_fills)

    def test_conflicting_duplicate_trade_id_is_rejected(self) -> None:
        fill = make_fill("trade-1")
        conflicting = replace(fill, price=Decimal("50001"))

        with self.assertRaisesRegex(ValueError, "conflicting fill data"):
            self.reconciler.reconcile(
                local_orders=(),
                open_orders=(),
                exact_orders={},
                fills=(fill, conflicting),
                local_positions=(),
                exchange_positions=(),
            )

    def test_fill_without_client_id_or_exact_order_blocks_ready(self) -> None:
        fill = replace(make_fill("trade-no-client"), client_order_id=None)

        report = self.reconciler.reconcile(
            local_orders=(),
            open_orders=(),
            exact_orders={},
            fills=(fill,),
            local_positions=(),
            exchange_positions=(),
        )

        self.assertEqual(report.unmatched_fills, (fill,))
        self.assertTrue(
            any(blocker.startswith("AMBIGUOUS_FILL:") for blocker in report.blockers)
        )
        self.assertFalse(report.ready_safe)

    def test_trade_evidence_above_order_quantity_is_ambiguous(self) -> None:
        local = make_local_order(quantity_contracts=2)
        fills = tuple(
            make_fill(f"trade-overfill-{index}", contracts=1, seconds=index)
            for index in range(3)
        )

        report = self.reconciler.reconcile(
            local_orders=(local,),
            open_orders=(),
            exact_orders={local["client_order_id"]: None},
            fills=fills,
            local_positions=(),
            exchange_positions=(),
        )

        self.assertIs(report.orders[0].kind, ReconciliationKind.AMBIGUOUS)
        self.assertIn("exceeds", report.orders[0].reason)

    def test_exchange_cumulative_fill_without_trade_history_is_ambiguous(self) -> None:
        local = make_local_order(quantity_contracts=2)
        exchange = make_exchange_order(
            client_order_id=local["client_order_id"],
            status=ExchangeOrderStatus.PARTIALLY_FILLED,
            original_contracts=2,
            filled_contracts=1,
        )

        report = self.reconciler.reconcile(
            local_orders=(local,),
            open_orders=(exchange,),
            exact_orders={},
            fills=(),
            local_positions=(),
            exchange_positions=(),
        )

        self.assertIs(report.orders[0].kind, ReconciliationKind.AMBIGUOUS)
        self.assertIn("missing", report.orders[0].reason)
        self.assertFalse(report.ready_safe)

    def test_local_filled_status_without_linked_trade_history_is_ambiguous(self) -> None:
        local = make_local_order(
            local_state="terminal",
            exchange_status="filled",
            cumulative_filled_contracts=2,
            quantity_contracts=2,
        )

        report = self.reconciler.reconcile(
            local_orders=(local,),
            open_orders=(),
            exact_orders={local["client_order_id"]: None},
            fills=(),
            local_positions=(),
            exchange_positions=(),
        )

        self.assertIs(report.orders[0].kind, ReconciliationKind.AMBIGUOUS)
        self.assertIn("locally persisted", report.orders[0].reason)
        self.assertFalse(report.ready_safe)

    def test_local_filled_status_is_proven_by_complete_linked_trades(self) -> None:
        local = make_local_order(
            local_state="terminal",
            exchange_status="filled",
            cumulative_filled_contracts=2,
            quantity_contracts=2,
        )
        fills = (
            make_fill("trade-local-filled-1", contracts=1, seconds=1),
            make_fill("trade-local-filled-2", contracts=1, seconds=2),
        )

        report = self.reconciler.reconcile(
            local_orders=(local,),
            open_orders=(),
            exact_orders={local["client_order_id"]: None},
            fills=fills,
            local_positions=(),
            exchange_positions=(),
        )

        self.assertIs(report.orders[0].kind, ReconciliationKind.TERMINAL)
        self.assertTrue(report.ready_safe)

    def test_local_partial_canceled_status_without_trade_history_is_ambiguous(self) -> None:
        local = make_local_order(
            local_state="terminal",
            exchange_status="canceled",
            cumulative_filled_contracts=1,
            quantity_contracts=2,
        )

        report = self.reconciler.reconcile(
            local_orders=(local,),
            open_orders=(),
            exact_orders={local["client_order_id"]: None},
            fills=(),
            local_positions=(),
            exchange_positions=(),
        )

        self.assertIs(report.orders[0].kind, ReconciliationKind.AMBIGUOUS)
        self.assertFalse(report.ready_safe)

    def test_rejected_terminal_state_with_real_fill_is_ambiguous(self) -> None:
        for quantity in (1, 2):
            with self.subTest(quantity=quantity):
                local = make_local_order(
                    local_state="terminal",
                    exchange_status="rejected",
                    cumulative_filled_contracts=0,
                    quantity_contracts=quantity,
                )

                report = self.reconciler.reconcile(
                    local_orders=(local,),
                    open_orders=(),
                    exact_orders={local["client_order_id"]: None},
                    fills=(
                        make_fill(
                            f"trade-impossible-rejected-{quantity}",
                            contracts=1,
                        ),
                    ),
                    local_positions=(),
                    exchange_positions=(),
                )

                self.assertIs(
                    report.orders[0].kind,
                    ReconciliationKind.AMBIGUOUS,
                )
                self.assertIn("REJECTED", report.orders[0].reason)
                self.assertFalse(report.ready_safe)

    def test_matching_client_id_with_conflicting_exchange_order_id_is_ambiguous(self) -> None:
        local = make_local_order(exchange_order_id="90001")
        conflicting_fill = make_fill(
            "trade-conflicting-order-id",
            client_order_id=local["client_order_id"],
            exchange_order_id="99999",
        )

        report = self.reconciler.reconcile(
            local_orders=(local,),
            open_orders=(),
            exact_orders={local["client_order_id"]: None},
            fills=(conflicting_fill,),
            local_positions=(),
            exchange_positions=(),
        )

        self.assertIs(report.orders[0].kind, ReconciliationKind.AMBIGUOUS)
        self.assertIn("exchange_order_id", report.orders[0].reason)
        self.assertEqual(report.unmatched_fills, (conflicting_fill,))
        self.assertFalse(report.ready_safe)


if __name__ == "__main__":
    unittest.main()
