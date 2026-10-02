import unittest
from decimal import Decimal

from gridtrader.core.enums import (
    ExchangeOrderStatus,
    OrderLocalState,
    PositionSide,
    Side,
)
from gridtrader.orders import (
    InvalidOrderTransition,
    InvalidOrderUpdate,
    OrderIntent,
    OrderRecord,
    transition_order,
)


def make_record(quantity=5):
    intent = OrderIntent(
        strategy_id="strategy-a",
        generation_id="generation-1",
        level_id="level-1",
        cycle_no=0,
        leg_role="entry",
        symbol="BTCUSD_PERP",
        side=Side.BUY,
        position_side=PositionSide.BOTH,
        price=Decimal("60000"),
        quantity_contracts=quantity,
    )
    return OrderRecord(intent=intent)


def make_active(quantity=5):
    record = transition_order(make_record(quantity), OrderLocalState.SUBMITTING)
    return transition_order(
        record,
        OrderLocalState.ACTIVE,
        exchange_status=ExchangeOrderStatus.NEW,
        exchange_order_id="12345",
    )


class OrderStateMachineTests(unittest.TestCase):
    def test_every_exchange_terminal_status_maps_to_terminal_local_state(self):
        terminal_statuses = (
            ExchangeOrderStatus.FILLED,
            ExchangeOrderStatus.CANCELED,
            ExchangeOrderStatus.REJECTED,
            ExchangeOrderStatus.EXPIRED,
            ExchangeOrderStatus.EXPIRED_IN_MATCH,
        )
        for status in terminal_statuses:
            with self.subTest(status=status):
                active = make_active(quantity=5)
                cumulative = 5 if status is ExchangeOrderStatus.FILLED else 0
                terminal = transition_order(
                    active,
                    OrderLocalState.TERMINAL,
                    exchange_status=status,
                    cumulative_filled_contracts=cumulative,
                )
                self.assertTrue(terminal.is_terminal)

    def test_allowed_state_chain_and_partial_update_while_canceling(self):
        active = make_active()
        partial = transition_order(
            active,
            OrderLocalState.ACTIVE,
            exchange_status=ExchangeOrderStatus.PARTIALLY_FILLED,
            cumulative_filled_contracts=2,
        )
        pending = transition_order(partial, OrderLocalState.CANCEL_PENDING)
        pending = transition_order(
            pending,
            OrderLocalState.CANCEL_PENDING,
            exchange_status=ExchangeOrderStatus.PARTIALLY_FILLED,
            cumulative_filled_contracts=3,
        )
        terminal = transition_order(
            pending,
            OrderLocalState.TERMINAL,
            exchange_status=ExchangeOrderStatus.CANCELED,
            cumulative_filled_contracts=3,
        )
        self.assertEqual(terminal.local_state, OrderLocalState.TERMINAL)
        self.assertEqual(terminal.remaining_contracts, 2)

    def test_ack_unknown_can_recover_or_block_but_not_resubmit(self):
        submitting = transition_order(make_record(), OrderLocalState.SUBMITTING)
        unknown = transition_order(submitting, OrderLocalState.ACK_UNKNOWN)
        with self.assertRaises(InvalidOrderTransition):
            transition_order(unknown, OrderLocalState.SUBMITTING)

        active = transition_order(
            unknown,
            OrderLocalState.ACTIVE,
            exchange_status=ExchangeOrderStatus.NEW,
        )
        self.assertEqual(active.local_state, OrderLocalState.ACTIVE)

        submitting = transition_order(make_record(), OrderLocalState.SUBMITTING)
        unknown = transition_order(submitting, OrderLocalState.ACK_UNKNOWN)
        blocked = transition_order(unknown, OrderLocalState.BLOCKED)
        self.assertEqual(blocked.local_state, OrderLocalState.BLOCKED)
        resolved = transition_order(
            blocked,
            OrderLocalState.TERMINAL,
            exchange_status=ExchangeOrderStatus.REJECTED,
        )
        self.assertTrue(resolved.is_terminal)

    def test_submitting_accepts_immediate_terminal_ack(self):
        submitting = transition_order(make_record(), OrderLocalState.SUBMITTING)
        rejected = transition_order(
            submitting,
            OrderLocalState.TERMINAL,
            exchange_status=ExchangeOrderStatus.REJECTED,
        )
        self.assertTrue(rejected.is_terminal)

    def test_disallowed_transition_is_rejected(self):
        with self.assertRaises(InvalidOrderTransition):
            transition_order(make_record(), OrderLocalState.ACTIVE)

    def test_cumulative_fill_is_monotonic_and_bounded(self):
        active = make_active(quantity=5)
        with self.assertRaises(InvalidOrderUpdate):
            transition_order(
                active,
                OrderLocalState.ACTIVE,
                exchange_status=ExchangeOrderStatus.PARTIALLY_FILLED,
                cumulative_filled_contracts=-1,
            )
        partial = transition_order(
            active,
            OrderLocalState.ACTIVE,
            exchange_status=ExchangeOrderStatus.PARTIALLY_FILLED,
            cumulative_filled_contracts=2,
        )
        with self.assertRaises(InvalidOrderUpdate):
            transition_order(
                partial,
                OrderLocalState.ACTIVE,
                exchange_status=ExchangeOrderStatus.PARTIALLY_FILLED,
                cumulative_filled_contracts=1,
            )
        with self.assertRaises(InvalidOrderUpdate):
            transition_order(
                partial,
                OrderLocalState.TERMINAL,
                exchange_status=ExchangeOrderStatus.FILLED,
                cumulative_filled_contracts=6,
            )
        with self.assertRaises(InvalidOrderUpdate):
            transition_order(
                active,
                OrderLocalState.ACTIVE,
                exchange_status=ExchangeOrderStatus.PARTIALLY_FILLED,
                cumulative_filled_contracts=5,
            )

    def test_partial_exchange_status_is_never_terminal(self):
        active = make_active(quantity=5)
        with self.assertRaises(InvalidOrderUpdate):
            transition_order(
                active,
                OrderLocalState.TERMINAL,
                exchange_status=ExchangeOrderStatus.PARTIALLY_FILLED,
                cumulative_filled_contracts=2,
            )

    def test_filled_requires_exact_order_quantity(self):
        active = make_active(quantity=5)
        with self.assertRaises(InvalidOrderUpdate):
            transition_order(
                active,
                OrderLocalState.TERMINAL,
                exchange_status=ExchangeOrderStatus.FILLED,
                cumulative_filled_contracts=4,
            )
        filled = transition_order(
            active,
            OrderLocalState.TERMINAL,
            exchange_status=ExchangeOrderStatus.FILLED,
            cumulative_filled_contracts=5,
        )
        self.assertEqual(filled.remaining_contracts, 0)

    def test_terminal_never_regresses_but_same_fact_can_be_completed(self):
        active = make_active()
        terminal = transition_order(
            active,
            OrderLocalState.TERMINAL,
            exchange_status=ExchangeOrderStatus.CANCELED,
            cumulative_filled_contracts=1,
        )
        completed = transition_order(
            terminal,
            OrderLocalState.TERMINAL,
            exchange_status=ExchangeOrderStatus.CANCELED,
            cumulative_filled_contracts=2,
        )
        self.assertEqual(completed.cumulative_filled_contracts, 2)
        with self.assertRaises(InvalidOrderTransition):
            transition_order(completed, OrderLocalState.ACTIVE)
        with self.assertRaises(InvalidOrderUpdate):
            transition_order(
                completed,
                OrderLocalState.TERMINAL,
                exchange_status=ExchangeOrderStatus.FILLED,
                cumulative_filled_contracts=5,
            )

    def test_exchange_order_id_is_immutable_once_known(self):
        active = make_active()
        with self.assertRaises(InvalidOrderUpdate):
            transition_order(
                active,
                OrderLocalState.ACTIVE,
                exchange_status=ExchangeOrderStatus.NEW,
                exchange_order_id="different-order",
            )


if __name__ == "__main__":
    unittest.main()
