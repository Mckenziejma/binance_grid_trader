from __future__ import annotations

import unittest

from gridtrader.core.enums import ExchangeOrderStatus, OrderLocalState
from gridtrader.orders import InvalidOrderTransition, transition_order

from .test_state_machine import make_active


class TerminalStateMonotonicityContractTests(unittest.TestCase):
    def test_terminal_order_never_returns_to_active(self) -> None:
        terminal = transition_order(
            make_active(),
            OrderLocalState.TERMINAL,
            exchange_status=ExchangeOrderStatus.CANCELED,
        )
        with self.assertRaises(InvalidOrderTransition):
            transition_order(terminal, OrderLocalState.ACTIVE)


if __name__ == "__main__":
    unittest.main()
