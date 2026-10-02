from __future__ import annotations

import unittest

from gridtrader.core.enums import OrderLocalState
from gridtrader.orders import InvalidOrderTransition, transition_order

from .test_state_machine import make_record


class RequiredOrderStateMachineContractTests(unittest.TestCase):
    def test_planned_must_pass_through_submitting(self) -> None:
        planned = make_record()
        with self.assertRaises(InvalidOrderTransition):
            transition_order(planned, OrderLocalState.ACTIVE)
        submitting = transition_order(planned, OrderLocalState.SUBMITTING)
        self.assertEqual(OrderLocalState.SUBMITTING, submitting.local_state)


if __name__ == "__main__":
    unittest.main()
