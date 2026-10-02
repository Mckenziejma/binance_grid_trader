from __future__ import annotations

import unittest

from gridtrader.orders import CLIENT_ORDER_ID_MAX_LENGTH, make_client_order_id


class ClientOrderIdContractTests(unittest.TestCase):
    def test_semantic_identity_is_stable_and_bounded(self) -> None:
        semantic_key = ("strategy-a", "generation-1", "level-10", 7, "grid_buy")
        first = make_client_order_id(*semantic_key)
        second = make_client_order_id(*semantic_key)
        self.assertEqual(first, second)
        self.assertLessEqual(len(first), CLIENT_ORDER_ID_MAX_LENGTH)


if __name__ == "__main__":
    unittest.main()
