import re
import unittest

from gridtrader.orders.idempotency import (
    CLIENT_ORDER_ID_MAX_LENGTH,
    canonical_logical_slot_key,
    logical_slot_key,
    logical_slot_key_text,
    make_client_order_id,
)


class ClientOrderIdTests(unittest.TestCase):
    def test_same_semantic_key_is_stable(self):
        args = ("strategy-a", "generation-7", "level-002", 4, "entry")
        self.assertEqual(make_client_order_id(*args), make_client_order_id(*args))

    def test_different_semantic_keys_get_different_ids(self):
        base = ("strategy-a", "generation-7", "level-002", 4, "entry")
        variants = [
            ("strategy-b", base[1], base[2], base[3], base[4]),
            (base[0], "generation-8", base[2], base[3], base[4]),
            (base[0], base[1], "level-003", base[3], base[4]),
            (base[0], base[1], base[2], 5, base[4]),
            (base[0], base[1], base[2], base[3], "take-profit"),
        ]
        base_id = make_client_order_id(*base)
        for variant in variants:
            with self.subTest(variant=variant):
                self.assertNotEqual(base_id, make_client_order_id(*variant))

    def test_id_fits_binance_envelope(self):
        value = make_client_order_id(
            "strategy-with-a-very-long-id",
            "generation-with-a-long-name",
            "level-with-a-long-name",
            123456,
            "take-profit",
        )
        self.assertLessEqual(len(value), CLIENT_ORDER_ID_MAX_LENGTH)
        self.assertRegex(value, re.compile(r"^[A-Za-z0-9._:/-]+$"))

    def test_logical_slot_rejects_invalid_cycle_and_blank_role(self):
        with self.assertRaises(ValueError):
            logical_slot_key("s", "g", "l", -1, "entry")
        with self.assertRaises(ValueError):
            logical_slot_key("s", "g", "l", 0, "   ")

    def test_canonical_key_is_stable_versioned_text(self):
        key = logical_slot_key("strategy-a", "generation-1", "level-2", 3, "exit")
        serialized = canonical_logical_slot_key(key)
        self.assertIsInstance(serialized, str)
        self.assertTrue(serialized.startswith("v1:"))
        self.assertEqual(serialized, canonical_logical_slot_key(key))
        self.assertEqual(
            serialized,
            logical_slot_key_text("strategy-a", "generation-1", "level-2", 3, "exit"),
        )


if __name__ == "__main__":
    unittest.main()
