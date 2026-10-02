import unittest
from decimal import Decimal

from gridtrader.core.enums import (
    ExchangeOrderStatus,
    GridGenerationStatus,
    GridLevelState,
    OrderLocalState,
    PositionSide,
    Side,
    SpacingMode,
    StrategyMode,
)
from gridtrader.grid import GridGeneration, GridLevel
from gridtrader.orders import FillRecord, OrderIntent, OrderRecord


def make_intent(quantity=3):
    return OrderIntent(
        strategy_id="strategy-a",
        generation_id="generation-1",
        level_id="level-10",
        cycle_no=0,
        leg_role="entry",
        symbol="BTCUSD_PERP",
        side=Side.BUY,
        position_side=PositionSide.BOTH,
        price=Decimal("60000.1"),
        quantity_contracts=quantity,
    )


class OrderModelTests(unittest.TestCase):
    def test_record_derives_client_id_from_intent(self):
        intent = make_intent()
        record = OrderRecord(intent=intent)
        self.assertEqual(record.client_order_id, intent.client_order_id)
        self.assertTrue(intent.logical_slot_key.startswith("v1:"))
        self.assertEqual(record.remaining_contracts, 3)

    def test_order_values_require_decimal_price_and_integer_contracts(self):
        values = dict(
            strategy_id="s",
            generation_id="g",
            level_id="l",
            cycle_no=0,
            leg_role="entry",
            symbol="ETHUSD_PERP",
            side=Side.BUY,
            position_side=PositionSide.BOTH,
            price=Decimal("1000"),
            quantity_contracts=1,
        )
        with self.assertRaises(TypeError):
            OrderIntent(**dict(values, price=1000.0))
        with self.assertRaises(TypeError):
            OrderIntent(**dict(values, quantity_contracts=Decimal("1")))

    def test_order_record_rejects_state_status_combinations_that_bypass_machine(self):
        intent = make_intent()
        with self.assertRaises(ValueError):
            OrderRecord(
                intent=intent,
                local_state=OrderLocalState.ACTIVE,
                exchange_status=ExchangeOrderStatus.UNKNOWN,
            )
        with self.assertRaises(ValueError):
            OrderRecord(
                intent=intent,
                local_state=OrderLocalState.PLANNED,
                exchange_status=ExchangeOrderStatus.PARTIALLY_FILLED,
                cumulative_filled_contracts=1,
            )

    def test_fill_requires_nonempty_trade_id_and_integer_contracts(self):
        intent = make_intent()
        with self.assertRaises(ValueError):
            FillRecord(
                trade_id="",
                client_order_id=intent.client_order_id,
                symbol=intent.symbol,
                side=intent.side,
                position_side=intent.position_side,
                price=intent.price,
                fill_contracts=1,
                cumulative_filled_contracts=1,
            )
        with self.assertRaises(ValueError):
            FillRecord(
                trade_id="trade-1",
                client_order_id=intent.client_order_id,
                symbol=intent.symbol,
                side=intent.side,
                position_side=intent.position_side,
                price=intent.price,
                fill_contracts=2,
                cumulative_filled_contracts=1,
            )

    def test_grid_models_validate_required_fields(self):
        generation = GridGeneration(
            generation_id="generation-1",
            strategy_id="strategy-a",
            generation_no=1,
            lower_price=Decimal("50000"),
            upper_price=Decimal("70000"),
            logical_level_count=201,
            strategy_mode=StrategyMode.NEUTRAL,
            spacing_mode=SpacingMode.GEOMETRIC,
            order_contracts=2,
            max_active_orders=20,
            status=GridGenerationStatus.DRAFT,
            geometric_ratio=Decimal("1.001"),
        )
        level = GridLevel(
            level_id="level-1",
            generation_id=generation.generation_id,
            level_index=0,
            price=Decimal("50000"),
            planned_contracts=2,
            cycle_no=0,
            state=GridLevelState.DORMANT,
        )
        self.assertEqual(generation.logical_level_count, 201)
        self.assertEqual(level.planned_contracts, 2)

    def test_grid_models_reject_float_prices_and_nonpositive_contracts(self):
        with self.assertRaises(TypeError):
            GridGeneration(
                generation_id="generation-1",
                strategy_id="strategy-a",
                generation_no=1,
                lower_price=50000.0,
                upper_price=Decimal("70000"),
                logical_level_count=201,
                strategy_mode=StrategyMode.NEUTRAL,
                spacing_mode=SpacingMode.ARITHMETIC,
                order_contracts=2,
                max_active_orders=20,
                status=GridGenerationStatus.DRAFT,
                arithmetic_step=Decimal("100"),
            )
        with self.assertRaises(ValueError):
            GridLevel(
                level_id="level-1",
                generation_id="generation-1",
                level_index=0,
                price=Decimal("50000"),
                planned_contracts=0,
                cycle_no=0,
                state=GridLevelState.DORMANT,
            )


if __name__ == "__main__":
    unittest.main()
