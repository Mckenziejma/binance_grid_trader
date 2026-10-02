import unittest
from datetime import datetime, timezone
from decimal import Decimal

from gridtrader.core.enums import ExchangeOrderStatus, PositionSide, Side
from gridtrader.core.errors import DomainValidationError
from gridtrader.exchange.models import (
    ExchangeFill,
    ExchangeOrderSnapshot,
    ExchangePosition,
    InstrumentRules,
    SubmitLimitOrder,
)
from gridtrader.exchange.ports import ExchangePort


UTC_NOW = datetime(2026, 10, 2, tzinfo=timezone.utc)


def make_rules() -> InstrumentRules:
    return InstrumentRules(
        symbol="BTCUSD_PERP",
        base_asset="BTC",
        quote_asset="USD",
        margin_asset="BTC",
        contract_type="PERPETUAL",
        status="TRADING",
        price_tick=Decimal("0.1"),
        contract_size=Decimal("100"),
        contract_step=1,
        min_contracts=1,
        max_contracts=100_000,
        min_price=Decimal("0.1"),
        max_price=Decimal("1000000"),
    )


class InstrumentRulesTests(unittest.TestCase):
    def test_rules_validate_decimal_price_and_integer_contracts(self) -> None:
        rules = make_rules()
        rules.validate_order(Decimal("65000.1"), 200)

        with self.assertRaises(DomainValidationError):
            rules.validate_order(Decimal("65000.15"), 200)
        with self.assertRaises(DomainValidationError):
            rules.validate_order(Decimal("65000.1"), 200.5)  # type: ignore[arg-type]
        with self.assertRaises(DomainValidationError):
            rules.validate_order(65000.1, 200)  # type: ignore[arg-type]

    def test_contract_size_must_be_decimal(self) -> None:
        with self.assertRaises(DomainValidationError):
            InstrumentRules(
                symbol="BTCUSD_PERP",
                base_asset="BTC",
                quote_asset="USD",
                margin_asset="BTC",
                contract_type="PERPETUAL",
                status="TRADING",
                price_tick=Decimal("0.1"),
                contract_size=100,  # type: ignore[arg-type]
                contract_step=1,
                min_contracts=1,
                max_contracts=None,
            )


class ExchangeOrderSnapshotTests(unittest.TestCase):
    def test_partial_and_terminal_snapshots(self) -> None:
        partial = ExchangeOrderSnapshot(
            symbol="BTCUSD_PERP",
            client_order_id="grid-a-1",
            exchange_order_id="123",
            status=ExchangeOrderStatus.PARTIALLY_FILLED,
            side=Side.BUY,
            position_side=PositionSide.BOTH,
            original_contracts=10,
            filled_contracts=4,
            update_time=UTC_NOW,
            price=Decimal("65000.1"),
            average_fill_price=Decimal("65000"),
        )
        self.assertEqual(partial.remaining_contracts, 6)
        self.assertFalse(partial.is_terminal)

        filled = ExchangeOrderSnapshot(
            symbol="BTCUSD_PERP",
            client_order_id="grid-a-1",
            exchange_order_id="123",
            status=ExchangeOrderStatus.FILLED,
            side=Side.BUY,
            position_side=PositionSide.BOTH,
            original_contracts=10,
            filled_contracts=10,
            update_time=UTC_NOW,
        )
        self.assertTrue(filled.is_terminal)

    def test_inconsistent_partial_and_filled_counts_are_rejected(self) -> None:
        common = {
            "symbol": "BTCUSD_PERP",
            "client_order_id": "grid-a-1",
            "exchange_order_id": "123",
            "side": Side.BUY,
            "position_side": PositionSide.BOTH,
            "original_contracts": 10,
            "update_time": UTC_NOW,
        }
        with self.assertRaises(DomainValidationError):
            ExchangeOrderSnapshot(
                **common,
                status=ExchangeOrderStatus.PARTIALLY_FILLED,
                filled_contracts=0,
            )
        with self.assertRaises(DomainValidationError):
            ExchangeOrderSnapshot(
                **common,
                status=ExchangeOrderStatus.FILLED,
                filled_contracts=9,
            )
        with self.assertRaises(DomainValidationError):
            ExchangeOrderSnapshot(
                **common,
                status=ExchangeOrderStatus.NEW,
                filled_contracts=1,
            )

    def test_enum_and_bool_fields_are_strictly_typed(self) -> None:
        base = {
            "symbol": "BTCUSD_PERP",
            "client_order_id": "grid-a-1",
            "exchange_order_id": "123",
            "status": ExchangeOrderStatus.NEW,
            "side": Side.BUY,
            "position_side": PositionSide.BOTH,
            "original_contracts": 10,
            "filled_contracts": 0,
            "update_time": UTC_NOW,
        }
        with self.assertRaises(DomainValidationError):
            ExchangeOrderSnapshot(**{**base, "status": "new"})  # type: ignore[arg-type]
        with self.assertRaises(DomainValidationError):
            ExchangeOrderSnapshot(**{**base, "side": "buy"})  # type: ignore[arg-type]
        with self.assertRaises(DomainValidationError):
            ExchangeOrderSnapshot(
                **{**base, "position_side": "both"}  # type: ignore[arg-type]
            )
        with self.assertRaises(DomainValidationError):
            ExchangeOrderSnapshot(
                **{**base, "reduce_only": 1}  # type: ignore[arg-type]
            )


class ExchangeFillAndPositionTests(unittest.TestCase):
    def test_fill_deduplication_key_uses_account_symbol_and_real_trade_id(self) -> None:
        fill = ExchangeFill(
            account_id="coin-m-main",
            symbol="BTCUSD_PERP",
            trade_id="98765",
            exchange_order_id="123",
            side=Side.BUY,
            position_side=PositionSide.BOTH,
            price=Decimal("65000"),
            contracts=2,
            realized_pnl=Decimal("0"),
            commission=Decimal("0.000001"),
            commission_asset="BTC",
            trade_time=UTC_NOW,
        )
        self.assertEqual(
            fill.deduplication_key,
            ("coin-m-main", "BTCUSD_PERP", "98765"),
        )

        with self.assertRaises(DomainValidationError):
            ExchangeFill(
                account_id="coin-m-main",
                symbol="BTCUSD_PERP",
                trade_id="",
                exchange_order_id="123",
                side=Side.BUY,
                position_side=PositionSide.BOTH,
                price=Decimal("65000"),
                contracts=2,
                realized_pnl=Decimal("0"),
                commission=Decimal("0"),
                commission_asset="BTC",
                trade_time=UTC_NOW,
            )

    def test_fill_position_and_submit_command_reject_raw_enum_or_bool_values(self) -> None:
        with self.assertRaises(DomainValidationError):
            ExchangeFill(
                account_id="coin-m-main",
                symbol="BTCUSD_PERP",
                trade_id="98765",
                exchange_order_id="123",
                side="buy",  # type: ignore[arg-type]
                position_side=PositionSide.BOTH,
                price=Decimal("65000"),
                contracts=2,
                realized_pnl=Decimal("0"),
                commission=Decimal("0"),
                commission_asset="BTC",
                trade_time=UTC_NOW,
            )

        with self.assertRaises(DomainValidationError):
            ExchangeFill(
                account_id="coin-m-main",
                symbol="BTCUSD_PERP",
                trade_id="98765",
                exchange_order_id="123",
                side=Side.BUY,
                position_side="both",  # type: ignore[arg-type]
                price=Decimal("65000"),
                contracts=2,
                realized_pnl=Decimal("0"),
                commission=Decimal("0"),
                commission_asset="BTC",
                trade_time=UTC_NOW,
            )

        with self.assertRaises(DomainValidationError):
            ExchangePosition(
                symbol="BTCUSD_PERP",
                position_side=PositionSide.BOTH,
                contracts=2,
                entry_price=Decimal("65000"),
                mark_price=Decimal("65001"),
                unrealized_pnl=Decimal("0.1"),
                leverage=5,
                margin_asset="BTC",
                isolated=1,  # type: ignore[arg-type]
                update_time=UTC_NOW,
            )

        with self.assertRaises(DomainValidationError):
            ExchangePosition(
                symbol="BTCUSD_PERP",
                position_side="both",  # type: ignore[arg-type]
                contracts=2,
                entry_price=Decimal("65000"),
                mark_price=Decimal("65001"),
                unrealized_pnl=Decimal("0.1"),
                leverage=5,
                margin_asset="BTC",
                isolated=False,
                update_time=UTC_NOW,
            )

        with self.assertRaises(DomainValidationError):
            SubmitLimitOrder(
                symbol="BTCUSD_PERP",
                client_order_id="grid-a-1",
                side=Side.BUY,
                position_side="both",  # type: ignore[arg-type]
                price=Decimal("65000"),
                contracts=2,
            )

        with self.assertRaises(DomainValidationError):
            SubmitLimitOrder(
                symbol="BTCUSD_PERP",
                client_order_id="grid-a-1",
                side="buy",  # type: ignore[arg-type]
                position_side=PositionSide.BOTH,
                price=Decimal("65000"),
                contracts=2,
            )

        with self.assertRaises(DomainValidationError):
            SubmitLimitOrder(
                symbol="BTCUSD_PERP",
                client_order_id="grid-a-1",
                side=Side.BUY,
                position_side=PositionSide.BOTH,
                price=Decimal("65000"),
                contracts=2,
                reduce_only=1,  # type: ignore[arg-type]
            )


class ExchangePortContractTests(unittest.TestCase):
    def test_recovery_and_command_methods_are_reserved_by_the_protocol(self) -> None:
        required_methods = {
            "get_instrument_rules",
            "get_open_orders",
            "get_positions",
            "get_margin_balances",
            "get_user_trades",
            "get_order_by_client_id",
            "submit_limit_order",
            "cancel_order",
        }
        for method_name in required_methods:
            with self.subTest(method_name=method_name):
                self.assertTrue(hasattr(ExchangePort, method_name))


if __name__ == "__main__":
    unittest.main()
