import unittest
from datetime import datetime, timezone
from decimal import Decimal

from gridtrader.core.enums import ExchangeOrderStatus, PositionSide, Side
from gridtrader.core.errors import TradingDisabledError
from gridtrader.exchange.fake import FakeExchangeAdapter
from gridtrader.exchange.fake_backend import FakeExchangeBackend
from gridtrader.exchange.models import (
    CancelOrder,
    ExchangeOrderSnapshot,
    InstrumentRules,
    SubmitLimitOrder,
    TradePage,
)
from gridtrader.exchange.ports import ExchangePort


UTC_NOW = datetime(2026, 10, 2, 12, 0, tzinfo=timezone.utc)


class ExchangePortContractTests(unittest.TestCase):
    def setUp(self) -> None:
        backend = FakeExchangeBackend("coinm-main")
        backend.set_instrument_rules(
            InstrumentRules(
                symbol="BTCUSD_PERP",
                pair="BTCUSD",
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
                observed_at=UTC_NOW,
            )
        )
        self.order = ExchangeOrderSnapshot(
            symbol="BTCUSD_PERP",
            client_order_id="dg1-order-1",
            exchange_order_id="1001",
            status=ExchangeOrderStatus.NEW,
            side=Side.BUY,
            position_side=PositionSide.BOTH,
            original_contracts=2,
            filled_contracts=0,
            update_time=UTC_NOW,
            price=Decimal("65000"),
        )
        backend.seed_order(self.order)
        self.adapter = FakeExchangeAdapter(backend)

    def test_fake_is_runtime_compatible_with_exchange_port(self) -> None:
        self.assertIsInstance(self.adapter, ExchangePort)

    def test_read_contract_returns_explicit_trade_page_and_account_snapshots(self) -> None:
        self.assertEqual(
            self.adapter.get_instrument_rules("BTCUSD_PERP").symbol,
            "BTCUSD_PERP",
        )
        self.assertEqual(self.adapter.get_open_orders("BTCUSD_PERP"), (self.order,))
        self.assertEqual(
            self.adapter.get_order_by_client_id(
                self.order.symbol,
                self.order.client_order_id,
            ),
            self.order,
        )
        self.assertEqual(
            self.adapter.get_order_by_exchange_id(
                self.order.symbol,
                self.order.exchange_order_id,
            ),
            self.order,
        )
        self.assertEqual(self.adapter.get_positions("BTCUSD_PERP"), ())
        self.assertEqual(self.adapter.get_margin_account_snapshot().balances, ())
        self.assertIsNotNone(self.adapter.get_position_mode().observed_at)
        page = self.adapter.get_user_trades("BTCUSD_PERP")
        self.assertIsInstance(page, TradePage)
        self.assertTrue(page.complete)
        self.assertEqual(page.items, ())
        self.assertEqual(page.pagination_watermark, "0")

    def test_every_phase2_write_port_is_explicitly_disabled(self) -> None:
        submit = SubmitLimitOrder(
            symbol=self.order.symbol,
            client_order_id="dg1-order-2",
            side=Side.SELL,
            position_side=PositionSide.BOTH,
            price=Decimal("66000"),
            contracts=1,
        )
        cancel = CancelOrder(self.order.symbol, self.order.client_order_id)
        for action in (
            lambda: self.adapter.submit_limit_order(submit),
            lambda: self.adapter.cancel_order(cancel),
        ):
            with self.assertRaisesRegex(
                TradingDisabledError,
                "^Trading disabled in Phase 2$",
            ):
                action()


if __name__ == "__main__":
    unittest.main()
