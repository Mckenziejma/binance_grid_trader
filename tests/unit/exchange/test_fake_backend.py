import unittest
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from gridtrader.core.clock import Clock
from gridtrader.core.enums import ExchangeOrderStatus, PositionSide, Side
from gridtrader.core.errors import DomainValidationError
from gridtrader.exchange.fake import FakeExchangeAdapter
from gridtrader.exchange.errors import ExchangeAmbiguousResultError, RequestClass
from gridtrader.exchange.fake_backend import (
    FakeExchangeBackend,
    FakeFaultKind,
)
from gridtrader.exchange.models import (
    ExchangeMarginBalance,
    ExchangeOrderSnapshot,
    InstrumentRules,
    MarginAccountSnapshot,
    PositionMode,
)


UTC_NOW = datetime(2026, 10, 2, 12, 0, tzinfo=timezone.utc)


class FixedClock(Clock):
    def __init__(self, value: datetime = UTC_NOW) -> None:
        self.value = value

    def now(self) -> datetime:
        return self.value


def make_rules(symbol: str = "BTCUSD_PERP") -> InstrumentRules:
    asset = "BTC" if symbol.startswith("BTC") else "ETH"
    return InstrumentRules(
        symbol=symbol,
        pair=f"{asset}USD",
        base_asset=asset,
        quote_asset="USD",
        margin_asset=asset,
        contract_type="PERPETUAL",
        status="TRADING",
        price_tick=Decimal("0.1"),
        contract_size=Decimal("100") if asset == "BTC" else Decimal("10"),
        contract_step=1,
        min_contracts=1,
        max_contracts=100_000,
        min_price=Decimal("0.1"),
        max_price=Decimal("1000000"),
        supported_order_types=("LIMIT", "MARKET"),
        observed_at=UTC_NOW,
        rules_hash=f"rules-{symbol}",
    )


def make_order(
    *,
    client_order_id: str = "dg1-order-1",
    exchange_order_id: str = "1001",
    symbol: str = "BTCUSD_PERP",
    contracts: int = 6,
    side: Side = Side.BUY,
    position_side: PositionSide = PositionSide.BOTH,
) -> ExchangeOrderSnapshot:
    return ExchangeOrderSnapshot(
        symbol=symbol,
        client_order_id=client_order_id,
        exchange_order_id=exchange_order_id,
        status=ExchangeOrderStatus.NEW,
        side=side,
        position_side=position_side,
        original_contracts=contracts,
        filled_contracts=0,
        update_time=UTC_NOW,
        price=Decimal("65000"),
    )


class FakeExchangeBackendTests(unittest.TestCase):
    def setUp(self) -> None:
        self.clock = FixedClock()
        self.backend = FakeExchangeBackend(
            "coinm-main",
            clock=self.clock,
            default_page_size=2,
        )
        self.backend.set_instrument_rules(make_rules())

    def test_backend_keeps_multi_symbol_rules_and_account_observations(self) -> None:
        self.backend.set_instrument_rules(make_rules("ETHUSD_PERP"))
        self.backend.set_position_mode(PositionMode.HEDGE)
        margin = MarginAccountSnapshot(
            balances=(
                ExchangeMarginBalance(
                    asset="BTC",
                    wallet_balance=Decimal("1.25"),
                    available_balance=Decimal("1.00"),
                    unrealized_pnl=Decimal("0.01"),
                    update_time=UTC_NOW,
                ),
            ),
            observed_at=UTC_NOW,
            total_wallet_balance=Decimal("1.25"),
            total_unrealized_pnl=Decimal("0.01"),
            available_balance=Decimal("1.00"),
        )
        self.backend.set_margin_snapshot(margin)

        self.assertEqual(
            self.backend.get_instrument_rules("ETHUSD_PERP").contract_size,
            Decimal("10"),
        )
        self.assertIs(self.backend.get_position_mode().mode, PositionMode.HEDGE)
        self.assertEqual(self.backend.get_margin_snapshot(), margin)

    def test_multiple_partial_fills_are_stable_and_update_exchange_truth(self) -> None:
        order = make_order()
        self.backend.seed_order(order)

        first = self.backend.record_fill(
            order.symbol,
            order.client_order_id,
            contracts=2,
            price=Decimal("65000"),
            commission=Decimal("0.00001"),
            realized_pnl=Decimal("0.0002"),
            trade_id="trade-1",
        )
        partial = self.backend.get_order(order.symbol, order.client_order_id)
        self.assertIs(partial.status, ExchangeOrderStatus.PARTIALLY_FILLED)
        self.assertEqual(partial.filled_contracts, 2)
        self.assertEqual(first.commission, Decimal("0.00001"))
        self.assertEqual(first.realized_pnl, Decimal("0.0002"))

        duplicate = self.backend.record_fill(
            order.symbol,
            order.client_order_id,
            contracts=2,
            price=Decimal("99999"),
            trade_id="trade-1",
        )
        self.assertEqual(duplicate, first)
        self.assertEqual(
            self.backend.get_order(order.symbol, order.client_order_id).filled_contracts,
            2,
        )

        self.backend.record_fill(
            order.symbol,
            order.client_order_id,
            contracts=1,
            price=Decimal("65100"),
            trade_id="trade-2",
        )
        self.backend.record_fill(
            order.symbol,
            order.client_order_id,
            contracts=3,
            price=Decimal("65200"),
            trade_id="trade-3",
        )
        filled = self.backend.get_order(order.symbol, order.client_order_id)
        self.assertIs(filled.status, ExchangeOrderStatus.FILLED)
        self.assertEqual(filled.filled_contracts, 6)
        self.assertEqual(
            self.backend.get_positions(order.symbol)[0].contracts,
            6,
        )

    def test_exchange_order_ids_are_scoped_by_symbol(self) -> None:
        btc = make_order(exchange_order_id="shared-id")
        eth = make_order(
            symbol="ETHUSD_PERP",
            client_order_id="dg1-eth-order",
            exchange_order_id="shared-id",
        )
        self.backend.seed_order(btc)
        self.backend.seed_order(eth)

        self.assertEqual(
            self.backend.get_order_by_exchange_id(
                btc.symbol,
                btc.exchange_order_id,
            ),
            btc,
        )
        self.assertEqual(
            self.backend.get_order_by_exchange_id(
                eth.symbol,
                eth.exchange_order_id,
            ),
            eth,
        )

    def test_trade_pages_have_stable_watermark_and_resumable_cursor(self) -> None:
        order = make_order(contracts=8)
        self.backend.seed_order(order)
        for number in range(3):
            self.clock.value = UTC_NOW + timedelta(seconds=number)
            self.backend.record_fill(
                order.symbol,
                order.client_order_id,
                contracts=2,
                price=Decimal("65000") + number,
                trade_id=f"trade-{number + 1}",
            )

        first = self.backend.get_user_trades(order.symbol, limit=2)
        self.assertFalse(first.complete)
        self.assertIsNotNone(first.next_cursor)
        self.assertEqual(first.pagination_watermark, "3")
        self.assertEqual([item.trade_id for item in first.items], ["trade-1", "trade-2"])

        # A fill arriving between pages belongs to the next snapshot, not the
        # in-progress replay bounded by the first page's watermark.
        self.backend.record_fill(
            order.symbol,
            order.client_order_id,
            contracts=2,
            price=Decimal("65003"),
            trade_id="trade-4",
        )

        second = self.backend.get_user_trades(
            order.symbol,
            cursor=first.next_cursor,
            limit=2,
        )
        self.assertTrue(second.complete)
        self.assertIsNone(second.next_cursor)
        self.assertEqual(second.snapshot_time, first.snapshot_time)
        self.assertEqual(second.pagination_watermark, first.pagination_watermark)
        self.assertEqual([item.trade_id for item in second.items], ["trade-3"])

    def test_trade_cursor_is_bound_to_account_and_symbol(self) -> None:
        order = make_order(contracts=4)
        self.backend.seed_order(order)
        for number in range(2):
            self.backend.record_fill(
                order.symbol,
                order.client_order_id,
                contracts=1,
                price=Decimal("65000"),
                trade_id=f"bound-{number}",
            )
        first = self.backend.get_user_trades(order.symbol, limit=1)
        self.assertIsNotNone(first.next_cursor)

        with self.assertRaises(DomainValidationError):
            self.backend.get_user_trades(
                "ETHUSD_PERP",
                cursor=first.next_cursor,
            )
        another_account = FakeExchangeBackend("another-account")
        with self.assertRaises(DomainValidationError):
            another_account.get_user_trades(
                order.symbol,
                cursor=first.next_cursor,
            )

    def test_trade_cursor_cannot_be_combined_with_time_filters(self) -> None:
        order = make_order(contracts=4)
        self.backend.seed_order(order)
        for number in range(2):
            self.backend.record_fill(
                order.symbol,
                order.client_order_id,
                contracts=1,
                price=Decimal("65000"),
                trade_id=f"filter-{number}",
            )
        first = self.backend.get_user_trades(order.symbol, limit=1)

        with self.assertRaises(DomainValidationError):
            self.backend.get_user_trades(
                order.symbol,
                cursor=first.next_cursor,
                from_time=UTC_NOW,
            )

    def test_trade_cursor_preserves_time_window_across_pages(self) -> None:
        order = make_order(contracts=8)
        self.backend.seed_order(order)
        cases = (
            ("before-1", UTC_NOW - timedelta(seconds=10)),
            ("before-2", UTC_NOW - timedelta(seconds=5)),
            ("inside-1", UTC_NOW),
            ("inside-2", UTC_NOW + timedelta(seconds=5)),
            ("after", UTC_NOW + timedelta(seconds=10)),
        )
        for trade_id, trade_time in cases:
            self.clock.value = trade_time
            self.backend.record_fill(
                order.symbol,
                order.client_order_id,
                contracts=1,
                price=Decimal("65000"),
                trade_id=trade_id,
            )

        first = self.backend.get_user_trades(
            order.symbol,
            from_time=UTC_NOW,
            end_time=UTC_NOW + timedelta(seconds=5),
            limit=1,
        )
        second = self.backend.get_user_trades(
            order.symbol,
            cursor=first.next_cursor,
            limit=1,
        )

        self.assertEqual([item.trade_id for item in first.items], ["inside-1"])
        self.assertEqual([item.trade_id for item in second.items], ["inside-2"])
        self.assertTrue(second.complete)
        self.assertIsNone(second.next_cursor)

    def test_delayed_fill_does_not_shift_an_in_progress_page_snapshot(self) -> None:
        order = make_order(contracts=8)
        self.backend.seed_order(order)
        self.backend.record_fill(
            order.symbol,
            order.client_order_id,
            contracts=2,
            price=Decimal("65000"),
            trade_id="delayed-first",
            visibility_delay_reads=1,
        )
        for number in range(3):
            self.backend.record_fill(
                order.symbol,
                order.client_order_id,
                contracts=2,
                price=Decimal("65001") + number,
                trade_id=f"visible-{number + 1}",
            )
        adapter = FakeExchangeAdapter(self.backend)

        first = adapter.get_user_trades(order.symbol, limit=2)
        self.assertEqual(
            [item.trade_id for item in first.items],
            ["visible-1", "visible-2"],
        )
        second = adapter.get_user_trades(
            order.symbol,
            cursor=first.next_cursor,
            limit=2,
        )
        self.assertEqual(
            [item.trade_id for item in second.items],
            ["visible-3"],
        )
        self.assertTrue(second.complete)

    def test_stale_snapshot_freezes_previous_order_view(self) -> None:
        order = make_order()
        self.backend.seed_order(order)
        self.backend.enable_stale_snapshots()
        self.backend.set_order_status(
            order.symbol,
            order.client_order_id,
            ExchangeOrderStatus.CANCELED,
        )

        stale = self.backend.get_order(order.symbol, order.client_order_id)
        self.assertIs(stale.status, ExchangeOrderStatus.NEW)
        self.backend.disable_stale_snapshots()
        current = self.backend.get_order(order.symbol, order.client_order_id)
        self.assertIs(current.status, ExchangeOrderStatus.CANCELED)

    def test_test_control_can_model_accepted_but_lost_write_response(self) -> None:
        for index, fault_kind in enumerate(
            (
                FakeFaultKind.RESPONSE_LOST_AFTER_ACCEPT,
                FakeFaultKind.EXECUTION_UNKNOWN_503,
            ),
            start=1,
        ):
            with self.subTest(fault_kind=fault_kind):
                order = make_order(
                    client_order_id=f"dg1-ambiguous-{index}",
                    exchange_order_id=f"ambiguous-{index}",
                )
                with self.assertRaises(ExchangeAmbiguousResultError) as raised:
                    self.backend.simulate_accepted_order_fact_then_ambiguous(
                        order,
                        fault_kind=fault_kind,
                    )
                self.assertIs(raised.exception.request_class, RequestClass.WRITE)
                self.assertEqual(
                    self.backend.get_order(order.symbol, order.client_order_id),
                    order,
                )

                # Replaying the same deterministic identity is idempotent; it
                # never creates a second exchange fact.
                with self.assertRaises(ExchangeAmbiguousResultError):
                    self.backend.simulate_accepted_order_fact_then_ambiguous(
                        order,
                        fault_kind=fault_kind,
                    )
                matches = [
                    item
                    for item in self.backend.get_open_orders(order.symbol)
                    if item.client_order_id == order.client_order_id
                ]
                self.assertEqual(matches, [order])


if __name__ == "__main__":
    unittest.main()
