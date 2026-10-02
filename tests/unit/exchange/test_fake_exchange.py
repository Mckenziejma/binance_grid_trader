import unittest
from datetime import datetime, timezone
from decimal import Decimal

from gridtrader.core.enums import ExchangeOrderStatus, PositionSide, Side
from gridtrader.core.errors import TradingDisabledError
from gridtrader.exchange.errors import (
    ExchangeAuthError,
    ExchangeBannedError,
    ExchangeNotFoundError,
    ExchangePermanentRequestError,
    ExchangeRateLimitError,
    ExchangeTimeoutError,
    ExchangeUnavailableError,
)
from gridtrader.exchange.fake import FakeExchangeAdapter
from gridtrader.exchange.fake_backend import (
    FakeExchangeBackend,
    FakeFault,
    FakeFaultKind,
)
from gridtrader.exchange.models import (
    CancelOrder,
    ExchangeOrderSnapshot,
    InstrumentRules,
    SubmitLimitOrder,
)


UTC_NOW = datetime(2026, 10, 2, 12, 0, tzinfo=timezone.utc)


def make_rules() -> InstrumentRules:
    return InstrumentRules(
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
        supported_order_types=("LIMIT",),
        observed_at=UTC_NOW,
    )


def make_order() -> ExchangeOrderSnapshot:
    return ExchangeOrderSnapshot(
        symbol="BTCUSD_PERP",
        client_order_id="dg1-order-1",
        exchange_order_id="1001",
        status=ExchangeOrderStatus.NEW,
        side=Side.BUY,
        position_side=PositionSide.BOTH,
        original_contracts=4,
        filled_contracts=0,
        update_time=UTC_NOW,
        price=Decimal("65000"),
    )


class FakeExchangeAdapterTests(unittest.TestCase):
    def setUp(self) -> None:
        self.backend = FakeExchangeBackend("coinm-main", default_page_size=2)
        self.backend.set_instrument_rules(make_rules())
        self.order = make_order()
        self.backend.seed_order(self.order)
        self.adapter = FakeExchangeAdapter(self.backend)

    def test_backend_truth_survives_adapter_reconstruction(self) -> None:
        first_adapter = self.adapter
        self.assertEqual(
            first_adapter.get_order_by_client_id(
                self.order.symbol,
                self.order.client_order_id,
            ),
            self.order,
        )

        del first_adapter
        restarted_adapter = FakeExchangeAdapter(self.backend)
        self.backend.record_fill(
            self.order.symbol,
            self.order.client_order_id,
            contracts=2,
            price=Decimal("65000"),
            trade_id="trade-after-restart",
        )
        restored = restarted_adapter.get_order_by_client_id(
            self.order.symbol,
            self.order.client_order_id,
        )
        self.assertIs(restored.status, ExchangeOrderStatus.PARTIALLY_FILLED)
        self.assertEqual(restored.filled_contracts, 2)

    def test_exact_exchange_order_id_lookup_preserves_symbol_scope(self) -> None:
        self.assertEqual(
            self.adapter.get_order_by_exchange_id(
                self.order.symbol,
                self.order.exchange_order_id,
            ),
            self.order,
        )
        with self.assertRaises(ExchangeNotFoundError):
            self.adapter.get_order_by_exchange_id(
                "ETHUSD_PERP",
                self.order.exchange_order_id,
            )

    def test_delayed_order_visibility_is_deterministic(self) -> None:
        delayed = ExchangeOrderSnapshot(
            **{
                **self.order.__dict__,
                "client_order_id": "dg1-delayed",
                "exchange_order_id": "1002",
            }
        )
        self.backend.seed_order(delayed, visibility_delay_reads=1)

        with self.assertRaises(ExchangeNotFoundError):
            self.adapter.get_order_by_client_id(
                delayed.symbol,
                delayed.client_order_id,
            )
        self.assertEqual(
            self.adapter.get_order_by_client_id(
                delayed.symbol,
                delayed.client_order_id,
            ),
            delayed,
        )

    def test_duplicate_and_out_of_order_trade_delivery_can_be_injected(self) -> None:
        self.backend.record_fill(
            self.order.symbol,
            self.order.client_order_id,
            contracts=1,
            price=Decimal("65000"),
            trade_id="trade-1",
        )
        self.backend.record_fill(
            self.order.symbol,
            self.order.client_order_id,
            contracts=1,
            price=Decimal("65001"),
            trade_id="trade-2",
        )
        self.backend.queue_fault(
            "get_user_trades",
            FakeFault(FakeFaultKind.DUPLICATE_DATA),
        )
        duplicate = self.adapter.get_user_trades(self.order.symbol)
        self.assertEqual(
            [item.trade_id for item in duplicate.items],
            ["trade-1", "trade-2", "trade-1"],
        )

        self.backend.queue_fault(
            "get_user_trades",
            FakeFault(FakeFaultKind.OUT_OF_ORDER_DATA),
        )
        reversed_page = self.adapter.get_user_trades(self.order.symbol)
        self.assertEqual(
            [item.trade_id for item in reversed_page.items],
            ["trade-2", "trade-1"],
        )

    def test_read_faults_use_the_shared_exchange_error_taxonomy(self) -> None:
        cases = (
            (FakeFaultKind.TIMEOUT_BEFORE_REQUEST, ExchangeTimeoutError),
            (FakeFaultKind.PAGINATION_INTERRUPTION, ExchangeUnavailableError),
            (FakeFaultKind.SERVICE_UNAVAILABLE_503, ExchangeUnavailableError),
            (FakeFaultKind.RATE_LIMIT_429, ExchangeRateLimitError),
            (FakeFaultKind.BANNED_418, ExchangeBannedError),
            (FakeFaultKind.AUTHENTICATION_ERROR, ExchangeAuthError),
            (FakeFaultKind.PERMANENT_PARAMETER_ERROR, ExchangePermanentRequestError),
        )
        for kind, expected_error in cases:
            with self.subTest(kind=kind):
                self.backend.queue_fault(
                    "get_user_trades",
                    FakeFault(
                        kind,
                        retry_after_seconds=(
                            Decimal("2.5")
                            if kind is FakeFaultKind.RATE_LIMIT_429
                            else None
                        ),
                    ),
                )
                with self.assertRaises(expected_error):
                    self.adapter.get_user_trades(self.order.symbol)

    def test_timeout_before_request_does_not_advance_delayed_visibility(self) -> None:
        delayed = ExchangeOrderSnapshot(
            **{
                **self.order.__dict__,
                "client_order_id": "dg1-timeout-delayed",
                "exchange_order_id": "1003",
            }
        )
        self.backend.seed_order(delayed, visibility_delay_reads=1)
        self.backend.queue_fault(
            "get_order_by_client_id",
            FakeFault(FakeFaultKind.TIMEOUT_BEFORE_REQUEST),
        )

        with self.assertRaises(ExchangeTimeoutError):
            self.adapter.get_order_by_client_id(
                delayed.symbol,
                delayed.client_order_id,
            )
        self.assertEqual(
            self.backend.operation_call_count("get_order_by_client_id"),
            0,
        )
        with self.assertRaises(ExchangeNotFoundError):
            self.adapter.get_order_by_client_id(
                delayed.symbol,
                delayed.client_order_id,
            )
        self.assertEqual(
            self.adapter.get_order_by_client_id(
                delayed.symbol,
                delayed.client_order_id,
            ),
            delayed,
        )

    def test_unrelated_reads_do_not_advance_order_visibility(self) -> None:
        delayed = ExchangeOrderSnapshot(
            **{
                **self.order.__dict__,
                "client_order_id": "dg1-unrelated-delayed",
                "exchange_order_id": "1004",
            }
        )
        self.backend.seed_order(delayed, visibility_delay_reads=1)

        self.adapter.get_instrument_rules(delayed.symbol)
        self.adapter.get_positions(delayed.symbol)
        self.adapter.get_position_mode()
        self.adapter.get_margin_account_snapshot()

        with self.assertRaises(ExchangeNotFoundError):
            self.adapter.get_order_by_client_id(
                delayed.symbol,
                delayed.client_order_id,
            )
        self.assertEqual(
            self.adapter.get_order_by_client_id(
                delayed.symbol,
                delayed.client_order_id,
            ),
            delayed,
        )

    def test_delayed_visibility_fault_defers_one_visibility_tick(self) -> None:
        delayed = ExchangeOrderSnapshot(
            **{
                **self.order.__dict__,
                "client_order_id": "dg1-fault-delayed",
                "exchange_order_id": "1005",
            }
        )
        self.backend.seed_order(delayed, visibility_delay_reads=1)
        self.backend.queue_fault(
            "get_order_by_client_id",
            FakeFault(FakeFaultKind.DELAYED_VISIBILITY),
        )

        for _ in range(2):
            with self.assertRaises(ExchangeNotFoundError):
                self.adapter.get_order_by_client_id(
                    delayed.symbol,
                    delayed.client_order_id,
                )
        self.assertEqual(
            self.adapter.get_order_by_client_id(
                delayed.symbol,
                delayed.client_order_id,
            ),
            delayed,
        )

    def test_phase2_write_entries_fail_before_touching_backend(self) -> None:
        submit = SubmitLimitOrder(
            symbol=self.order.symbol,
            client_order_id="dg1-new-order",
            side=Side.BUY,
            position_side=PositionSide.BOTH,
            price=Decimal("64000"),
            contracts=1,
        )
        cancel = CancelOrder(
            symbol=self.order.symbol,
            client_order_id=self.order.client_order_id,
        )
        for command_name, operation in (
            ("submit_limit_order", lambda: self.adapter.submit_limit_order(submit)),
            ("cancel_order", lambda: self.adapter.cancel_order(cancel)),
        ):
            with self.subTest(command_name=command_name):
                with self.assertRaisesRegex(
                    TradingDisabledError,
                    "^Trading disabled in Phase 2$",
                ):
                    operation()
                self.assertEqual(self.backend.operation_call_count(command_name), 0)


if __name__ == "__main__":
    unittest.main()
