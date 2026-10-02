import unittest
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

from gridtrader.core.enums import ExchangeOrderStatus, PositionSide, Side
from gridtrader.core.errors import TradingDisabledError
from gridtrader.exchange.binance_coinm.adapter import CoinMExchangeAdapter
from gridtrader.exchange.binance_coinm.connector import OfficialCoinMConnector
from gridtrader.exchange.binance_coinm.errors import (
    ExchangeInvalidResponseError,
    ExchangeNotFoundError,
)
from gridtrader.exchange.binance_coinm.mapper import (
    map_fill,
    map_instrument_rules,
    map_margin_account,
    map_order,
    map_position,
    map_position_mode,
)
from gridtrader.exchange.binance_coinm.retry_policy import RetryPolicy, RetrySettings
from gridtrader.exchange.models import (
    CancelOrder,
    PositionMode,
    SubmitLimitOrder,
    TradePage,
)


OBSERVED_AT = datetime(2026, 10, 2, 12, 0, tzinfo=timezone.utc)
FIRST_TRADE_AT = datetime.fromtimestamp(1_780_401_600, tz=timezone.utc)
TRADE_FROM = datetime.fromtimestamp(1_780_401_600, tz=timezone.utc)
TRADE_END = datetime.fromtimestamp(1_780_401_620, tz=timezone.utc)


class SdkResponse:
    """Minimal stand-in for the official SDK response wrapper."""

    def __init__(self, payload: object) -> None:
        self._payload = payload

    def data(self) -> object:
        return self._payload


def instrument(
    symbol: str,
    *,
    pair: str,
    base_asset: str,
    tick_size: str,
    contract_size: str,
    contract_type: str = "PERPETUAL",
    contract_status: str = "TRADING",
) -> dict[str, object]:
    return {
        "symbol": symbol,
        "pair": pair,
        "contractType": contract_type,
        "contractStatus": contract_status,
        "baseAsset": base_asset,
        "quoteAsset": "USD",
        "marginAsset": base_asset,
        "contractSize": contract_size,
        "orderTypes": ["LIMIT", "MARKET", "STOP"],
        "filters": [
            {
                "filterType": "PRICE_FILTER",
                "minPrice": tick_size,
                "maxPrice": "10000000",
                "tickSize": tick_size,
            },
            {
                "filterType": "LOT_SIZE",
                "minQty": "1",
                "maxQty": "1000000",
                "stepSize": "1",
            },
        ],
    }


class FakeOfficialCoinMRestApi:
    """Offline SDK double exposing only the read methods used in Phase 2."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, object]] = []
        self.server_time_ms = int(TRADE_END.timestamp() * 1000)
        self.symbols = [
            instrument(
                "BTCUSD_PERP",
                pair="BTCUSD",
                base_asset="BTC",
                tick_size="0.1",
                contract_size="100",
            ),
            instrument(
                "ETHUSD_PERP",
                pair="ETHUSD",
                base_asset="ETH",
                tick_size="0.01",
                contract_size="10",
            ),
            instrument(
                "DOGEUSD_PERP",
                pair="DOGEUSD",
                base_asset="DOGE",
                tick_size="0.000001",
                contract_size="10",
            ),
            instrument(
                "BTCUSD_261225",
                pair="BTCUSD",
                base_asset="BTC",
                tick_size="0.1",
                contract_size="100",
                contract_type="CURRENT_QUARTER",
            ),
        ]
        self.orders = [
            {
                "symbol": "BTCUSD_PERP",
                "clientOrderId": "grid-btc-new",
                "orderId": 101,
                "status": "NEW",
                "side": "BUY",
                "positionSide": "BOTH",
                "type": "LIMIT",
                "origType": "LIMIT",
                "timeInForce": "GTC",
                "origQty": "3",
                "executedQty": "0",
                "price": "65000.1",
                "avgPrice": "0",
                "reduceOnly": False,
                "updateTime": 1_780_401_600_000,
            },
            {
                "symbol": "ETHUSD_PERP",
                "clientOrderId": "grid-eth-partial",
                "orderId": 102,
                "status": "PARTIALLY_FILLED",
                "side": "SELL",
                "positionSide": "SHORT",
                "type": "LIMIT",
                "origType": "LIMIT",
                "timeInForce": "GTC",
                "origQty": "7",
                "executedQty": "2",
                "price": "4100.25",
                "avgPrice": "4101.5",
                "reduceOnly": False,
                "updateTime": 1_780_401_601_000,
            },
        ]
        self.positions = [
            {
                "symbol": "BTCUSD_PERP",
                "positionSide": "BOTH",
                "positionAmt": "-2",
                "entryPrice": "65000.1",
                "markPrice": "64990.5",
                "unRealizedProfit": "0.000004",
                "leverage": "5",
                "marginType": "cross",
                "liquidationPrice": "80000",
                "updateTime": 1_780_401_602_000,
            },
            {
                "symbol": "ETHUSD_PERP",
                "positionSide": "LONG",
                "positionAmt": "4",
                "entryPrice": "4000",
                "markPrice": "4100",
                "unRealizedProfit": "0.0002",
                "leverage": "8",
                "marginType": "isolated",
                "liquidationPrice": "3000",
                "updateTime": 1_780_401_603_000,
            },
        ]
        self.trades = {
            "BTCUSD_PERP": [
                {
                    "symbol": "BTCUSD_PERP",
                    "id": trade_id,
                    "orderId": 101,
                    "clientOrderId": "grid-btc-new",
                    "side": "BUY",
                    "positionSide": "BOTH",
                    "price": str(65000 + trade_id - 9001),
                    "qty": "1",
                    "realizedPnl": "0.000003",
                    "commission": "0.000001",
                    "commissionAsset": "BTC",
                    "baseQty": "0.00153846",
                    "time": 1_780_401_600_000 + trade_id,
                }
                for trade_id in (9001, 9002, 9003)
            ]
        }
        self.return_newest_trades_first = False

    def exchange_information(self) -> SdkResponse:
        self.calls.append(("exchange_information", None))
        return SdkResponse({"symbols": self.symbols})

    def check_server_time(self) -> SdkResponse:
        self.calls.append(("check_server_time", None))
        return SdkResponse({"serverTime": self.server_time_ms})

    def current_all_open_orders(self, *, symbol: str | None) -> SdkResponse:
        self.calls.append(("current_all_open_orders", symbol))
        return SdkResponse(
            self.orders
            if symbol is None
            else [item for item in self.orders if item["symbol"] == symbol]
        )

    def position_information(self, *, pair: str | None) -> SdkResponse:
        self.calls.append(("position_information", pair))
        return SdkResponse(self.positions)

    def get_current_position_mode(self) -> SdkResponse:
        self.calls.append(("get_current_position_mode", None))
        return SdkResponse({"dualSidePosition": True})

    def account_information(self) -> SdkResponse:
        self.calls.append(("account_information", None))
        return SdkResponse(
            {
                "totalWalletBalance": "1.25",
                "totalUnrealizedProfit": "0.03",
                "availableBalance": "0.80",
                "assets": [
                    {
                        "asset": "BTC",
                        "walletBalance": "1.1",
                        "availableBalance": "0.7",
                        "unrealizedProfit": "0.02",
                        "updateTime": 1_780_401_604_000,
                    },
                    {
                        "asset": "ETH",
                        "walletBalance": "0.15",
                        "availableBalance": "0.1",
                        "unrealizedProfit": "0.01",
                        "updateTime": 1_780_401_604_000,
                    },
                ],
            }
        )

    def account_trade_list(self, **params: object) -> SdkResponse:
        self.calls.append(("account_trade_list", dict(params)))
        symbol = str(params["symbol"])
        items = list(self.trades.get(symbol, []))
        if "from_id" in params:
            items = [item for item in items if int(item["id"]) >= int(params["from_id"])]
        if "start_time" in params:
            items = [
                item
                for item in items
                if int(item["time"]) >= int(params["start_time"])
            ]
        if "end_time" in params:
            items = [
                item
                for item in items
                if int(item["time"]) <= int(params["end_time"])
            ]
        page_limit = int(params["limit"])
        if self.return_newest_trades_first:
            # Model an API implementation that fills a limited response from
            # the newest edge of the interval.  Reverse it as well so adapter
            # correctness cannot depend on response ordering.
            return SdkResponse(list(reversed(items[-page_limit:])))
        return SdkResponse(items[:page_limit])

    def query_order(
        self,
        *,
        symbol: str,
        orig_client_order_id: str | None = None,
        order_id: int | None = None,
    ) -> SdkResponse:
        self.calls.append(
            (
                "query_order",
                {
                    "symbol": symbol,
                    "orig_client_order_id": orig_client_order_id,
                    "order_id": order_id,
                },
            )
        )
        for order in self.orders:
            if (
                order["symbol"] == symbol
                and (
                    order["clientOrderId"] == orig_client_order_id
                    or order["orderId"] == order_id
                )
            ):
                return SdkResponse(order)
        raise AssertionError("test requested an unknown order")


class CoinMMappingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.rest_api = FakeOfficialCoinMRestApi()
        self.adapter = CoinMExchangeAdapter(
            OfficialCoinMConnector(self.rest_api),
            account_id="coinm-main",
            clock=lambda: OBSERVED_AT,
            trade_page_limit=2,
        )

    def _drain_trade_pages(
        self,
        first_page: TradePage,
        *,
        adapter: CoinMExchangeAdapter | None = None,
        symbol: str = "BTCUSD_PERP",
    ) -> list[TradePage]:
        active_adapter = adapter or self.adapter
        pages = [first_page]
        for _ in range(256):
            page = pages[-1]
            if page.complete:
                return pages
            page = active_adapter.get_user_trades(
                symbol,
                cursor=page.next_cursor,
            )
            pages.append(page)
        self.fail("trade replay did not complete within its deterministic bound")

    def test_dynamic_perpetual_instrument_mapping_is_not_symbol_specific(self) -> None:
        expected = {
            "BTCUSD_PERP": ("BTCUSD", "BTC", Decimal("0.1"), Decimal("100")),
            "ETHUSD_PERP": ("ETHUSD", "ETH", Decimal("0.01"), Decimal("10")),
            "DOGEUSD_PERP": (
                "DOGEUSD",
                "DOGE",
                Decimal("0.000001"),
                Decimal("10"),
            ),
        }
        hashes: set[str] = set()
        for symbol, (pair, asset, tick, contract_size) in expected.items():
            with self.subTest(symbol=symbol):
                rules = self.adapter.get_instrument_rules(symbol)
                self.assertEqual(rules.symbol, symbol)
                self.assertEqual(rules.pair, pair)
                self.assertEqual(rules.base_asset, asset)
                self.assertEqual(rules.margin_asset, asset)
                self.assertEqual(rules.contract_type, "PERPETUAL")
                self.assertEqual(rules.status, "TRADING")
                self.assertEqual(rules.price_tick, tick)
                self.assertEqual(rules.contract_size, contract_size)
                self.assertEqual(rules.contract_step, 1)
                self.assertEqual(rules.supported_order_types, ("LIMIT", "MARKET", "STOP"))
                self.assertEqual(rules.observed_at, OBSERVED_AT)
                self.assertIsNotNone(rules.rules_hash)
                hashes.add(rules.rules_hash or "")
        self.assertEqual(len(hashes), len(expected))

    def test_non_perpetual_or_unknown_symbols_are_rejected(self) -> None:
        for symbol in ("BTCUSD_261225", "NO_SUCH_SYMBOL"):
            with self.subTest(symbol=symbol), self.assertRaises(ExchangeNotFoundError):
                self.adapter.get_instrument_rules(symbol)

    def test_contract_status_is_authoritative_over_legacy_status_alias(self) -> None:
        self.rest_api.symbols[0]["status"] = "BREAK"
        rules = self.adapter.get_instrument_rules("BTCUSD_PERP")
        self.assertEqual(rules.status, "TRADING")

        self.rest_api.symbols[0]["status"] = "TRADING"
        self.rest_api.symbols[0]["contractStatus"] = "PRE_DELIVERING"
        with self.assertRaises(ExchangeNotFoundError):
            self.adapter.get_instrument_rules("BTCUSD_PERP")

    def test_instrument_rules_require_structured_limit_order_support(self) -> None:
        original = dict(self.rest_api.symbols[0])
        bad_values = (None, [], "LIMIT", ["MARKET"])
        for bad_value in bad_values:
            with self.subTest(order_types=bad_value):
                candidate = dict(original)
                candidate["orderTypes"] = bad_value
                with self.assertRaises(ExchangeInvalidResponseError):
                    map_instrument_rules(candidate, observed_at=OBSERVED_AT)

        missing = dict(original)
        missing.pop("orderTypes")
        with self.assertRaises(ExchangeInvalidResponseError):
            map_instrument_rules(missing, observed_at=OBSERVED_AT)

    def test_orders_and_positions_preserve_coinm_contract_semantics(self) -> None:
        eth_orders = self.adapter.get_open_orders("ETHUSD_PERP")
        self.assertEqual(len(eth_orders), 1)
        order = eth_orders[0]
        self.assertIs(order.status, ExchangeOrderStatus.PARTIALLY_FILLED)
        self.assertIs(order.side, Side.SELL)
        self.assertIs(order.position_side, PositionSide.SHORT)
        self.assertEqual(order.original_contracts, 7)
        self.assertEqual(order.filled_contracts, 2)
        self.assertEqual(order.average_fill_price, Decimal("4101.5"))

        exact = self.adapter.get_order_by_client_id(
            "BTCUSD_PERP", "grid-btc-new"
        )
        self.assertEqual(exact.exchange_order_id, "101")
        self.assertIs(exact.status, ExchangeOrderStatus.NEW)
        self.assertEqual(
            self.adapter.get_order_by_exchange_id("BTCUSD_PERP", "101"),
            exact,
        )

        btc_positions = self.adapter.get_positions("BTCUSD_PERP")
        self.assertEqual(len(btc_positions), 1)
        position = btc_positions[0]
        self.assertEqual(position.contracts, -2)
        self.assertIs(position.position_side, PositionSide.BOTH)
        self.assertFalse(position.isolated)
        self.assertEqual(position.margin_asset, "BTC")
        self.assertEqual(position.liquidation_price, Decimal("80000"))

        eth_position = self.adapter.get_positions("ETHUSD_PERP")[0]
        self.assertEqual(eth_position.contracts, 4)
        self.assertIs(eth_position.position_side, PositionSide.LONG)
        self.assertTrue(eth_position.isolated)

    def test_symbol_scoped_open_orders_rejects_cross_symbol_payload(self) -> None:
        self.rest_api.current_all_open_orders = (  # type: ignore[method-assign]
            lambda **_params: SdkResponse(self.rest_api.orders)
        )
        with self.assertRaisesRegex(
            ExchangeInvalidResponseError,
            "different symbol",
        ):
            self.adapter.get_open_orders("BTCUSD_PERP")

    def test_position_margin_asset_comes_from_exchange_info_and_conflicts_fail(self) -> None:
        # COIN-M positionRisk does not itself carry marginAsset.  The adapter
        # joins that fact from the exact exchangeInfo symbol.
        position = self.adapter.get_positions("BTCUSD_PERP")[0]
        self.assertEqual(position.margin_asset, "BTC")

        self.rest_api.positions[0]["marginAsset"] = "ETH"
        with self.assertRaisesRegex(
            ExchangeInvalidResponseError,
            "marginAsset conflicts",
        ):
            self.adapter.get_positions("BTCUSD_PERP")

    def test_every_required_order_status_maps_without_spot_semantics(self) -> None:
        expected_filled = {
            "NEW": 0,
            "PARTIALLY_FILLED": 1,
            "FILLED": 3,
            "CANCELED": 1,
            "REJECTED": 0,
            "EXPIRED": 0,
        }
        for status, filled in expected_filled.items():
            with self.subTest(status=status):
                mapped = map_order(
                    {
                        "symbol": "DOGEUSD_PERP",
                        "clientOrderId": f"grid-{status.lower()}",
                        "orderId": 200,
                        "status": status,
                        "side": "SELL",
                        "positionSide": "BOTH",
                        "type": "LIMIT",
                        "origType": "LIMIT",
                        "timeInForce": "GTC",
                        "origQty": "3",
                        "executedQty": str(filled),
                        "price": "0.1",
                        "avgPrice": "0.1" if filled else "0",
                        "reduceOnly": False,
                        "updateTime": 1_780_401_600_000,
                    }
                )
                self.assertEqual(mapped.status.value, status.lower())
                self.assertEqual(mapped.filled_contracts, filled)
                self.assertEqual(mapped.order_type, "LIMIT")
                self.assertEqual(mapped.time_in_force, "GTC")

    def test_order_mapping_requires_type_and_time_in_force(self) -> None:
        raw = dict(self.rest_api.orders[0])
        for missing in ("type", "timeInForce"):
            with self.subTest(missing=missing):
                incomplete = dict(raw)
                incomplete.pop(missing)
                with self.assertRaises(ExchangeInvalidResponseError):
                    map_order(incomplete)

    def test_order_mapping_rejects_conflicting_original_type(self) -> None:
        raw = dict(self.rest_api.orders[0])
        raw["origType"] = "STOP"
        with self.assertRaisesRegex(
            ExchangeInvalidResponseError,
            "conflicts with origType",
        ):
            map_order(raw)

    def test_trade_mapping_and_pagination_expose_completeness_evidence(self) -> None:
        first = self.adapter.get_user_trades(
            "BTCUSD_PERP",
            from_time=TRADE_FROM,
            end_time=TRADE_END,
        )
        self.assertFalse(first.complete)
        self.assertIsNotNone(first.next_cursor)
        self.assertTrue(first.next_cursor.startswith("coinm-trades-v2."))
        self.assertNotEqual(first.next_cursor, "9003")
        self.assertEqual(first.snapshot_time, TRADE_END)

        # The opaque cursor can be resumed by a new adapter instance after a
        # process restart.  A page may legitimately be empty when a full time
        # interval had to be split before its completeness could be proven.
        restarted = CoinMExchangeAdapter(
            OfficialCoinMConnector(self.rest_api),
            account_id="coinm-main",
            clock=lambda: OBSERVED_AT,
            trade_page_limit=2,
        )
        pages = self._drain_trade_pages(first, adapter=restarted)
        fills = [fill for page in pages for fill in page.items]
        self.assertEqual([item.trade_id for item in fills], ["9001", "9002", "9003"])
        self.assertTrue(pages[-1].complete)
        self.assertIsNone(pages[-1].next_cursor)
        self.assertEqual(pages[-1].pagination_watermark, "9003")
        self.assertTrue(all(page.snapshot_time == TRADE_END for page in pages))

        fill = fills[0]
        self.assertEqual(fill.account_id, "coinm-main")
        self.assertEqual(fill.contracts, 1)
        self.assertEqual(fill.realized_pnl, Decimal("0.000003"))
        self.assertEqual(fill.commission, Decimal("0.000001"))
        self.assertEqual(fill.commission_asset, "BTC")
        self.assertEqual(fill.base_amount, Decimal("0.00153846"))

        trade_calls = [
            params
            for operation, params in self.rest_api.calls
            if operation == "account_trade_list"
        ]
        self.assertTrue(trade_calls)
        for params in trade_calls:
            self.assertIn("start_time", params)  # type: ignore[operator]
            self.assertIn("end_time", params)  # type: ignore[operator]
            self.assertNotIn("from_id", params)  # type: ignore[operator]

    def test_trade_replay_uses_at_most_seven_day_windows(self) -> None:
        adapter = CoinMExchangeAdapter(
            OfficialCoinMConnector(self.rest_api),
            account_id="coinm-main",
            clock=lambda: OBSERVED_AT,
            trade_page_limit=1000,
        )
        replay_from = FIRST_TRADE_AT - timedelta(days=1)
        snapshot_end = replay_from + timedelta(days=8)
        self.rest_api.server_time_ms = int(snapshot_end.timestamp() * 1000)

        first = adapter.get_user_trades(
            "BTCUSD_PERP",
            from_time=replay_from,
            end_time=snapshot_end,
        )
        first_params = self.rest_api.calls[-1][1]
        self.assertLessEqual(  # type: ignore[index]
            first_params["end_time"] - first_params["start_time"],
            7 * 24 * 60 * 60 * 1000,
        )
        self.assertFalse(first.complete)
        self.assertIsNotNone(first.next_cursor)

        second = adapter.get_user_trades(
            "BTCUSD_PERP",
            cursor=first.next_cursor,
        )
        second_params = self.rest_api.calls[-1][1]
        self.assertLessEqual(  # type: ignore[index]
            second_params["end_time"] - second_params["start_time"],
            7 * 24 * 60 * 60 * 1000,
        )
        self.assertTrue(second.complete)
        self.assertEqual(second.snapshot_time, first.snapshot_time)

    def test_resumed_time_windows_do_not_admit_trades_after_fixed_snapshot(self) -> None:
        snapshot_end = FIRST_TRADE_AT + timedelta(seconds=30)
        self.rest_api.server_time_ms = int(snapshot_end.timestamp() * 1000)
        first = self.adapter.get_user_trades(
            "BTCUSD_PERP",
            from_time=FIRST_TRADE_AT - timedelta(seconds=1),
            end_time=snapshot_end,
        )
        self.rest_api.trades["BTCUSD_PERP"].append(
            {
                **self.rest_api.trades["BTCUSD_PERP"][-1],
                "id": 9004,
                "time": int(
                    (snapshot_end + timedelta(seconds=1)).timestamp() * 1000
                ),
            }
        )

        pages = self._drain_trade_pages(first)
        fills = [fill for page in pages for fill in page.items]
        self.assertEqual([item.trade_id for item in fills], ["9001", "9002", "9003"])
        self.assertTrue(pages[-1].complete)
        self.assertTrue(all(page.snapshot_time == snapshot_end for page in pages))

    def test_newest_limited_responses_are_split_without_skipping_older_fills(
        self,
    ) -> None:
        self.rest_api.return_newest_trades_first = True
        first = self.adapter.get_user_trades(
            "BTCUSD_PERP",
            from_time=FIRST_TRADE_AT,
            end_time=TRADE_END,
        )

        pages = self._drain_trade_pages(first)
        fills = [fill for page in pages for fill in page.items]
        self.assertEqual([fill.trade_id for fill in fills], ["9001", "9002", "9003"])
        self.assertEqual(len({fill.deduplication_key for fill in fills}), 3)
        self.assertTrue(pages[-1].complete)

    def test_trade_cursor_is_bound_to_account_and_symbol(self) -> None:
        first = self.adapter.get_user_trades(
            "BTCUSD_PERP",
            from_time=TRADE_FROM,
            end_time=TRADE_END,
        )
        self.assertIsNotNone(first.next_cursor)

        other_account = CoinMExchangeAdapter(
            OfficialCoinMConnector(self.rest_api),
            account_id="another-account",
            clock=lambda: OBSERVED_AT,
            trade_page_limit=2,
        )
        with self.assertRaisesRegex(ValueError, "account and symbol"):
            other_account.get_user_trades(
                "BTCUSD_PERP",
                cursor=first.next_cursor,
            )
        with self.assertRaisesRegex(ValueError, "account and symbol"):
            self.adapter.get_user_trades(
                "ETHUSD_PERP",
                cursor=first.next_cursor,
            )

    def test_full_one_millisecond_trade_bucket_fails_closed(self) -> None:
        bucket_ms = int(self.rest_api.trades["BTCUSD_PERP"][0]["time"])
        bucket_time = datetime.fromtimestamp(bucket_ms / 1000, tz=timezone.utc)
        second = {
            **self.rest_api.trades["BTCUSD_PERP"][1],
            "time": bucket_ms,
        }
        self.rest_api.trades["BTCUSD_PERP"] = [
            self.rest_api.trades["BTCUSD_PERP"][0],
            second,
        ]
        self.rest_api.server_time_ms = bucket_ms

        with self.assertRaisesRegex(
            ExchangeInvalidResponseError,
            "unpageable full 1ms bucket",
        ):
            self.adapter.get_user_trades(
                "BTCUSD_PERP",
                from_time=bucket_time,
                end_time=bucket_time,
            )

    def test_initial_trade_replay_requires_an_explicit_lower_bound(self) -> None:
        with self.assertRaisesRegex(ValueError, "from_time is required"):
            self.adapter.get_user_trades("BTCUSD_PERP")

    def test_trade_replay_rejects_a_lower_bound_outside_retention(self) -> None:
        server_time = datetime(2026, 10, 2, 12, 0, tzinfo=timezone.utc)
        self.rest_api.server_time_ms = int(server_time.timestamp() * 1000)
        retention_floor = server_time - timedelta(days=90)

        with self.assertRaisesRegex(
            ExchangeInvalidResponseError,
            "older than the provable",
        ):
            self.adapter.get_user_trades(
                "BTCUSD_PERP",
                from_time=retention_floor - timedelta(milliseconds=1),
            )

        page = self.adapter.get_user_trades(
            "BTCUSD_PERP",
            from_time=retention_floor,
            end_time=retention_floor + timedelta(seconds=1),
        )
        self.assertTrue(page.complete)

    def test_trade_snapshot_uses_binance_server_time(self) -> None:
        server_time = FIRST_TRADE_AT + timedelta(seconds=30)
        self.rest_api.server_time_ms = int(server_time.timestamp() * 1000)
        page = self.adapter.get_user_trades(
            "BTCUSD_PERP",
            from_time=FIRST_TRADE_AT - timedelta(seconds=1),
        )
        self.assertEqual(page.snapshot_time, server_time)

    def test_server_time_dto_alias_is_supported_and_malformed_data_fails(self) -> None:
        class ServerTimeDto:
            def __init__(self, server_time: object) -> None:
                self.server_time = server_time

        server_time = FIRST_TRADE_AT + timedelta(seconds=30)
        returned = [ServerTimeDto(int(server_time.timestamp() * 1000))]
        self.rest_api.check_server_time = (  # type: ignore[method-assign]
            lambda: SdkResponse(returned[0])
        )
        page = self.adapter.get_user_trades(
            "BTCUSD_PERP",
            from_time=FIRST_TRADE_AT - timedelta(seconds=1),
        )
        self.assertEqual(page.snapshot_time, server_time)

        returned[0] = ServerTimeDto("not-a-timestamp")
        with self.assertRaisesRegex(
            ExchangeInvalidResponseError,
            "server-time response is invalid",
        ):
            self.adapter.get_user_trades(
                "BTCUSD_PERP",
                from_time=FIRST_TRADE_AT - timedelta(seconds=1),
            )

    def test_exact_order_lookup_rejects_mismatched_sdk_identity(self) -> None:
        original = self.rest_api.orders[0]
        mismatched = {**original, "clientOrderId": "wrong-client"}
        self.rest_api.query_order = (  # type: ignore[method-assign]
            lambda **_params: SdkResponse(mismatched)
        )
        with self.assertRaisesRegex(
            ExchangeInvalidResponseError,
            "identity did not match",
        ):
            self.adapter.get_order_by_client_id("BTCUSD_PERP", "grid-btc-new")
        mismatched = {**original, "orderId": 999}
        self.rest_api.query_order = (  # type: ignore[method-assign]
            lambda **_params: SdkResponse(mismatched)
        )
        with self.assertRaisesRegex(
            ExchangeInvalidResponseError,
            "identity did not match",
        ):
            self.adapter.get_order_by_exchange_id("BTCUSD_PERP", "101")

    def test_sdk_response_decoding_is_inside_retry_boundary(self) -> None:
        class FlakyResponse:
            def __init__(self, attempts: list[int]) -> None:
                self.attempts = attempts

            def data(self) -> object:
                self.attempts[0] += 1
                if self.attempts[0] == 1:
                    raise ConnectionError("response body interrupted")
                return {"serverTime": 1}

        class FlakyApi:
            def __init__(self) -> None:
                self.calls = 0
                self.decode_attempts = [0]

            def check_server_time(self) -> FlakyResponse:
                self.calls += 1
                return FlakyResponse(self.decode_attempts)

        rest_api = FlakyApi()
        connector = OfficialCoinMConnector(
            rest_api,
            retry_policy=RetryPolicy(
                RetrySettings(max_attempts=2),
                sleep=lambda _seconds: None,
                random_value=lambda: 0,
            ),
        )
        self.assertEqual(connector.check_server_time(), {"serverTime": 1})
        self.assertEqual(rest_api.calls, 2)
        self.assertEqual(rest_api.decode_attempts[0], 2)

    def test_position_mode_and_margin_account_are_read_only_observations(self) -> None:
        mode = self.adapter.get_position_mode()
        self.assertIs(mode.mode, PositionMode.HEDGE)
        self.assertEqual(mode.observed_at, OBSERVED_AT)

        account = self.adapter.get_margin_account_snapshot()
        self.assertEqual(account.total_wallet_balance, Decimal("1.25"))
        self.assertEqual(account.total_unrealized_pnl, Decimal("0.03"))
        self.assertEqual(account.available_balance, Decimal("0.80"))
        self.assertEqual([item.asset for item in account.balances], ["BTC", "ETH"])

    def test_invalid_position_mode_boolean_is_not_silently_coerced(self) -> None:
        with self.assertRaises(ExchangeInvalidResponseError):
            map_position_mode(
                {"dualSidePosition": "not-a-boolean"}, observed_at=OBSERVED_AT
            )

    def test_invalid_reduce_only_boolean_is_not_silently_coerced(self) -> None:
        raw_order = dict(self.rest_api.orders[0])
        raw_order["reduceOnly"] = "false"
        with self.assertRaises(ExchangeInvalidResponseError):
            map_order(raw_order)

    def test_snake_case_order_fill_and_position_dtos_are_supported(self) -> None:
        order = map_order(
            {
                "symbol": "BTCUSD_PERP",
                "client_order_id": "snake-order",
                "order_id": 501,
                "status": "PARTIALLY_FILLED",
                "side": "BUY",
                "position_side": "BOTH",
                "order_type": "LIMIT",
                "orig_type": "LIMIT",
                "time_in_force": "GTC",
                "orig_qty": "3",
                "executed_qty": "1",
                "price": "65000",
                "avg_price": "65001",
                "reduce_only": False,
                "update_time": 1_780_401_600_000,
            }
        )
        self.assertEqual(order.exchange_order_id, "501")
        self.assertEqual(order.filled_contracts, 1)
        self.assertEqual(order.average_fill_price, Decimal("65001"))

        fill = map_fill(
            {
                "symbol": "BTCUSD_PERP",
                "trade_id": 601,
                "order_id": 501,
                "client_order_id": "snake-order",
                "side": "BUY",
                "position_side": "BOTH",
                "price": "65001",
                "qty": "1",
                "realized_pnl": "0.0002",
                "commission": "0.000001",
                "commission_asset": "BTC",
                "base_qty": "0.001538",
                "time": 1_780_401_601_000,
            },
            account_id="coinm-main",
        )
        self.assertEqual(fill.trade_id, "601")
        self.assertEqual(fill.realized_pnl, Decimal("0.0002"))
        self.assertEqual(fill.base_amount, Decimal("0.001538"))

        position = map_position(
            {
                "symbol": "BTCUSD_PERP",
                "position_side": "BOTH",
                "position_amt": "-2",
                "entry_price": "65000",
                "mark_price": "64990",
                "unrealized_pnl": "0.0003",
                "leverage": "5",
                "margin_asset": "BTC",
                "margin_type": "crossed",
                "liquidation_price": "80000",
                "update_time": 1_780_401_602_000,
            },
            observed_at=OBSERVED_AT,
        )
        self.assertEqual(position.contracts, -2)
        self.assertEqual(position.entry_price, Decimal("65000"))
        self.assertFalse(position.isolated)

    def test_snake_case_instrument_and_balance_dtos_are_supported(self) -> None:
        rules = map_instrument_rules(
            {
                "symbol": "BTCUSD_PERP",
                "pair": "BTCUSD",
                "contract_type": "PERPETUAL",
                "contract_status": "TRADING",
                "base_asset": "BTC",
                "quote_asset": "USD",
                "margin_asset": "BTC",
                "contract_size": "100",
                "order_types": ["LIMIT"],
                "filters": [
                    {
                        "filter_type": "PRICE_FILTER",
                        "min_price": "0.1",
                        "max_price": "1000000",
                        "tick_size": "0.1",
                    },
                    {
                        "filter_type": "LOT_SIZE",
                        "min_qty": "1",
                        "max_qty": "1000",
                        "step_size": "1",
                    },
                ],
            },
            observed_at=OBSERVED_AT,
        )
        self.assertEqual(rules.contract_size, Decimal("100"))
        self.assertEqual(rules.contract_step, 1)

        account = map_margin_account(
            {
                "total_wallet_balance": "1.25",
                "total_unrealized_pnl": "0.03",
                "available_balance": "0.80",
                "assets": [
                    {
                        "asset": "BTC",
                        "wallet_balance": "1.25",
                        "available_balance": "0.80",
                        "unrealized_profit": "0.03",
                        "update_time": 1_780_401_604_000,
                    }
                ],
            },
            observed_at=OBSERVED_AT,
        )
        self.assertEqual(account.total_wallet_balance, Decimal("1.25"))
        self.assertEqual(account.balances[0].available_balance, Decimal("0.80"))

    def test_order_authoritative_fields_cannot_be_missing_or_none(self) -> None:
        raw = dict(self.rest_api.orders[0])
        for name in ("executedQty", "reduceOnly", "updateTime"):
            with self.subTest(field=name):
                self.assert_missing_or_none_rejected(
                    raw,
                    name,
                    map_order,
                )

    def test_fill_financial_fields_cannot_be_missing_or_none(self) -> None:
        raw = dict(self.rest_api.trades["BTCUSD_PERP"][0])
        for name in ("realizedPnl", "commission", "time"):
            with self.subTest(field=name):
                self.assert_missing_or_none_rejected(
                    raw,
                    name,
                    lambda value: map_fill(value, account_id="coinm-main"),
                )

    def test_position_authoritative_fields_cannot_be_missing_or_none(self) -> None:
        raw = dict(self.rest_api.positions[0])
        for name in (
            "entryPrice",
            "markPrice",
            "unRealizedProfit",
            "leverage",
            "marginType",
            "updateTime",
        ):
            with self.subTest(field=name):
                self.assert_missing_or_none_rejected(
                    raw,
                    name,
                    lambda value: map_position(
                        value,
                        observed_at=OBSERVED_AT,
                        margin_asset="BTC",
                    ),
                )

    def test_balance_authoritative_fields_cannot_be_missing_or_none(self) -> None:
        asset = {
            "asset": "BTC",
            "walletBalance": "1.1",
            "availableBalance": "0.7",
            "unrealizedProfit": "0.02",
            "updateTime": 1_780_401_604_000,
        }
        for name in (
            "walletBalance",
            "availableBalance",
            "unrealizedProfit",
            "updateTime",
        ):
            with self.subTest(field=name):
                for replacement in (self._missing, None):
                    candidate = dict(asset)
                    if replacement is self._missing:
                        candidate.pop(name)
                    else:
                        candidate[name] = None
                    with self.assertRaises(ExchangeInvalidResponseError):
                        map_margin_account(
                            {"assets": [candidate]},
                            observed_at=OBSERVED_AT,
                        )

        self.assert_missing_or_none_rejected(
            {"assets": [asset]},
            "assets",
            lambda value: map_margin_account(value, observed_at=OBSERVED_AT),
        )

    def test_genuinely_optional_fields_remain_optional(self) -> None:
        raw_order = dict(self.rest_api.orders[0])
        raw_order.pop("price")
        raw_order["avgPrice"] = "0E-8"
        order = map_order(raw_order)
        self.assertIsNone(order.price)
        self.assertIsNone(order.average_fill_price)

        raw_fill = dict(self.rest_api.trades["BTCUSD_PERP"][0])
        raw_fill.pop("clientOrderId")
        raw_fill.pop("baseQty")
        fill = map_fill(raw_fill, account_id="coinm-main")
        self.assertIsNone(fill.client_order_id)
        self.assertIsNone(fill.base_amount)

        raw_position = dict(self.rest_api.positions[0])
        raw_position.pop("liquidationPrice")
        position = map_position(
            raw_position,
            observed_at=OBSERVED_AT,
            margin_asset="BTC",
        )
        self.assertIsNone(position.liquidation_price)

        account = map_margin_account(
            {
                "assets": [
                    {
                        "asset": "BTC",
                        "walletBalance": "0",
                        "availableBalance": "0",
                        "unrealizedProfit": "0",
                        "updateTime": 1_780_401_604_000,
                    }
                ]
            },
            observed_at=OBSERVED_AT,
        )
        self.assertIsNone(account.total_wallet_balance)
        self.assertIsNone(account.total_unrealized_pnl)
        self.assertIsNone(account.available_balance)

    def test_explicit_authoritative_zeroes_are_not_treated_as_missing(self) -> None:
        raw_fill = dict(self.rest_api.trades["BTCUSD_PERP"][0])
        raw_fill["realizedPnl"] = "0"
        raw_fill["commission"] = "0"
        fill = map_fill(raw_fill, account_id="coinm-main")
        self.assertEqual(fill.realized_pnl, Decimal("0"))
        self.assertEqual(fill.commission, Decimal("0"))

        raw_position = dict(self.rest_api.positions[0])
        raw_position["positionAmt"] = "0"
        raw_position["entryPrice"] = "0"
        raw_position["unRealizedProfit"] = "0"
        raw_position["leverage"] = "1"
        position = map_position(
            raw_position,
            observed_at=OBSERVED_AT,
            margin_asset="BTC",
        )
        self.assertEqual(position.contracts, 0)
        self.assertEqual(position.entry_price, Decimal("0"))
        self.assertEqual(position.unrealized_pnl, Decimal("0"))
        self.assertEqual(position.leverage, 1)

    _missing = object()

    def assert_missing_or_none_rejected(
        self,
        payload: dict[str, Any],
        field_name: str,
        mapper: Callable[[dict[str, Any]], object],
    ) -> None:
        for replacement in (self._missing, None):
            candidate = dict(payload)
            if replacement is self._missing:
                candidate.pop(field_name)
            else:
                candidate[field_name] = None
            with self.assertRaises(ExchangeInvalidResponseError):
                mapper(candidate)

    def test_phase2_mutations_fail_before_the_sdk_double_is_touched(self) -> None:
        calls_before = list(self.rest_api.calls)
        submit = SubmitLimitOrder(
            symbol="BTCUSD_PERP",
            client_order_id="grid-new",
            side=Side.BUY,
            position_side=PositionSide.BOTH,
            price=Decimal("64000"),
            contracts=1,
        )
        cancel = CancelOrder("BTCUSD_PERP", "grid-btc-new")
        for operation in (
            lambda: self.adapter.submit_limit_order(submit),
            lambda: self.adapter.cancel_order(cancel),
        ):
            with self.subTest(operation=operation), self.assertRaisesRegex(
                TradingDisabledError, "^Trading disabled in Phase 2$"
            ):
                operation()
        self.assertEqual(self.rest_api.calls, calls_before)


if __name__ == "__main__":
    unittest.main()
