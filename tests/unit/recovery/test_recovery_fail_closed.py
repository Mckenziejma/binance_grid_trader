from __future__ import annotations

import unittest
from dataclasses import replace
from datetime import timedelta
from decimal import Decimal

from gridtrader.core.enums import (
    ExchangeOrderStatus,
    PositionSide,
    ReadinessState,
    Side,
)
from gridtrader.core.readiness import ReadinessGate
from gridtrader.exchange.errors import ExchangeTimeoutError, ExchangeUnavailableError
from gridtrader.exchange.fake import FakeExchangeAdapter
from gridtrader.exchange.fake_backend import FakeFault, FakeFaultKind
from gridtrader.exchange.models import ExchangeMarginBalance, MarginAccountSnapshot
from gridtrader.recovery.manager import RecoveryBlockedError, RecoveryManager
from gridtrader.storage import InvariantViolation

from .support import (
    ACCOUNT_ID,
    NOW,
    SYMBOL,
    local_position,
    make_exchange_order,
    make_harness,
    make_local_order,
    make_position,
)


class RecoveryFailClosedTests(unittest.TestCase):
    def setUp(self) -> None:
        self.harness = make_harness(default_page_size=1)

    def tearDown(self) -> None:
        self.harness.close()

    def test_semantic_order_identity_conflict_is_blocked_without_mutating_local(self) -> None:
        local = self.harness.add_local_order()
        self.harness.backend.seed_order(
            make_exchange_order(
                client_order_id=local["client_order_id"],
                side=Side.SELL,
            )
        )

        result = self.harness.manager.recover(
            self.harness.request(run_id="run-semantic-conflict")
        )

        self.assertFalse(result.complete)
        self.assertIs(result.state, ReadinessState.DEGRADED)
        self.assertTrue(
            any("AMBIGUOUS" in blocker and "side" in blocker for blocker in result.blockers)
        )
        with self.harness.ledger.unit_of_work(immediate=False) as uow:
            persisted = uow.orders.require(local["local_order_id"])
            checkpoint = uow.recovery_checkpoints.latest_for_strategy("strategy-1")
        self.assertEqual(persisted["exchange_status"], "new")
        self.assertIsNone(persisted["exchange_order_id"])
        self.assertEqual(checkpoint["status"], "BLOCKED")

    def test_non_limit_or_non_gtc_order_identity_conflict_blocks_ready(self) -> None:
        for field_name, field_value in (
            ("order_type", "STOP"),
            ("time_in_force", "IOC"),
        ):
            with self.subTest(field_name=field_name):
                harness = make_harness()
                try:
                    local = harness.add_local_order()
                    harness.backend.seed_order(
                        make_exchange_order(
                            client_order_id=local["client_order_id"],
                            **{field_name: field_value},
                        )
                    )

                    result = harness.manager.recover(
                        harness.request(run_id=f"run-{field_name}-conflict")
                    )

                    self.assertFalse(result.complete)
                    self.assertIs(result.state, ReadinessState.DEGRADED)
                    self.assertTrue(
                        any(field_name in blocker for blocker in result.blockers)
                    )
                finally:
                    harness.close()

    def test_older_conflicting_terminal_order_cannot_leave_local_active_and_ready(self) -> None:
        local = self.harness.add_local_order(
            exchange_update_ms=int((NOW + timedelta(seconds=10)).timestamp() * 1000)
        )
        self.harness.backend.seed_order(
            make_exchange_order(
                client_order_id=local["client_order_id"],
                status=ExchangeOrderStatus.CANCELED,
                update_time=NOW,
            )
        )

        with self.assertRaisesRegex(
            InvariantViolation,
            "stale exchange observation cannot be proven equivalent",
        ):
            self.harness.manager.recover(
                self.harness.request(run_id="run-stale-terminal")
            )

        self.assertIs(self.harness.readiness.state, ReadinessState.DEGRADED)
        with self.harness.ledger.unit_of_work(immediate=False) as uow:
            persisted = uow.orders.require(local["local_order_id"])
            checkpoint = uow.recovery_checkpoints.latest_for_strategy("strategy-1")
        self.assertEqual("active", persisted["local_state"])
        self.assertEqual("new", persisted["exchange_status"])
        self.assertEqual("BLOCKED", checkpoint["status"])

    def test_conflicting_fill_order_identity_is_not_attached_or_persisted(self) -> None:
        local = self.harness.add_local_order(exchange_order_id="90001")
        exchange_order = make_exchange_order(
            client_order_id=local["client_order_id"],
            exchange_order_id="99999",
        )
        self.harness.backend.seed_order(exchange_order)
        self.harness.backend.record_fill(
            SYMBOL,
            exchange_order.client_order_id,
            contracts=1,
            price=Decimal("50000"),
            trade_id="trade-conflicting-order-id",
        )

        result = self.harness.manager.recover(
            self.harness.request(run_id="run-conflicting-fill-order-id")
        )

        self.assertFalse(result.complete)
        self.assertIs(result.state, ReadinessState.DEGRADED)
        self.assertTrue(any("AMBIGUOUS" in blocker for blocker in result.blockers))
        with self.harness.ledger.unit_of_work(immediate=False) as uow:
            persisted = uow.orders.require(local["local_order_id"])
            fills = uow.fills.list_for_account_symbol(ACCOUNT_ID, SYMBOL)
            checkpoint = uow.recovery_checkpoints.latest_for_strategy("strategy-1")
        self.assertEqual(persisted["exchange_order_id"], "90001")
        self.assertEqual(fills, [])
        self.assertEqual(checkpoint["status"], "BLOCKED")

    def test_conflicting_client_and_exchange_id_lookups_block_recovery(self) -> None:
        local = self.harness.add_local_order(quantity_contracts=2)
        exchange_order = make_exchange_order(
            client_order_id=local["client_order_id"],
            original_contracts=2,
        )
        self.harness.backend.seed_order(exchange_order)
        self.harness.backend.record_fill(
            SYMBOL,
            exchange_order.client_order_id,
            contracts=2,
            price=Decimal("50000"),
            trade_id="trade-conflicting-exact-lookups",
        )
        self.harness.backend.queue_fault(
            "get_order_by_client_id",
            FakeFault(FakeFaultKind.MISSING_ORDER),
        )

        with self.assertRaisesRegex(
            RecoveryBlockedError,
            "conflicting evidence",
        ):
            self.harness.manager.recover(
                self.harness.request(run_id="run-conflicting-exact-lookups")
            )

        with self.harness.ledger.unit_of_work(immediate=False) as uow:
            checkpoint = uow.recovery_checkpoints.latest_for_strategy("strategy-1")
            fills = uow.fills.list_for_account_symbol(ACCOUNT_ID, SYMBOL)
        self.assertEqual(checkpoint["status"], "BLOCKED")
        self.assertEqual(
            [item["binance_trade_id"] for item in fills],
            ["trade-conflicting-exact-lookups"],
        )

    def test_one_way_mode_rejects_long_position_side_even_when_quantities_match(self) -> None:
        local = local_position(1)
        local["position_side"] = PositionSide.LONG.value
        with self.harness.ledger.unit_of_work() as uow:
            uow.positions.upsert(local)
            uow.commit()
        self.harness.backend.set_position(
            make_position(1, position_side=PositionSide.LONG)
        )

        result = self.harness.manager.recover(
            self.harness.request(run_id="run-mode-side-conflict")
        )

        self.assertFalse(result.complete)
        self.assertIn(
            "POSITION_MODE_POSITION_SIDE_INCONSISTENT",
            result.blockers,
        )

    def test_interrupted_unlinked_fills_do_not_advance_resume_cursor(self) -> None:
        first = make_exchange_order(
            client_order_id="dg1_orphan_a",
            exchange_order_id="orphan-order-a",
            original_contracts=1,
        )
        second = make_exchange_order(
            client_order_id="dg1_orphan_b",
            exchange_order_id="orphan-order-b",
            original_contracts=1,
        )
        self.harness.backend.seed_order(first)
        self.harness.backend.seed_order(second)
        for index, order in enumerate((first, second), start=1):
            self.harness.backend.record_fill(
                SYMBOL,
                order.client_order_id,
                contracts=1,
                price=Decimal("50000"),
                trade_id=f"orphan-trade-{index}",
            )
        self.harness.backend.queue_fault(
            "get_user_trades",
            FakeFault(FakeFaultKind.DUPLICATE_DATA),
        )
        self.harness.backend.queue_fault(
            "get_user_trades",
            FakeFault(FakeFaultKind.PAGINATION_INTERRUPTION),
        )

        with self.assertRaises(ExchangeUnavailableError):
            self.harness.manager.recover(
                self.harness.request(run_id="run-unlinked-page")
            )

        with self.harness.ledger.unit_of_work(immediate=False) as uow:
            checkpoint = uow.recovery_checkpoints.latest_for_strategy("strategy-1")
            fills = uow.fills.list_for_account_symbol(ACCOUNT_ID, SYMBOL)
            observed_trades = uow.exchange_trade_observations.list_all()
        self.assertEqual(checkpoint["status"], "BLOCKED")
        self.assertIsNone(checkpoint["next_trade_cursor"])
        self.assertEqual(checkpoint["trades_complete"], 0)
        self.assertEqual(fills, [])
        self.assertEqual(
            {item["binance_trade_id"] for item in observed_trades},
            {"orphan-trade-1"},
        )

    def test_interrupted_conflicting_side_fill_is_not_persisted_or_advanced(self) -> None:
        local = self.harness.add_local_order(quantity_contracts=2)
        exchange_order = make_exchange_order(
            client_order_id=local["client_order_id"],
            original_contracts=2,
        )
        self.harness.backend.seed_order(exchange_order)
        for index in range(2):
            self.harness.backend.record_fill(
                SYMBOL,
                exchange_order.client_order_id,
                contracts=1,
                price=Decimal("50000"),
                trade_id=f"side-conflict-{index}",
            )

        class ConflictingFillAdapter(FakeExchangeAdapter):
            def __init__(self) -> None:
                super().__init__(self_backend)
                self.calls = 0

            def get_user_trades(self, *args, **kwargs):  # type: ignore[no-untyped-def]
                self.calls += 1
                if self.calls == 2:
                    raise ExchangeUnavailableError("second page unavailable")
                page = super().get_user_trades(*args, **kwargs)
                return replace(
                    page,
                    items=tuple(replace(fill, side=Side.SELL) for fill in page.items),
                )

        self_backend = self.harness.backend
        manager = RecoveryManager(
            exchange=ConflictingFillAdapter(),
            ledger=self.harness.ledger,
            readiness=ReadinessGate(),
            clock=lambda: NOW,
        )

        with self.assertRaises(ExchangeUnavailableError):
            manager.recover(self.harness.request(run_id="run-side-conflict-page"))

        with self.harness.ledger.unit_of_work(immediate=False) as uow:
            checkpoint = uow.recovery_checkpoints.latest_for_strategy("strategy-1")
            fills = uow.fills.list_for_account_symbol(ACCOUNT_ID, SYMBOL)
        self.assertEqual(fills, [])
        self.assertIsNone(checkpoint["next_trade_cursor"])
        self.assertEqual(checkpoint["status"], "BLOCKED")

    def test_read_timeout_preserves_local_state_and_blocks_checkpoint(self) -> None:
        local = self.harness.add_local_order()
        self.harness.backend.queue_fault(
            "get_open_orders",
            FakeFault(FakeFaultKind.TIMEOUT_BEFORE_REQUEST),
        )

        with self.assertRaises(ExchangeTimeoutError):
            self.harness.manager.recover(
                self.harness.request(run_id="run-read-timeout")
            )

        with self.harness.ledger.unit_of_work(immediate=False) as uow:
            persisted = uow.orders.require(local["local_order_id"])
            checkpoint = uow.recovery_checkpoints.latest_for_strategy("strategy-1")
        self.assertEqual(persisted["exchange_status"], "new")
        self.assertEqual(persisted["cumulative_filled_contracts"], 0)
        self.assertEqual(checkpoint["status"], "BLOCKED")

    def test_corrupt_local_order_scope_blocks_before_exchange_reads(self) -> None:
        for column, value in (
            ("account_id", "another-account"),
            ("symbol", "ETHUSD_PERP"),
        ):
            with self.subTest(column=column):
                harness = make_harness()
                try:
                    local = harness.add_local_order()
                    with harness.ledger.unit_of_work() as uow:
                        assert uow.connection is not None
                        uow.connection.execute(
                            f"UPDATE orders SET {column} = ? WHERE local_order_id = ?",
                            (value, local["local_order_id"]),
                        )
                        uow.commit()

                    with self.assertRaisesRegex(
                        RecoveryBlockedError,
                        "local order does not match",
                    ):
                        harness.manager.recover(
                            harness.request(run_id=f"run-bad-{column}")
                        )

                    self.assertEqual(
                        harness.backend.operation_call_count("get_position_mode"),
                        0,
                    )
                    with harness.ledger.unit_of_work(immediate=False) as uow:
                        self.assertEqual(uow.recovery_checkpoints.list_all(), [])
                finally:
                    harness.close()

    def test_local_order_created_during_recovery_fences_ready(self) -> None:
        ledger = self.harness.ledger
        backend = self.harness.backend

        class LocalMutationAdapter(FakeExchangeAdapter):
            def __init__(self) -> None:
                super().__init__(backend)
                self.injected = False

            def get_position_mode(self):  # type: ignore[no-untyped-def]
                snapshot = super().get_position_mode()
                if not self.injected:
                    self.injected = True
                    late_order = make_local_order(
                        local_order_id="late-ack-unknown",
                        local_state="ack_unknown",
                        exchange_status="unknown",
                    )
                    with ledger.unit_of_work() as uow:
                        uow.orders.add(late_order)
                        uow.commit()
                return snapshot

        readiness = ReadinessGate()
        manager = RecoveryManager(
            exchange=LocalMutationAdapter(),
            ledger=ledger,
            readiness=readiness,
            clock=lambda: NOW,
        )

        with self.assertRaisesRegex(
            RecoveryBlockedError,
            "durable local recovery state changed",
        ):
            manager.recover(
                self.harness.request(run_id="run-local-state-race")
            )

        self.assertIs(readiness.state, ReadinessState.DEGRADED)
        with ledger.unit_of_work(immediate=False) as uow:
            checkpoint = uow.recovery_checkpoints.latest_for_strategy("strategy-1")
            late_order = uow.orders.require("late-ack-unknown")
        self.assertEqual(checkpoint["status"], "BLOCKED")
        self.assertEqual(late_order["local_state"], "ack_unknown")

    def test_claim_scope_and_deterministic_identity_are_validated_before_io(self) -> None:
        for mutation in (
            {"account_id": "another-account"},
            {"strategy_id": "another-strategy"},
            {"client_order_id": "dg1_not_the_logical_slot"},
            {"order_type": "STOP"},
            {"price": "50001"},
            {"quantity_contracts": 3},
            {"side": "sell"},
            {"intent": "GRID_SELL"},
            {"position_side": "long"},
            {"reduce_only": 1},
        ):
            with self.subTest(mutation=mutation):
                harness = make_harness()
                try:
                    claim = make_local_order(local_order_id="claim-local")
                    claim.update(mutation)
                    client_id = str(claim["client_order_id"])
                    with self.assertRaises(RecoveryBlockedError):
                        harness.manager.recover(
                            harness.request(
                                run_id="run-invalid-claim",
                                claimable_orders={client_id: claim},
                            )
                        )
                    self.assertEqual(
                        harness.backend.operation_call_count("get_position_mode"),
                        0,
                    )
                finally:
                    harness.close()

    def test_transient_valid_claim_cannot_create_bot_ownership(self) -> None:
        claim = make_local_order(local_order_id="claimed-local-order")
        claim["ownership"] = "EXTERNAL"
        client_id = str(claim["client_order_id"])
        self.harness.backend.seed_order(
            make_exchange_order(client_order_id=client_id)
        )
        request = self.harness.request(
            run_id="run-valid-claim",
            claimable_orders={client_id: claim},
        )
        claim["account_id"] = "mutated-after-request"

        result = self.harness.manager.recover(request)

        self.assertFalse(result.complete)
        self.assertTrue(
            any(blocker.startswith("UNCLAIMED:") for blocker in result.blockers)
        )
        with self.harness.ledger.unit_of_work(immediate=False) as uow:
            stored = uow.orders.get_by_client_order_id(client_id)
        self.assertIsNone(stored)

    def test_delayed_fill_history_blocks_until_rest_trade_is_visible(self) -> None:
        local = self.harness.add_local_order(quantity_contracts=2)
        self.harness.backend.seed_order(
            make_exchange_order(
                client_order_id=local["client_order_id"],
                original_contracts=2,
            )
        )
        self.harness.backend.record_fill(
            SYMBOL,
            local["client_order_id"],
            contracts=1,
            price=Decimal("50000"),
            trade_id="trade-delayed-history",
            # Recovery performs an initial, overlapping, and final replay.
            # Seven hidden read ticks keep the fill absent for two complete
            # recoveries and reveal it during the third.
            visibility_delay_reads=7,
        )

        first = self.harness.manager.recover(
            self.harness.request(run_id="run-delayed-fill-1")
        )
        second = self.harness.manager.recover(
            self.harness.request(run_id="run-delayed-fill-2")
        )

        self.assertFalse(first.complete)
        self.assertFalse(second.complete)
        self.assertTrue(
            any("trade history" in blocker for blocker in second.blockers)
        )
        with self.harness.ledger.unit_of_work(immediate=False) as uow:
            self.assertEqual(
                uow.fills.list_for_account_symbol(ACCOUNT_ID, SYMBOL),
                [],
            )

        third = self.harness.manager.recover(
            self.harness.request(run_id="run-delayed-fill-3")
        )

        self.assertTrue(third.complete)
        self.assertIs(third.state, ReadinessState.READY)
        with self.harness.ledger.unit_of_work(immediate=False) as uow:
            fills = uow.fills.list_for_account_symbol(ACCOUNT_ID, SYMBOL)
        self.assertEqual([item["binance_trade_id"] for item in fills], [
            "trade-delayed-history"
        ])

    def test_second_open_order_observation_detects_a_mixed_snapshot(self) -> None:
        local = self.harness.add_local_order()
        self.harness.backend.seed_order(
            make_exchange_order(client_order_id=local["client_order_id"])
        )

        class DriftingOpenOrdersAdapter(FakeExchangeAdapter):
            def __init__(self) -> None:
                super().__init__(self_backend)
                self.reads = 0

            def get_open_orders(self, symbol=None):  # type: ignore[no-untyped-def]
                self.reads += 1
                if self.reads == 1:
                    return super().get_open_orders(symbol)
                super().get_open_orders(symbol)
                return ()

        self_backend = self.harness.backend
        readiness = ReadinessGate()
        manager = RecoveryManager(
            exchange=DriftingOpenOrdersAdapter(),
            ledger=self.harness.ledger,
            readiness=readiness,
            clock=lambda: NOW,
        )

        result = manager.recover(
            self.harness.request(run_id="run-mixed-snapshot")
        )

        self.assertFalse(result.complete)
        self.assertIn("OPEN_ORDERS_CHANGED_DURING_RECOVERY", result.blockers)

    def test_tail_replay_catches_offsetting_external_fills(self) -> None:
        backend = self.harness.backend
        backend.set_position(make_position(0))

        class TailFillAdapter(FakeExchangeAdapter):
            def __init__(self):
                super().__init__(backend)
                self.mode_reads = 0

            def get_position_mode(self):  # type: ignore[no-untyped-def]
                snapshot = super().get_position_mode()
                self.mode_reads += 1
                if self.mode_reads == 3:
                    buy = make_exchange_order(
                        client_order_id="manual-tail-buy",
                        exchange_order_id="tail-buy-id",
                        original_contracts=1,
                        side=Side.BUY,
                    )
                    sell = make_exchange_order(
                        client_order_id="manual-tail-sell",
                        exchange_order_id="tail-sell-id",
                        original_contracts=1,
                        side=Side.SELL,
                    )
                    backend.seed_order(buy)
                    backend.seed_order(sell)
                    backend.record_fill(
                        SYMBOL,
                        buy.client_order_id,
                        contracts=1,
                        price=Decimal("50000"),
                        trade_id="tail-buy",
                    )
                    backend.record_fill(
                        SYMBOL,
                        sell.client_order_id,
                        contracts=1,
                        price=Decimal("50000"),
                        trade_id="tail-sell",
                    )
                return snapshot

        manager = RecoveryManager(
            exchange=TailFillAdapter(),
            ledger=self.harness.ledger,
            readiness=ReadinessGate(),
            clock=lambda: NOW,
        )

        result = manager.recover(
            self.harness.request(run_id="run-tail-offsetting-fills")
        )

        self.assertTrue(result.complete, result.blockers)
        self.assertEqual(
            {fill.trade_id for fill in result.report.fills},
            {"tail-buy", "tail-sell"},
        )
        with self.harness.ledger.unit_of_work(immediate=False) as uow:
            checkpoint = uow.recovery_checkpoints.require(result.checkpoint_id)
            strategy_fills = uow.fills.list_for_account_symbol(ACCOUNT_ID, SYMBOL)
            observations = uow.exchange_trade_observations.list_all()
        self.assertEqual(checkpoint["status"], "COMPLETE")
        self.assertEqual(strategy_fills, [])
        self.assertEqual(
            {item["binance_trade_id"] for item in observations},
            {"tail-buy", "tail-sell"},
        )

    def test_closing_replay_fill_is_compared_with_post_closing_facts(self) -> None:
        backend = self.harness.backend

        class ClosingFillAdapter(FakeExchangeAdapter):
            def __init__(self) -> None:
                super().__init__(backend)
                self.trade_reads = 0

            def get_user_trades(self, *args, **kwargs):  # type: ignore[no-untyped-def]
                self.trade_reads += 1
                if self.trade_reads == 3:
                    order = make_exchange_order(
                        client_order_id="manual-closing-buy",
                        exchange_order_id="closing-buy-id",
                        original_contracts=1,
                    )
                    backend.seed_order(order)
                    backend.record_fill(
                        SYMBOL,
                        order.client_order_id,
                        contracts=1,
                        price=Decimal("50000"),
                        trade_id="closing-buy",
                    )
                return super().get_user_trades(*args, **kwargs)

        manager = RecoveryManager(
            exchange=ClosingFillAdapter(),
            ledger=self.harness.ledger,
            readiness=ReadinessGate(),
            clock=lambda: NOW,
        )

        result = manager.recover(
            self.harness.request(run_id="run-closing-single-fill")
        )

        self.assertFalse(result.complete)
        self.assertIs(result.state, ReadinessState.DEGRADED)
        self.assertIn(
            "POSITIONS_CHANGED_DURING_CLOSING_REPLAY",
            result.blockers,
        )
        self.assertTrue(
            any(blocker.startswith("POSITION_MISMATCH:") for blocker in result.blockers)
        )
        self.assertEqual(result.report.positions[0].exchange_contracts, 1)
        with self.harness.ledger.unit_of_work(immediate=False) as uow:
            checkpoint = uow.recovery_checkpoints.require(result.checkpoint_id)
            position = uow.positions.get(ACCOUNT_ID, SYMBOL)
            observations = uow.exchange_trade_observations.list_all()
        self.assertEqual(checkpoint["status"], "BLOCKED")
        self.assertEqual(position["quantity_contracts"], 1)
        self.assertEqual(
            {item["binance_trade_id"] for item in observations},
            {"closing-buy"},
        )

    def test_complete_snapshot_zeroes_stale_position_side_cache_rows(self) -> None:
        with self.harness.ledger.unit_of_work() as uow:
            for side, contracts in (("long", 3), ("short", -2)):
                cached = local_position(contracts)
                cached["position_side"] = side
                uow.positions.upsert(cached)
            uow.commit()

        result = self.harness.manager.recover(
            self.harness.request(run_id="run-zero-stale-position-sides")
        )

        self.assertTrue(result.complete)
        with self.harness.ledger.unit_of_work(immediate=False) as uow:
            cached = {
                item["position_side"]: item["quantity_contracts"]
                for item in uow.positions.list_for_account(ACCOUNT_ID)
                if item["symbol"] == SYMBOL
            }
        self.assertEqual(cached, {"both": 0, "long": 0, "short": 0})

    def test_second_margin_observation_detects_available_balance_drift(self) -> None:
        snapshots = (
            MarginAccountSnapshot(
                balances=(
                    ExchangeMarginBalance(
                        asset="BTC",
                        wallet_balance=Decimal("1"),
                        available_balance=Decimal("0.8"),
                        unrealized_pnl=Decimal("0"),
                        update_time=NOW,
                    ),
                ),
                observed_at=NOW,
                total_wallet_balance=Decimal("1"),
                total_unrealized_pnl=Decimal("0"),
                available_balance=Decimal("0.8"),
            ),
            MarginAccountSnapshot(
                balances=(
                    ExchangeMarginBalance(
                        asset="BTC",
                        wallet_balance=Decimal("1"),
                        available_balance=Decimal("0.7"),
                        unrealized_pnl=Decimal("0"),
                        update_time=NOW,
                    ),
                ),
                observed_at=NOW,
                total_wallet_balance=Decimal("1"),
                total_unrealized_pnl=Decimal("0"),
                available_balance=Decimal("0.7"),
            ),
        )

        class DriftingMarginAdapter(FakeExchangeAdapter):
            def __init__(self) -> None:
                super().__init__(self_backend)
                self.reads = 0

            def get_margin_account_snapshot(self):  # type: ignore[no-untyped-def]
                value = snapshots[min(self.reads, 1)]
                self.reads += 1
                return value

        self_backend = self.harness.backend
        manager = RecoveryManager(
            exchange=DriftingMarginAdapter(),
            ledger=self.harness.ledger,
            readiness=ReadinessGate(),
            clock=lambda: NOW,
        )

        result = manager.recover(
            self.harness.request(run_id="run-margin-drift")
        )

        self.assertFalse(result.complete)
        self.assertIn("MARGIN_ACCOUNT_CHANGED_DURING_RECOVERY", result.blockers)

    def test_fill_without_client_id_is_classified_by_exact_exchange_order_id(self) -> None:
        external = make_exchange_order(
            client_order_id="manual-order",
            exchange_order_id="91001",
            original_contracts=1,
        )
        self.harness.backend.seed_order(external)
        self.harness.backend.record_fill(
            SYMBOL,
            external.client_order_id,
            contracts=1,
            price=Decimal("50000"),
            trade_id="manual-trade",
        )
        self.harness.add_local_position(1)

        class ClientIdOmittingAdapter(FakeExchangeAdapter):
            def get_user_trades(self, *args, **kwargs):  # type: ignore[no-untyped-def]
                page = super().get_user_trades(*args, **kwargs)
                return replace(
                    page,
                    items=tuple(
                        replace(item, client_order_id=None) for item in page.items
                    ),
                )

        readiness = ReadinessGate()
        manager = RecoveryManager(
            exchange=ClientIdOmittingAdapter(self.harness.backend),
            ledger=self.harness.ledger,
            readiness=readiness,
            clock=lambda: NOW,
        )

        result = manager.recover(
            self.harness.request(run_id="run-exchange-id-resolution")
        )

        # The manual fill is exchange truth but is not owned strategy
        # execution.  The strategy projection therefore stays at zero and
        # must not adopt the cached/exchange position implicitly.
        self.assertFalse(result.complete)
        self.assertTrue(
            any("POSITION_MISMATCH" in blocker for blocker in result.blockers)
        )
        self.assertEqual(
            self.harness.backend.operation_call_count("get_order_by_exchange_id"),
            1,
        )
        self.assertEqual(result.report.unmatched_fills[0].client_order_id, None)


if __name__ == "__main__":
    unittest.main()
