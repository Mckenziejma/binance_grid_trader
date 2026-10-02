from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from gridtrader.storage import InvariantViolation, LeaseUnavailable, SQLiteLedger

from .support import NOW, fill_record, new_ledger, order_record, seed_grid


class RecoveryPersistenceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "ledger.sqlite3"

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_committed_strategy_order_fill_position_and_checkpoint_survive_restart(self) -> None:
        ledger = new_ledger(self.db_path)
        with ledger.unit_of_work() as uow:
            seed_grid(uow)
            uow.orders.add(order_record())
            event, _ = uow.events.append({
                "source": "BINANCE_WS",
                "dedupe_key": "trade:BTCUSD_PERP:trade-100",
                "event_type": "order_trade_update",
                "strategy_id": "strategy-1",
                "order_id": "order-1",
                "exchange_event_ms": NOW + 10,
                "received_at_ms": NOW + 11,
                "payload_json": {"trade_id": "trade-100", "status": "partially_filled"},
            })
            fill = fill_record()
            fill["event_id"] = event["event_id"]
            uow.fills.add_idempotent(fill)
            checkpoint = uow.recovery_checkpoints.add({
                "run_id": "run-1",
                "strategy_id": "strategy-1",
                "account_id": "coin-m-main",
                "symbol": "BTCUSD_PERP",
                "reason": "STARTUP",
                "status": "STARTED",
                "started_at_ms": NOW,
            })
            checkpoint = uow.recovery_checkpoints.advance(
                checkpoint["checkpoint_id"],
                status="SNAPSHOT_COMPLETE",
                rest_server_time_ms=NOW + 12,
                open_orders_observed_at_ms=NOW + 13,
                position_observed_at_ms=NOW + 14,
                margin_observed_at_ms=NOW + 15,
            )
            checkpoint = uow.recovery_checkpoints.advance(
                checkpoint["checkpoint_id"],
                status="REPLAY_COMPLETE",
                fills_from_ms=NOW - 1_000,
                fills_through_ms=NOW + 16,
                last_binance_trade_id="trade-100",
            )
            checkpoint = uow.recovery_checkpoints.advance(
                checkpoint["checkpoint_id"],
                status="RECONCILED",
            )
            checkpoint = uow.recovery_checkpoints.complete(
                checkpoint["checkpoint_id"],
                status="COMPLETE",
                completed_at_ms=NOW + 20,
                orders_seen=1,
                fills_seen=1,
                mismatch_count=0,
            )
            uow.positions.upsert({
                "account_id": "coin-m-main",
                "symbol": "BTCUSD_PERP",
                "position_side": "both",
                "quantity_contracts": 1,
                "entry_price": "50000",
                "break_even_price": "50001",
                "mark_price": "50100",
                "unrealized_pnl": "0.00001",
                "leverage": 2,
                "margin_type": "cross",
                "exchange_update_ms": NOW + 18,
                "observed_at_ms": NOW + 19,
                "checkpoint_id": checkpoint["checkpoint_id"],
                "source": "REST",
            })
            uow.commit()

        restarted = SQLiteLedger(self.db_path)
        restarted.initialize()
        with restarted.unit_of_work(immediate=False) as uow:
            self.assertEqual("BTCUSD_PERP", uow.strategies.require("strategy-1")["symbol"])
            self.assertEqual(
                order_record()["client_order_id"],
                uow.orders.require("order-1")["client_order_id"],
            )
            stored_fill = uow.fills.get_by_trade_id("coin-m-main", "BTCUSD_PERP", "trade-100")
            self.assertIsNotNone(stored_fill)
            self.assertEqual("0.000001", stored_fill["commission"])
            position = uow.positions.get("coin-m-main", "BTCUSD_PERP")
            self.assertEqual(1, position["quantity_contracts"])
            latest = uow.recovery_checkpoints.latest_complete("strategy-1")
            self.assertEqual(checkpoint["checkpoint_id"], latest["checkpoint_id"])

    def test_authoritative_rest_zero_position_can_tombstone_newer_local_update(self) -> None:
        ledger = new_ledger(self.db_path)
        with ledger.unit_of_work() as uow:
            uow.positions.upsert({
                "account_id": "coin-m-main",
                "symbol": "BTCUSD_PERP",
                "position_side": "both",
                "quantity_contracts": 2,
                "exchange_update_ms": NOW + 10_000,
                "observed_at_ms": NOW,
                "source": "USER_STREAM",
            })
            zero = uow.positions.upsert({
                "account_id": "coin-m-main",
                "symbol": "BTCUSD_PERP",
                "position_side": "both",
                "quantity_contracts": 0,
                # Binance may report updateTime=0 for an inactive COIN-M
                # position.  A later complete REST snapshot is still the
                # authoritative zero-position tombstone.
                "exchange_update_ms": 0,
                "observed_at_ms": NOW + 1,
                "source": "REST",
            })
            uow.commit()

        self.assertEqual(zero["quantity_contracts"], 0)
        self.assertEqual(zero["source"], "REST")

    def test_strategy_lease_uses_fencing_token_across_process_runs(self) -> None:
        ledger = new_ledger(self.db_path)
        with ledger.unit_of_work() as uow:
            seed_grid(uow)
            uow.bot_runs.add({
                "run_id": "run-1", "instance_id": "instance-a", "status": "RUNNING",
                "started_at_ms": NOW, "heartbeat_at_ms": NOW,
            })
            uow.bot_runs.add({
                "run_id": "run-2", "instance_id": "instance-b", "status": "STARTING",
                "started_at_ms": NOW, "heartbeat_at_ms": NOW,
            })
            first = uow.strategy_leases.acquire(
                strategy_id="strategy-1", run_id="run-1", now_ms=NOW,
                expires_at_ms=NOW + 100,
            )
            self.assertEqual(1, first["fencing_token"])
            with self.assertRaises(LeaseUnavailable):
                uow.strategy_leases.acquire(
                    strategy_id="strategy-1", run_id="run-2", now_ms=NOW + 50,
                    expires_at_ms=NOW + 150,
                )
            second = uow.strategy_leases.acquire(
                strategy_id="strategy-1", run_id="run-2", now_ms=NOW + 101,
                expires_at_ms=NOW + 201,
            )
            self.assertEqual(2, second["fencing_token"])
            uow.strategy_leases.release(
                strategy_id="strategy-1",
                run_id="run-2",
                fencing_token=2,
            )
            third = uow.strategy_leases.acquire(
                strategy_id="strategy-1",
                run_id="run-1",
                now_ms=NOW + 102,
                expires_at_ms=NOW + 202,
            )
            self.assertEqual(3, third["fencing_token"])
            uow.commit()

    def test_strategy_lease_renewal_requires_exact_live_fencing_token(self) -> None:
        ledger = new_ledger(self.db_path)
        with ledger.unit_of_work() as uow:
            seed_grid(uow)
            for run_id in ("run-owner", "run-takeover"):
                uow.bot_runs.add({
                    "run_id": run_id,
                    "instance_id": f"instance:{run_id}",
                    "status": "RUNNING",
                    "started_at_ms": NOW,
                    "heartbeat_at_ms": NOW,
                })
            first = uow.strategy_leases.acquire(
                strategy_id="strategy-1",
                run_id="run-owner",
                now_ms=NOW,
                expires_at_ms=NOW + 100,
            )
            with self.assertRaises(LeaseUnavailable):
                uow.strategy_leases.acquire(
                    strategy_id="strategy-1",
                    run_id="run-owner",
                    now_ms=NOW + 1,
                    expires_at_ms=NOW + 101,
                )
            renewed = uow.strategy_leases.renew(
                strategy_id="strategy-1",
                run_id="run-owner",
                fencing_token=first["fencing_token"],
                now_ms=NOW + 50,
                expires_at_ms=NOW + 200,
            )
            self.assertEqual(first["fencing_token"], renewed["fencing_token"])
            self.assertEqual(NOW + 200, renewed["expires_at_ms"])
            uow.strategy_leases.require_owned(
                strategy_id="strategy-1",
                run_id="run-owner",
                fencing_token=first["fencing_token"],
                now_ms=NOW + 199,
            )
            with self.assertRaises(LeaseUnavailable):
                uow.strategy_leases.renew(
                    strategy_id="strategy-1",
                    run_id="run-owner",
                    fencing_token=999,
                    now_ms=NOW + 60,
                    expires_at_ms=NOW + 210,
                )
            takeover = uow.strategy_leases.acquire(
                strategy_id="strategy-1",
                run_id="run-takeover",
                now_ms=NOW + 200,
                expires_at_ms=NOW + 300,
            )
            self.assertEqual(first["fencing_token"] + 1, takeover["fencing_token"])
            with self.assertRaises(LeaseUnavailable):
                uow.strategy_leases.require_owned(
                    strategy_id="strategy-1",
                    run_id="run-owner",
                    fencing_token=first["fencing_token"],
                    now_ms=NOW + 200,
                )
            with self.assertRaises(LeaseUnavailable):
                uow.strategy_leases.release(
                    strategy_id="strategy-1",
                    run_id="run-owner",
                    fencing_token=first["fencing_token"],
                )
            uow.commit()

    def test_phase2_replay_requires_explicit_complete_pagination(self) -> None:
        ledger = new_ledger(self.db_path)
        with ledger.unit_of_work() as uow:
            checkpoint = uow.recovery_checkpoints.add({
                "run_id": "run-phase2",
                "strategy_id": None,
                "account_id": "coin-m-main",
                "symbol": "BTCUSD_PERP",
                "reason": "STARTUP",
                "status": "STARTED",
                "recovery_epoch": 1,
                "started_at_ms": NOW,
            })
            checkpoint = uow.recovery_checkpoints.advance(
                checkpoint["checkpoint_id"],
                status="SNAPSHOT_COMPLETE",
                rest_server_time_ms=NOW + 1,
                open_orders_observed_at_ms=NOW + 2,
                position_observed_at_ms=NOW + 3,
                margin_observed_at_ms=NOW + 4,
            )

            with self.assertRaisesRegex(
                InvariantViolation,
                "Phase-2 replay completion requires explicit complete pagination",
            ):
                uow.recovery_checkpoints.advance(
                    checkpoint["checkpoint_id"],
                    status="REPLAY_COMPLETE",
                    fills_through_ms=NOW + 5,
                )

            replayed = uow.recovery_checkpoints.advance(
                checkpoint["checkpoint_id"],
                status="REPLAY_COMPLETE",
                fills_through_ms=NOW + 5,
                trades_complete=True,
            )
            self.assertEqual(1, replayed["trades_complete"])

    def test_phase1_replay_keeps_legacy_completeness_inference(self) -> None:
        ledger = new_ledger(self.db_path)
        with ledger.unit_of_work() as uow:
            checkpoint = uow.recovery_checkpoints.add({
                "run_id": "run-phase1",
                "strategy_id": None,
                "account_id": "coin-m-main",
                "symbol": "BTCUSD_PERP",
                "reason": "STARTUP",
                "status": "STARTED",
                "started_at_ms": NOW,
            })
            checkpoint = uow.recovery_checkpoints.advance(
                checkpoint["checkpoint_id"],
                status="SNAPSHOT_COMPLETE",
                rest_server_time_ms=NOW + 1,
                open_orders_observed_at_ms=NOW + 2,
                position_observed_at_ms=NOW + 3,
                margin_observed_at_ms=NOW + 4,
            )
            checkpoint = uow.recovery_checkpoints.advance(
                checkpoint["checkpoint_id"],
                status="REPLAY_COMPLETE",
                fills_through_ms=NOW + 5,
            )
            self.assertEqual(1, checkpoint["trades_complete"])

    def test_phase2_complete_requires_mode_rules_and_exhausted_cursor(self) -> None:
        ledger = new_ledger(self.db_path)

        def build_reconciled(
            uow: object,
            run_id: str,
            *,
            position_mode: str | None,
            rules_hash: str | None,
            next_trade_cursor: str | None,
        ) -> dict:
            checkpoint = uow.recovery_checkpoints.add({
                "run_id": run_id,
                "strategy_id": None,
                "account_id": "coin-m-main",
                "symbol": "BTCUSD_PERP",
                "reason": "STARTUP",
                "status": "STARTED",
                "recovery_epoch": 1,
                "started_at_ms": NOW,
            })
            checkpoint = uow.recovery_checkpoints.advance(
                checkpoint["checkpoint_id"],
                status="SNAPSHOT_COMPLETE",
                rest_server_time_ms=NOW + 1,
                open_orders_observed_at_ms=NOW + 2,
                position_observed_at_ms=NOW + 3,
                margin_observed_at_ms=NOW + 4,
                snapshot_observed_at_ms=NOW + 4,
                position_mode=position_mode,
                rules_hash=rules_hash,
            )
            checkpoint = uow.recovery_checkpoints.advance(
                checkpoint["checkpoint_id"],
                status="REPLAY_COMPLETE",
                fills_through_ms=NOW + 5,
                trades_complete=True,
                next_trade_cursor=next_trade_cursor,
            )
            return uow.recovery_checkpoints.advance(
                checkpoint["checkpoint_id"],
                status="RECONCILED",
            )

        with ledger.unit_of_work() as uow:
            missing_mode = build_reconciled(
                uow,
                "run-missing-mode",
                position_mode=None,
                rules_hash="rules-a",
                next_trade_cursor=None,
            )
            with self.assertRaisesRegex(InvariantViolation, "position-mode"):
                uow.recovery_checkpoints.complete(
                    missing_mode["checkpoint_id"],
                    status="COMPLETE",
                    completed_at_ms=NOW + 6,
                    orders_seen=0,
                    fills_seen=0,
                    mismatch_count=0,
                )

            open_cursor = build_reconciled(
                uow,
                "run-open-cursor",
                position_mode="one_way",
                rules_hash="rules-a",
                next_trade_cursor="page-2",
            )
            with self.assertRaisesRegex(InvariantViolation, "exhausted trade cursor"):
                uow.recovery_checkpoints.complete(
                    open_cursor["checkpoint_id"],
                    status="COMPLETE",
                    completed_at_ms=NOW + 6,
                    orders_seen=0,
                    fills_seen=0,
                    mismatch_count=0,
                )

            valid = build_reconciled(
                uow,
                "run-valid",
                position_mode="one_way",
                rules_hash="rules-a",
                next_trade_cursor=None,
            )
            completed = uow.recovery_checkpoints.complete(
                valid["checkpoint_id"],
                status="COMPLETE",
                completed_at_ms=NOW + 6,
                orders_seen=0,
                fills_seen=0,
                mismatch_count=0,
            )
            self.assertEqual("COMPLETE", completed["status"])


if __name__ == "__main__":
    unittest.main()
