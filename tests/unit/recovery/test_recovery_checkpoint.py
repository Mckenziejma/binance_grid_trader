from __future__ import annotations

import unittest
from dataclasses import replace
from decimal import Decimal

from gridtrader.core.enums import ReadinessState
from gridtrader.core.readiness import ReadinessGate
from gridtrader.exchange.fake import FakeExchangeAdapter
from gridtrader.recovery.manager import RecoveryManager
from gridtrader.storage import SQLiteLedger

from .support import NOW, SYMBOL, make_exchange_order, make_harness, make_rules


class RecoveryCheckpointTests(unittest.TestCase):
    def setUp(self) -> None:
        self.harness = make_harness()

    def tearDown(self) -> None:
        self.harness.close()

    def test_complete_snapshot_and_evidence_commit_atomically(self) -> None:
        result = self.harness.manager.recover(self.harness.request())

        self.assertTrue(result.complete)
        self.assertIs(result.state, ReadinessState.READY)

        restarted = SQLiteLedger(self.harness.db_path)
        restarted.initialize()
        with restarted.unit_of_work(immediate=False) as uow:
            checkpoint = uow.recovery_checkpoints.require(result.checkpoint_id)
            observations = uow.exchange_observations.list_all()
            rules = uow.instrument_rules.list_all()
            modes = uow.position_mode_observations.list_all()

        self.assertEqual(checkpoint["status"], "COMPLETE")
        self.assertEqual(checkpoint["recovery_epoch"], result.recovery_epoch)
        self.assertEqual(checkpoint["trades_complete"], 1)
        self.assertEqual(
            checkpoint["rules_hash"],
            RecoveryManager._semantic_rules_hash(make_rules()),
        )
        self.assertEqual(checkpoint["position_mode"], "one_way")
        self.assertEqual(checkpoint["mismatch_count"], 0)
        self.assertEqual(len(observations), 6)
        self.assertEqual(len(rules), 1)
        self.assertEqual(len(modes), 1)

    def test_changed_instrument_rules_block_a_later_recovery(self) -> None:
        first = self.harness.manager.recover(
            self.harness.request(run_id="run-rules-v1")
        )
        self.assertTrue(first.complete)
        self.harness.backend.set_instrument_rules(
            replace(
                make_rules(rules_hash="rules-v2"),
                contract_size=Decimal("101"),
            )
        )

        second = self.harness.manager.recover(
            self.harness.request(run_id="run-rules-v2")
        )

        self.assertFalse(second.complete)
        self.assertIs(second.state, ReadinessState.DEGRADED)
        self.assertIn("INSTRUMENT_RULES_CHANGED", second.blockers)
        with self.harness.ledger.unit_of_work(immediate=False) as uow:
            checkpoint = uow.recovery_checkpoints.require(second.checkpoint_id)
        self.assertEqual(checkpoint["status"], "BLOCKED")

    def test_first_recovery_blocks_grid_incompatible_with_current_tick(self) -> None:
        self.harness.backend.set_instrument_rules(
            replace(
                make_rules(rules_hash="rules-tick-incompatible"),
                price_tick=Decimal("0.3"),
            )
        )

        result = self.harness.manager.recover(
            self.harness.request(run_id="run-first-incompatible-tick")
        )

        self.assertFalse(result.complete)
        self.assertIs(result.state, ReadinessState.DEGRADED)
        self.assertTrue(
            any(
                blocker.startswith("INSTRUMENT_RULES_INCOMPATIBLE:")
                and "price_tick" in blocker
                for blocker in result.blockers
            )
        )

    def test_first_recovery_blocks_grid_incompatible_with_contract_filters(self) -> None:
        self.harness.backend.set_instrument_rules(
            replace(
                make_rules(rules_hash="rules-quantity-incompatible"),
                contract_step=3,
                min_contracts=3,
                max_contracts=30,
            )
        )

        result = self.harness.manager.recover(
            self.harness.request(run_id="run-first-incompatible-contracts")
        )

        self.assertFalse(result.complete)
        self.assertTrue(
            any(
                blocker.startswith("INSTRUMENT_RULES_INCOMPATIBLE:")
                and (
                    "contract_step" in blocker
                    or "min_contracts" in blocker
                )
                for blocker in result.blockers
            )
        )

    def test_symbol_wide_rule_history_is_not_another_strategy_checkpoint(self) -> None:
        old_rules = replace(
            make_rules(rules_hash="rules-observed-by-another-strategy"),
            contract_size=Decimal("99"),
        )
        with self.harness.ledger.unit_of_work() as uow:
            uow.instrument_rules.record_observation(
                RecoveryManager._rules_record(
                    old_rules,
                    int(NOW.timestamp() * 1_000) - 1,
                )
            )
            uow.commit()
        self.harness.backend.set_instrument_rules(
            make_rules(rules_hash="rules-current-first-checkpoint")
        )

        result = self.harness.manager.recover(
            self.harness.request(run_id="run-first-strategy-rules-baseline")
        )

        self.assertTrue(result.complete)
        self.assertNotIn("INSTRUMENT_RULES_CHANGED", result.blockers)
        with self.harness.ledger.unit_of_work(immediate=False) as uow:
            hashes = {item["rules_hash"] for item in uow.instrument_rules.list_all()}
        self.assertEqual(
            hashes,
            {
                RecoveryManager._semantic_rules_hash(old_rules),
                RecoveryManager._semantic_rules_hash(
                    make_rules(rules_hash="rules-current-first-checkpoint")
                ),
            },
        )

    def test_missing_provider_hash_cannot_hide_mid_recovery_rule_drift(self) -> None:
        initial = replace(make_rules(), rules_hash=None)
        changed = replace(
            initial,
            contract_size=Decimal("101"),
            observed_at=NOW,
        )
        backend = self.harness.backend

        class DriftingRulesAdapter(FakeExchangeAdapter):
            def __init__(self) -> None:
                super().__init__(backend)
                self.rule_reads = 0

            def get_instrument_rules(self, symbol):  # type: ignore[no-untyped-def]
                super().get_instrument_rules(symbol)
                self.rule_reads += 1
                return initial if self.rule_reads == 1 else changed

        manager = RecoveryManager(
            exchange=DriftingRulesAdapter(),
            ledger=self.harness.ledger,
            readiness=ReadinessGate(),
            clock=lambda: NOW,
        )

        result = manager.recover(
            self.harness.request(run_id="run-rules-without-provider-hash")
        )

        self.assertFalse(result.complete)
        self.assertIn("INSTRUMENT_RULES_CHANGED_DURING_RECOVERY", result.blockers)

    def test_phase1_complete_checkpoint_is_not_a_trusted_trade_watermark(self) -> None:
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
            trade_id="trade-before-legacy-watermark",
            trade_time=NOW,
        )
        self.harness.add_local_position(1)
        with self.harness.ledger.unit_of_work() as uow:
            assert uow.connection is not None
            uow.connection.execute(
                """
                INSERT INTO recovery_checkpoints(
                    run_id, strategy_id, account_id, symbol, reason, status,
                    fills_from_ms, fills_through_ms, started_at_ms, completed_at_ms
                ) VALUES (?, ?, ?, ?, 'STARTUP', 'COMPLETE', ?, ?, ?, ?)
                """,
                (
                    "legacy-phase1-run",
                    "strategy-1",
                    "coin-m-main",
                    SYMBOL,
                    int(NOW.timestamp() * 1000),
                    int(NOW.timestamp() * 1000) + 10_000,
                    int(NOW.timestamp() * 1000),
                    int(NOW.timestamp() * 1000) + 10_000,
                ),
            )
            uow.commit()

        result = self.harness.manager.recover(
            self.harness.request(run_id="run-after-phase1-checkpoint")
        )

        self.assertTrue(result.complete)
        with self.harness.ledger.unit_of_work(immediate=False) as uow:
            fills = uow.fills.list_for_account_symbol("coin-m-main", SYMBOL)
        self.assertEqual(
            [record["binance_trade_id"] for record in fills],
            ["trade-before-legacy-watermark"],
        )


if __name__ == "__main__":
    unittest.main()
