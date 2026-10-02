from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from gridtrader.core.enums import ReadinessState
from gridtrader.core.readiness import ReadinessGate
from gridtrader.exchange.fake import FakeExchangeAdapter
from gridtrader.exchange.fake_backend import FakeExchangeBackend
from gridtrader.exchange.models import InstrumentRules, PositionMode
from gridtrader.recovery.manager import RecoveryManager, RecoveryRequest
from gridtrader.storage import SQLiteLedger

from tests.unit.storage.support import strategy_record


NOW = datetime(2026, 10, 2, 12, 0, tzinfo=timezone.utc)


def rules() -> InstrumentRules:
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
        max_contracts=1000,
        min_price=Decimal("0.1"),
        max_price=Decimal("1000000"),
        supported_order_types=("LIMIT",),
        observed_at=NOW,
        rules_hash="rules-v1",
    )


class RecoveryManagerSmokeTests(unittest.TestCase):
    def test_empty_authoritative_snapshot_can_reach_ready_idempotently(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            ledger = SQLiteLedger(Path(directory) / "ledger.sqlite3")
            ledger.initialize()
            with ledger.unit_of_work() as uow:
                uow.strategies.add(strategy_record())
                for run_id in ("run-1", "run-2"):
                    uow.bot_runs.add(
                        {
                            "run_id": run_id,
                            "instance_id": f"test:{run_id}",
                            "status": "RUNNING",
                            "started_at_ms": int(NOW.timestamp() * 1_000),
                            "heartbeat_at_ms": int(NOW.timestamp() * 1_000),
                        }
                    )
                uow.commit()
            backend = FakeExchangeBackend("coin-m-main")
            backend.set_instrument_rules(rules())
            gate = ReadinessGate()
            manager = RecoveryManager(
                exchange=FakeExchangeAdapter(backend),
                ledger=ledger,
                readiness=gate,
                clock=lambda: NOW,
            )
            request = RecoveryRequest(
                run_id="run-1",
                strategy_id="strategy-1",
                account_id="coin-m-main",
                symbol="BTCUSD_PERP",
                expected_position_mode=PositionMode.ONE_WAY,
            )
            first = manager.recover(request)
            self.assertTrue(first.complete)
            self.assertIs(first.state, ReadinessState.READY)

            second = manager.recover(
                RecoveryRequest(
                    **{**request.__dict__, "run_id": "run-2"}
                )
            )
            self.assertTrue(second.complete)
            with ledger.unit_of_work(immediate=False) as uow:
                self.assertEqual(1, len(uow.instrument_rules.list_all()))
                self.assertEqual(2, len(uow.recovery_checkpoints.list_all()))


if __name__ == "__main__":
    unittest.main()
