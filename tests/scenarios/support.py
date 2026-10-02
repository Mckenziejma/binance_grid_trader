"""Shared fixtures for durable, offline Phase 2 recovery scenarios."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

from gridtrader.core.clock import Clock
from gridtrader.core.enums import ExchangeOrderStatus, PositionSide, Side
from gridtrader.core.readiness import ReadinessGate
from gridtrader.exchange.fake import FakeExchangeAdapter
from gridtrader.exchange.fake_backend import FakeExchangeBackend
from gridtrader.exchange.models import ExchangeOrderSnapshot, InstrumentRules, PositionMode
from gridtrader.orders.idempotency import logical_slot_key_text, make_client_order_id
from gridtrader.recovery.manager import RecoveryManager, RecoveryRequest
from gridtrader.storage import SQLiteLedger


UTC_NOW = datetime(2026, 10, 2, 12, 0, tzinfo=timezone.utc)
NOW_MS = int(UTC_NOW.timestamp() * 1_000)

ACCOUNT_ID = "coin-m-main"
STRATEGY_ID = "strategy-1"
GENERATION_ID = "generation-1"
LEVEL_ID = "level-1"
LOCAL_ORDER_ID = "order-1"
SYMBOL = "BTCUSD_PERP"
EXCHANGE_ORDER_ID = "90001"
CLIENT_ORDER_ID = make_client_order_id(
    STRATEGY_ID,
    GENERATION_ID,
    LEVEL_ID,
    0,
    "grid_buy",
)


class FixedClock(Clock):
    """One clock shared by the fake exchange and each rebuilt runtime."""

    def __init__(self, value: datetime = UTC_NOW) -> None:
        self.value = value

    def now(self) -> datetime:
        return self.value


@dataclass
class RuntimeStack:
    """Process-local objects that must not survive a simulated restart."""

    ledger: SQLiteLedger
    adapter: FakeExchangeAdapter
    readiness: ReadinessGate
    manager: RecoveryManager


def make_rules() -> InstrumentRules:
    return InstrumentRules(
        symbol=SYMBOL,
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
        min_price=Decimal("0.1"),
        max_price=Decimal("1000000"),
        supported_order_types=("LIMIT",),
        observed_at=UTC_NOW,
        rules_hash="btc-rules-v1",
    )


def make_backend(
    clock: FixedClock,
    *,
    default_page_size: int = 100,
) -> FakeExchangeBackend:
    backend = FakeExchangeBackend(
        ACCOUNT_ID,
        clock=clock,
        default_page_size=default_page_size,
    )
    backend.set_instrument_rules(make_rules())
    backend.set_position_mode(PositionMode.ONE_WAY, observed_at=clock.now())
    return backend


def make_exchange_order(*, contracts: int) -> ExchangeOrderSnapshot:
    return ExchangeOrderSnapshot(
        symbol=SYMBOL,
        client_order_id=CLIENT_ORDER_ID,
        exchange_order_id=EXCHANGE_ORDER_ID,
        status=ExchangeOrderStatus.NEW,
        side=Side.BUY,
        position_side=PositionSide.BOTH,
        original_contracts=contracts,
        filled_contracts=0,
        update_time=UTC_NOW,
        price=Decimal("65000"),
    )


def new_ledger(path: Path) -> SQLiteLedger:
    ledger = SQLiteLedger(path)
    ledger.initialize()
    return ledger


def build_runtime(
    path: Path,
    backend: FakeExchangeBackend,
    clock: FixedClock,
) -> RuntimeStack:
    ledger = new_ledger(path)
    adapter = FakeExchangeAdapter(backend)
    readiness = ReadinessGate()
    manager = RecoveryManager(
        exchange=adapter,
        ledger=ledger,
        readiness=readiness,
        clock=clock.now,
    )
    return RuntimeStack(
        ledger=ledger,
        adapter=adapter,
        readiness=readiness,
        manager=manager,
    )


def make_request(run_id: str, *, reason: str = "STARTUP") -> RecoveryRequest:
    return RecoveryRequest(
        run_id=run_id,
        strategy_id=STRATEGY_ID,
        account_id=ACCOUNT_ID,
        symbol=SYMBOL,
        expected_position_mode=PositionMode.ONE_WAY,
        reason=reason,
    )


def register_run(
    ledger: SQLiteLedger,
    run_id: str,
    *,
    instance_id: str | None = None,
) -> None:
    with ledger.unit_of_work() as uow:
        if uow.bot_runs.get(run_id) is None:
            uow.bot_runs.add(
                {
                    "run_id": run_id,
                    "instance_id": instance_id or f"scenario:{run_id}",
                    "status": "RUNNING",
                    "started_at_ms": NOW_MS,
                    "heartbeat_at_ms": NOW_MS,
                }
            )
        uow.commit()


def seed_strategy_and_order(
    ledger: SQLiteLedger,
    *,
    contracts: int,
    local_state: str = "active",
) -> None:
    exchange_status = "unknown" if local_state == "ack_unknown" else "new"
    with ledger.unit_of_work() as uow:
        uow.strategies.add(
            {
                "strategy_id": STRATEGY_ID,
                "account_id": ACCOUNT_ID,
                "name": "btc-neutral-recovery-scenario",
                "symbol": SYMBOL,
                "market_type": "COIN_M_PERP",
                "mode": "neutral",
                "spacing_mode": "arithmetic",
                "initial_position_contracts": 0,
                "client_id_namespace": "scenario",
                "status": "CREATED",
                "config_revision": 1,
                "created_at_ms": NOW_MS,
                "updated_at_ms": NOW_MS,
            }
        )
        uow.grid_generations.add(
            {
                "generation_id": GENERATION_ID,
                "strategy_id": STRATEGY_ID,
                "generation_no": 1,
                "lower_price": "50000",
                "upper_price": "70000",
                "logical_level_count": 1,
                "strategy_mode": "neutral",
                "spacing_mode": "arithmetic",
                "arithmetic_step": "100",
                "geometric_ratio": None,
                "order_contracts": contracts,
                "max_active_orders": 1,
                "status": "draft",
                "created_at_ms": NOW_MS,
                "activated_at_ms": None,
            }
        )
        uow.grid_levels.add(
            {
                "level_id": LEVEL_ID,
                "generation_id": GENERATION_ID,
                "level_index": 0,
                "price": "65000",
                "planned_contracts": contracts,
                "state": "armed",
                "cycle_no": 0,
                "version": 0,
                "updated_at_ms": NOW_MS,
            }
        )
        uow.grid_generations.activate(GENERATION_ID, NOW_MS)
        uow.orders.add(
            {
                "local_order_id": LOCAL_ORDER_ID,
                "strategy_id": STRATEGY_ID,
                "generation_id": GENERATION_ID,
                "level_id": LEVEL_ID,
                "account_id": ACCOUNT_ID,
                "symbol": SYMBOL,
                "logical_slot_key": logical_slot_key_text(
                    STRATEGY_ID,
                    GENERATION_ID,
                    LEVEL_ID,
                    0,
                    "grid_buy",
                ),
                "cycle_no": 0,
                "leg_role": "grid_buy",
                "attempt_no": 1,
                "ownership": "BOT",
                "intent": "GRID_BUY",
                "client_order_id": CLIENT_ORDER_ID,
                "side": "buy",
                "position_side": "both",
                "order_type": "LIMIT",
                "time_in_force": "GTC",
                "reduce_only": 0,
                "price": "65000",
                "quantity_contracts": contracts,
                "local_state": local_state,
                "exchange_status": exchange_status,
                "cumulative_filled_contracts": 0,
                "submitted_at_ms": NOW_MS,
                "created_at_ms": NOW_MS,
                "updated_at_ms": NOW_MS,
            }
        )
        uow.commit()


def seed_local_position(ledger: SQLiteLedger, *, contracts: int) -> None:
    with ledger.unit_of_work() as uow:
        uow.positions.upsert(
            {
                "account_id": ACCOUNT_ID,
                "symbol": SYMBOL,
                "position_side": "both",
                "quantity_contracts": contracts,
                "entry_price": "65000" if contracts else "0",
                "mark_price": "65000" if contracts else "0",
                "unrealized_pnl": "0",
                "leverage": 1,
                "margin_type": "cross",
                "margin_asset": "BTC",
                "isolated": 0,
                "exchange_update_ms": NOW_MS - 1,
                "observed_at_ms": NOW_MS - 1,
                "source": "REST",
            }
        )
        uow.commit()


def rows(repository: Any) -> list[dict[str, Any]]:
    return [dict(item) for item in repository.list_all()]
