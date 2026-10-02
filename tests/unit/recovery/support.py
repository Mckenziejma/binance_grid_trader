"""Shared deterministic fixtures for recovery unit tests."""

from __future__ import annotations

import tempfile
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from threading import Event, Lock
from typing import Any

from gridtrader.core.clock import Clock
from gridtrader.core.enums import ExchangeOrderStatus, PositionSide, Side
from gridtrader.core.readiness import ReadinessGate
from gridtrader.exchange.fake import FakeExchangeAdapter
from gridtrader.exchange.fake_backend import FakeExchangeBackend
from gridtrader.exchange.models import (
    ExchangeFill,
    ExchangeOrderSnapshot,
    ExchangePosition,
    InstrumentRules,
    PositionMode,
    PositionModeSnapshot,
)
from gridtrader.recovery.manager import RecoveryManager, RecoveryRequest
from gridtrader.storage import SQLiteLedger
from tests.unit.storage.support import (
    NOW as STORAGE_NOW_MS,
    new_ledger,
    order_record,
    seed_grid,
)


ACCOUNT_ID = "coin-m-main"
RUN_ID = "run-recovery-1"
STRATEGY_ID = "strategy-1"
SYMBOL = "BTCUSD_PERP"
NOW = datetime.fromtimestamp(STORAGE_NOW_MS / 1_000, tz=timezone.utc)


class FixedClock(Clock):
    def __init__(self, value: datetime = NOW) -> None:
        self.value = value

    def now(self) -> datetime:
        return self.value


class BlockingFakeExchangeAdapter(FakeExchangeAdapter):
    """Block a selected position-mode read so concurrency fences are testable."""

    def __init__(
        self,
        backend: FakeExchangeBackend,
        *,
        block_on_call: int = 1,
    ) -> None:
        super().__init__(backend)
        if block_on_call < 1:
            raise ValueError("block_on_call must be positive")
        self.entered = Event()
        self.release = Event()
        self._block_guard = Lock()
        self._block_on_call = block_on_call
        self._mode_read_count = 0

    def get_position_mode(self) -> PositionModeSnapshot:
        with self._block_guard:
            self._mode_read_count += 1
            should_block = self._mode_read_count == self._block_on_call
        if should_block:
            self.entered.set()
            if not self.release.wait(timeout=5):
                raise TimeoutError("test did not release the blocked recovery read")
        return super().get_position_mode()


def make_rules(
    *,
    rules_hash: str = "rules-v1",
    observed_at: datetime = NOW,
) -> InstrumentRules:
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
        supported_order_types=("LIMIT",),
        observed_at=observed_at,
        rules_hash=rules_hash,
    )


def make_local_order(**overrides: Any) -> dict[str, Any]:
    record = order_record()
    record.update(overrides)
    return record


def make_exchange_order(
    *,
    client_order_id: str | None = None,
    exchange_order_id: str = "90001",
    status: ExchangeOrderStatus = ExchangeOrderStatus.NEW,
    original_contracts: int = 2,
    filled_contracts: int = 0,
    side: Side = Side.BUY,
    position_side: PositionSide = PositionSide.BOTH,
    update_time: datetime = NOW,
    order_type: str = "LIMIT",
    time_in_force: str = "GTC",
) -> ExchangeOrderSnapshot:
    normalized_client_id = client_order_id or make_local_order()["client_order_id"]
    return ExchangeOrderSnapshot(
        symbol=SYMBOL,
        client_order_id=normalized_client_id,
        exchange_order_id=exchange_order_id,
        status=status,
        side=side,
        position_side=position_side,
        original_contracts=original_contracts,
        filled_contracts=filled_contracts,
        update_time=update_time,
        price=Decimal("50000"),
        average_fill_price=(
            Decimal("50000") if filled_contracts else None
        ),
        order_type=order_type,
        time_in_force=time_in_force,
    )


def make_fill(
    trade_id: str,
    *,
    client_order_id: str | None = None,
    exchange_order_id: str = "90001",
    contracts: int = 1,
    price: Decimal = Decimal("50000"),
    seconds: int = 0,
) -> ExchangeFill:
    return ExchangeFill(
        account_id=ACCOUNT_ID,
        symbol=SYMBOL,
        trade_id=trade_id,
        exchange_order_id=exchange_order_id,
        client_order_id=client_order_id or make_local_order()["client_order_id"],
        side=Side.BUY,
        position_side=PositionSide.BOTH,
        price=price,
        contracts=contracts,
        realized_pnl=Decimal("0"),
        commission=Decimal("0.000001"),
        commission_asset="BTC",
        trade_time=NOW + timedelta(seconds=seconds),
    )


def make_position(
    contracts: int,
    *,
    position_side: PositionSide = PositionSide.BOTH,
    update_time: datetime = NOW,
) -> ExchangePosition:
    return ExchangePosition(
        symbol=SYMBOL,
        position_side=position_side,
        contracts=contracts,
        entry_price=Decimal("50000") if contracts else Decimal("0"),
        mark_price=Decimal("50100"),
        unrealized_pnl=Decimal("0"),
        leverage=1,
        margin_asset="BTC",
        isolated=False,
        update_time=update_time,
    )


def local_position(contracts: int) -> dict[str, Any]:
    return {
        "account_id": ACCOUNT_ID,
        "symbol": SYMBOL,
        "position_side": PositionSide.BOTH.value,
        "quantity_contracts": contracts,
        "entry_price": "50000" if contracts else "0",
        "mark_price": "50100",
        "unrealized_pnl": "0",
        "leverage": 1,
        "margin_type": "cross",
        "margin_asset": "BTC",
        "isolated": 0,
        "exchange_update_ms": STORAGE_NOW_MS,
        "observed_at_ms": STORAGE_NOW_MS,
        "source": "REST",
    }


@dataclass
class RecoveryHarness:
    temp_dir: tempfile.TemporaryDirectory[str]
    db_path: Path
    ledger: SQLiteLedger
    clock: FixedClock
    backend: FakeExchangeBackend
    exchange: FakeExchangeAdapter
    readiness: ReadinessGate
    manager: RecoveryManager

    def close(self) -> None:
        self.temp_dir.cleanup()

    def request(
        self,
        *,
        run_id: str = RUN_ID,
        expected_position_mode: PositionMode = PositionMode.ONE_WAY,
        register_run: bool = True,
        **overrides: Any,
    ) -> RecoveryRequest:
        if register_run:
            self.register_run(run_id)
        values: dict[str, Any] = {
            "run_id": run_id,
            "strategy_id": STRATEGY_ID,
            "account_id": ACCOUNT_ID,
            "symbol": SYMBOL,
            "expected_position_mode": expected_position_mode,
        }
        values.update(overrides)
        return RecoveryRequest(**values)

    def register_run(
        self,
        run_id: str,
        *,
        instance_id: str | None = None,
        status: str = "RUNNING",
    ) -> dict[str, Any]:
        with self.ledger.unit_of_work() as uow:
            existing = uow.bot_runs.get(run_id)
            if existing is None:
                existing = uow.bot_runs.add(
                    {
                        "run_id": run_id,
                        "instance_id": instance_id or f"test:{run_id}",
                        "status": status,
                        "started_at_ms": STORAGE_NOW_MS,
                        "heartbeat_at_ms": STORAGE_NOW_MS,
                    }
                )
            uow.commit()
        return dict(existing)

    def add_local_order(self, **overrides: Any) -> dict[str, Any]:
        record = make_local_order(**overrides)
        with self.ledger.unit_of_work() as uow:
            stored = uow.orders.add(record)
            uow.commit()
        return stored

    def add_local_position(self, contracts: int) -> dict[str, Any]:
        with self.ledger.unit_of_work() as uow:
            stored = uow.positions.upsert(local_position(contracts))
            uow.commit()
        return stored


def make_harness(
    *,
    default_page_size: int = 100,
    position_mode: PositionMode = PositionMode.ONE_WAY,
) -> RecoveryHarness:
    temp_dir = tempfile.TemporaryDirectory()
    db_path = Path(temp_dir.name) / "recovery.sqlite3"
    ledger = new_ledger(db_path)
    with ledger.unit_of_work() as uow:
        seed_grid(uow)
        uow.commit()

    clock = FixedClock()
    backend = FakeExchangeBackend(
        ACCOUNT_ID,
        clock=clock,
        default_page_size=default_page_size,
    )
    backend.set_instrument_rules(make_rules())
    backend.set_position_mode(position_mode, observed_at=NOW)
    exchange = FakeExchangeAdapter(backend)
    readiness = ReadinessGate()
    manager = RecoveryManager(
        exchange=exchange,
        ledger=ledger,
        readiness=readiness,
        clock=lambda: NOW,
    )
    return RecoveryHarness(
        temp_dir=temp_dir,
        db_path=db_path,
        ledger=ledger,
        clock=clock,
        backend=backend,
        exchange=exchange,
        readiness=readiness,
        manager=manager,
    )


__all__ = [
    "ACCOUNT_ID",
    "BlockingFakeExchangeAdapter",
    "FixedClock",
    "NOW",
    "RecoveryHarness",
    "RUN_ID",
    "STORAGE_NOW_MS",
    "STRATEGY_ID",
    "SYMBOL",
    "local_position",
    "make_exchange_order",
    "make_fill",
    "make_harness",
    "make_local_order",
    "make_position",
    "make_rules",
]
