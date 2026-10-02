"""Test records for the isolated storage unit tests."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Optional

from gridtrader.orders.idempotency import logical_slot_key_text, make_client_order_id
from gridtrader.storage import SQLiteLedger


NOW = 1_800_000_000_000


def new_ledger(path: Path) -> SQLiteLedger:
    ledger = SQLiteLedger(path, busy_timeout_ms=1_234)
    ledger.initialize()
    return ledger


def strategy_record(strategy_id: str = "strategy-1") -> Dict[str, Any]:
    return {
        "strategy_id": strategy_id,
        "account_id": "coin-m-main",
        "name": "btc-neutral-{}".format(strategy_id),
        "symbol": "BTCUSD_PERP",
        "market_type": "COIN_M_PERP",
        "mode": "neutral",
        "spacing_mode": "arithmetic",
        "initial_position_contracts": 0,
        "client_id_namespace": "ns-{}".format(strategy_id),
        "status": "CREATED",
        "config_revision": 1,
        "created_at_ms": NOW,
        "updated_at_ms": NOW,
    }


def generation_record(
    generation_id: str = "generation-1",
    strategy_id: str = "strategy-1",
    *,
    generation_no: int = 1,
    status: str = "draft",
    logical_level_count: int = 200,
    max_active_orders: int = 20,
) -> Dict[str, Any]:
    return {
        "generation_id": generation_id,
        "strategy_id": strategy_id,
        "generation_no": generation_no,
        "lower_price": "50000",
        "upper_price": "70000",
        "logical_level_count": logical_level_count,
        "strategy_mode": "neutral",
        "spacing_mode": "arithmetic",
        "arithmetic_step": "100",
        "geometric_ratio": None,
        "order_contracts": 1,
        "max_active_orders": max_active_orders,
        "status": status,
        "created_at_ms": NOW,
        "activated_at_ms": NOW if status == "active" else None,
    }


def level_record(
    level_id: str = "level-1",
    generation_id: str = "generation-1",
    *,
    level_index: int = 0,
) -> Dict[str, Any]:
    return {
        "level_id": level_id,
        "generation_id": generation_id,
        "level_index": level_index,
        "price": str(50_000 + level_index * 100),
        "planned_contracts": 2,
        "state": "armed",
        "cycle_no": 0,
        "version": 0,
        "updated_at_ms": NOW,
    }


def order_record(
    local_order_id: str = "order-1",
    client_order_id: Optional[str] = None,
    logical_slot_key: Optional[str] = None,
    *,
    level_id: str = "level-1",
    symbol: str = "BTCUSD_PERP",
    local_state: str = "active",
    attempt_no: int = 1,
    cycle_no: int = 0,
    leg_role: str = "grid_buy",
) -> Dict[str, Any]:
    if logical_slot_key is None:
        logical_slot_key = logical_slot_key_text(
            "strategy-1",
            "generation-1",
            level_id,
            cycle_no,
            leg_role,
        )
    if client_order_id is None:
        client_order_id = make_client_order_id(
            "strategy-1",
            "generation-1",
            level_id,
            cycle_no,
            leg_role,
        )
    return {
        "local_order_id": local_order_id,
        "strategy_id": "strategy-1",
        "generation_id": "generation-1",
        "level_id": level_id,
        "account_id": "coin-m-main",
        "symbol": symbol,
        "logical_slot_key": logical_slot_key,
        "cycle_no": cycle_no,
        "leg_role": leg_role,
        "attempt_no": attempt_no,
        "ownership": "BOT",
        "intent": "GRID_BUY",
        "client_order_id": client_order_id,
        "side": "buy",
        "position_side": "both",
        "order_type": "LIMIT",
        "time_in_force": "GTC",
        "reduce_only": 0,
        "price": "50000",
        "quantity_contracts": 2,
        "local_state": local_state,
        "exchange_status": "new",
        "cumulative_filled_contracts": 0,
        "submitted_at_ms": NOW,
        "created_at_ms": NOW,
        "updated_at_ms": NOW,
    }


def fill_record(
    fill_id: str = "fill-1",
    trade_id: str = "trade-100",
    *,
    fill_contracts: int = 1,
) -> Dict[str, Any]:
    client_order_id = make_client_order_id(
        "strategy-1", "generation-1", "level-1", 0, "grid_buy"
    )
    return {
        "fill_id": fill_id,
        "order_id": "order-1",
        "account_id": "coin-m-main",
        "symbol": "BTCUSD_PERP",
        "binance_trade_id": trade_id,
        "exchange_order_id": "90001",
        "client_order_id": client_order_id,
        "side": "buy",
        "price": "50000",
        "fill_contracts": fill_contracts,
        "commission": "0.000001",
        "commission_asset": "BTC",
        "realized_pnl": "0",
        "is_maker": 1,
        "trade_time_ms": NOW + 10,
        "source": "USER_STREAM",
        "created_at_ms": NOW + 11,
    }


def seed_grid(uow: Any) -> None:
    uow.strategies.add(strategy_record())
    uow.grid_generations.add(generation_record(
        logical_level_count=1,
        max_active_orders=1,
    ))
    uow.grid_levels.add(level_record())
    uow.grid_generations.activate("generation-1", NOW)
