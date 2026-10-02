"""Repository implementations for the durable SQLite ledger."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from decimal import Decimal
from typing import Any, Dict, List, Mapping, Optional, Set, Tuple

from gridtrader.orders.idempotency import logical_slot_key_text, make_client_order_id

from ..ports import (
    ConstraintViolation,
    InvariantViolation,
    LeaseUnavailable,
    NotFound,
)


Record = Dict[str, Any]
TERMINAL_EXCHANGE_STATUSES = {
    "filled", "canceled", "rejected", "expired", "expired_in_match",
}


def _decimal(value: Any) -> str:
    # Lazy import avoids a facade/repository import cycle.
    from ..ledger import decimal_text

    return decimal_text(value)


def _safe_text(value: Optional[str], field_name: str) -> Optional[str]:
    from ..ledger import ensure_safe_text

    return ensure_safe_text(value, field_name)


def _safe_json(payload: Any) -> str:
    from ..ledger import safe_json

    return safe_json(payload)


def _row(row: Optional[sqlite3.Row]) -> Optional[Record]:
    return None if row is None else dict(row)


class BaseRepository:
    table: str = ""
    primary_key: str = ""
    allowed_fields: Set[str] = set()
    required_fields: Set[str] = set()
    financial_fields: Set[str] = set()

    def __init__(self, connection: sqlite3.Connection) -> None:
        self.connection = connection

    def get(self, record_id: Any) -> Optional[Record]:
        result = self.connection.execute(
            "SELECT * FROM {} WHERE {} = ?".format(self.table, self.primary_key),
            (record_id,),
        ).fetchone()
        return _row(result)

    def require(self, record_id: Any) -> Record:
        record = self.get(record_id)
        if record is None:
            raise NotFound("{} {} was not found".format(self.table, record_id))
        return record

    def list_all(self) -> List[Record]:
        return [dict(row) for row in self.connection.execute("SELECT * FROM {}".format(self.table))]

    def add(self, record: Mapping[str, Any]) -> Record:
        values = self._prepare(record)
        columns = list(values)
        sql = "INSERT INTO {} ({}) VALUES ({})".format(
            self.table,
            ", ".join(columns),
            ", ".join("?" for _ in columns),
        )
        try:
            self.connection.execute(sql, tuple(values[column] for column in columns))
        except sqlite3.IntegrityError as exc:
            raise ConstraintViolation(str(exc)) from exc
        return self.require(values[self.primary_key])

    def _prepare(self, record: Mapping[str, Any]) -> Record:
        unknown = set(record).difference(self.allowed_fields)
        if unknown:
            raise ValueError("unsupported {} fields: {}".format(self.table, sorted(unknown)))
        missing = self.required_fields.difference(record)
        if missing:
            raise ValueError("missing {} fields: {}".format(self.table, sorted(missing)))
        values: Record = dict(record)
        for field in self.financial_fields:
            if field in values and values[field] is not None:
                values[field] = _decimal(values[field])
        return values


class StrategyRepository(BaseRepository):
    table = "strategies"
    primary_key = "strategy_id"
    allowed_fields = {
        "strategy_id", "account_id", "name", "symbol", "market_type", "mode",
        "spacing_mode", "initial_position_contracts", "client_id_namespace", "status",
        "config_revision", "created_at_ms", "updated_at_ms", "last_error",
    }
    required_fields = {
        "strategy_id", "account_id", "name", "symbol", "mode", "spacing_mode",
        "client_id_namespace", "status", "created_at_ms", "updated_at_ms",
    }

    def add(self, record: Mapping[str, Any]) -> Record:
        values = dict(record)
        if "last_error" in values:
            values["last_error"] = _safe_text(
                values["last_error"], "strategy.last_error"
            )
        return super().add(values)

    def get_by_name(self, account_id: str, name: str) -> Optional[Record]:
        return _row(self.connection.execute(
            "SELECT * FROM strategies WHERE account_id = ? AND name = ?",
            (account_id, name),
        ).fetchone())

    def update_status(
        self,
        strategy_id: str,
        status: str,
        updated_at_ms: int,
        *,
        last_error: Optional[str] = None,
        expected_status: Optional[str] = None,
    ) -> Record:
        last_error = _safe_text(last_error, "strategy.last_error")
        sql = (
            "UPDATE strategies SET status = ?, updated_at_ms = ?, last_error = ? "
            "WHERE strategy_id = ?"
        )
        params: List[Any] = [status, updated_at_ms, last_error, strategy_id]
        if expected_status is not None:
            sql += " AND status = ?"
            params.append(expected_status)
        try:
            cursor = self.connection.execute(sql, tuple(params))
        except sqlite3.IntegrityError as exc:
            raise ConstraintViolation(str(exc)) from exc
        if cursor.rowcount != 1:
            raise InvariantViolation("strategy status changed concurrently")
        return self.require(strategy_id)


class GridGenerationRepository(BaseRepository):
    table = "grid_generations"
    primary_key = "generation_id"
    allowed_fields = {
        "generation_id", "strategy_id", "generation_no", "lower_price", "upper_price",
        "logical_level_count", "strategy_mode", "spacing_mode",
        "arithmetic_step", "geometric_ratio",
        "order_contracts", "max_active_orders", "status", "change_reason",
        "created_at_ms", "activated_at_ms", "retired_at_ms",
    }
    required_fields = {
        "generation_id", "strategy_id", "generation_no", "lower_price", "upper_price",
        "logical_level_count", "strategy_mode", "spacing_mode", "order_contracts",
        "max_active_orders", "status",
        "created_at_ms",
    }
    financial_fields = {"lower_price", "upper_price", "arithmetic_step", "geometric_ratio"}

    def add(self, record: Mapping[str, Any]) -> Record:
        if record.get("status") not in {"draft", "preparing"}:
            raise InvariantViolation(
                "new grid generation must begin as draft or preparing"
            )
        return super().add(record)

    def _prepare(self, record: Mapping[str, Any]) -> Record:
        values = super()._prepare(record)
        lower = Decimal(values["lower_price"])
        upper = Decimal(values["upper_price"])
        if lower <= 0 or upper <= lower:
            raise InvariantViolation(
                "grid generation bounds must be positive and increasing"
            )
        if values["max_active_orders"] > values["logical_level_count"]:
            raise InvariantViolation(
                "max_active_orders cannot exceed logical_level_count"
            )
        if values["spacing_mode"] == "arithmetic":
            step = values.get("arithmetic_step")
            if step is None or Decimal(step) <= 0 or values.get("geometric_ratio") is not None:
                raise InvariantViolation(
                    "arithmetic generation requires one positive arithmetic_step"
                )
        elif values["spacing_mode"] == "geometric":
            ratio = values.get("geometric_ratio")
            if ratio is None or Decimal(ratio) <= 1 or values.get("arithmetic_step") is not None:
                raise InvariantViolation(
                    "geometric generation requires one geometric_ratio greater than one"
                )
        return values

    def get_active(self, strategy_id: str) -> Optional[Record]:
        return _row(self.connection.execute(
            "SELECT * FROM grid_generations WHERE strategy_id = ? AND status = 'active'",
            (strategy_id,),
        ).fetchone())

    def activate(self, generation_id: str, activated_at_ms: int) -> Record:
        generation = self.require(generation_id)
        level_count = self.connection.execute(
            "SELECT COUNT(*) FROM grid_levels WHERE generation_id = ?",
            (generation_id,),
        ).fetchone()[0]
        if level_count != generation["logical_level_count"]:
            raise InvariantViolation(
                "generation cannot activate until every logical level is persisted"
            )
        try:
            cursor = self.connection.execute(
                """
                UPDATE grid_generations
                   SET status = 'active', activated_at_ms = ?, retired_at_ms = NULL
                 WHERE generation_id = ? AND status IN ('draft', 'preparing')
                """,
                (activated_at_ms, generation_id),
            )
        except sqlite3.IntegrityError as exc:
            raise ConstraintViolation(str(exc)) from exc
        if cursor.rowcount != 1:
            raise InvariantViolation("only a draft or preparing generation can be activated")
        return self.require(generation_id)


class GridLevelRepository(BaseRepository):
    table = "grid_levels"
    primary_key = "level_id"
    allowed_fields = {
        "level_id", "generation_id", "level_index", "price", "planned_contracts",
        "state", "cycle_no", "version", "updated_at_ms",
    }
    required_fields = {
        "level_id", "generation_id", "level_index", "price", "planned_contracts",
        "state", "updated_at_ms",
    }
    financial_fields = {"price"}

    def list_for_generation(self, generation_id: str) -> List[Record]:
        return [dict(row) for row in self.connection.execute(
            "SELECT * FROM grid_levels WHERE generation_id = ? ORDER BY level_index",
            (generation_id,),
        )]


class OrderRepository(BaseRepository):
    table = "orders"
    primary_key = "local_order_id"
    allowed_fields = {
        "local_order_id", "strategy_id", "generation_id", "level_id", "account_id",
        "symbol", "logical_slot_key", "cycle_no", "leg_role", "attempt_no", "ownership", "intent",
        "client_order_id", "exchange_order_id", "side", "position_side", "order_type",
        "time_in_force", "reduce_only", "price", "quantity_contracts", "local_state",
        "exchange_status", "cumulative_filled_contracts", "avg_fill_price", "exchange_update_ms",
        "submitted_at_ms", "terminal_at_ms", "last_source", "version", "created_at_ms",
        "updated_at_ms",
    }
    required_fields = {
        "local_order_id", "account_id", "symbol", "logical_slot_key", "intent",
        "leg_role", "client_order_id", "side", "price", "quantity_contracts", "local_state",
        "created_at_ms", "updated_at_ms",
    }
    financial_fields = {"price", "avg_fill_price"}

    def _prepare(self, record: Mapping[str, Any]) -> Record:
        values = super()._prepare(record)
        if values.get("ownership", "BOT") == "BOT":
            required_identity = {
                "strategy_id",
                "generation_id",
                "level_id",
                "cycle_no",
                "leg_role",
            }
            missing = [field for field in required_identity if values.get(field) is None]
            if missing:
                raise InvariantViolation(
                    "BOT order is missing semantic identity fields: {}".format(
                        ", ".join(sorted(missing))
                    )
                )
            expected_slot = logical_slot_key_text(
                values["strategy_id"],
                values["generation_id"],
                values["level_id"],
                values["cycle_no"],
                values["leg_role"],
            )
            if values["logical_slot_key"] != expected_slot:
                raise InvariantViolation(
                    "logical_slot_key does not match the BOT semantic identity"
                )
            expected_client_id = make_client_order_id(
                values["strategy_id"],
                values["generation_id"],
                values["level_id"],
                values["cycle_no"],
                values["leg_role"],
            )
            if values["client_order_id"] != expected_client_id:
                raise InvariantViolation(
                    "client_order_id does not match the BOT semantic identity"
                )
        return values

    def get_by_client_order_id(self, client_order_id: str) -> Optional[Record]:
        return _row(self.connection.execute(
            "SELECT * FROM orders WHERE client_order_id = ?", (client_order_id,)
        ).fetchone())

    def list_unfinished(self, strategy_id: Optional[str] = None) -> List[Record]:
        params: List[Any] = []
        sql = "SELECT * FROM orders WHERE local_state <> 'terminal'"
        if strategy_id is not None:
            sql += " AND strategy_id = ?"
            params.append(strategy_id)
        sql += " ORDER BY created_at_ms, local_order_id"
        return [dict(row) for row in self.connection.execute(sql, tuple(params))]

    def list_for_strategy(
        self, strategy_id: str, *, symbol: Optional[str] = None
    ) -> List[Record]:
        params: List[Any] = [strategy_id]
        sql = "SELECT * FROM orders WHERE strategy_id = ?"
        if symbol is not None:
            sql += " AND symbol = ?"
            params.append(symbol)
        sql += " ORDER BY created_at_ms, local_order_id"
        return [dict(row) for row in self.connection.execute(sql, tuple(params))]

    def mark_ack_unknown(self, local_order_id: str, updated_at_ms: int) -> Record:
        cursor = self.connection.execute(
            """
            UPDATE orders
               SET local_state = 'ack_unknown', updated_at_ms = ?, version = version + 1
             WHERE local_order_id = ? AND local_state IN ('submitting', 'ack_unknown')
            """,
            (updated_at_ms, local_order_id),
        )
        if cursor.rowcount != 1:
            raise InvariantViolation("order cannot enter ACK_UNKNOWN from its current state")
        return self.require(local_order_id)

    def record_exchange_update(
        self,
        local_order_id: str,
        *,
        exchange_status: str,
        cumulative_filled_contracts: int,
        exchange_update_ms: int,
        updated_at_ms: int,
        exchange_order_id: Optional[str] = None,
        avg_fill_price: Optional[Any] = None,
        side: Optional[str] = None,
        position_side: Optional[str] = None,
        price: Optional[Any] = None,
        original_contracts: Optional[int] = None,
        reduce_only: Optional[bool] = None,
        order_type: Optional[str] = None,
        time_in_force: Optional[str] = None,
        last_source: Optional[str] = None,
    ) -> Record:
        current = self.require(local_order_id)
        if isinstance(cumulative_filled_contracts, bool) or not isinstance(
            cumulative_filled_contracts, int
        ):
            raise InvariantViolation("cumulative fill quantity must be an integer")
        if cumulative_filled_contracts < 0:
            raise InvariantViolation("cumulative fill quantity cannot be negative")
        if cumulative_filled_contracts < current["cumulative_filled_contracts"]:
            raise InvariantViolation("cumulative fill quantity cannot decrease")
        if cumulative_filled_contracts > current["quantity_contracts"]:
            raise InvariantViolation("cumulative fill quantity cannot exceed order quantity")
        if current["exchange_status"] in TERMINAL_EXCHANGE_STATUSES:
            if exchange_status != current["exchange_status"]:
                raise InvariantViolation("terminal exchange status cannot regress")
        if current["exchange_status"] == "new" and exchange_status == "unknown":
            raise InvariantViolation("exchange status cannot regress to unknown")
        if current["exchange_status"] == "partially_filled" and exchange_status in {
            "unknown",
            "new",
        }:
            raise InvariantViolation("exchange status cannot regress after a partial fill")
        if exchange_status in {"unknown", "new"} and cumulative_filled_contracts != 0:
            raise InvariantViolation(
                "unknown or new exchange status cannot carry a cumulative fill"
            )
        if exchange_status == "partially_filled" and not (
            0 < cumulative_filled_contracts < current["quantity_contracts"]
        ):
            raise InvariantViolation("partially filled status requires a strict partial fill")
        if (
            exchange_status == "filled"
            and cumulative_filled_contracts != current["quantity_contracts"]
        ):
            raise InvariantViolation("filled status requires the complete order quantity")
        if (
            exchange_order_id is not None
            and current["exchange_order_id"] is not None
            and exchange_order_id != current["exchange_order_id"]
        ):
            raise InvariantViolation("exchange order id is immutable once known")
        if (
            current["exchange_update_ms"] is not None
            and exchange_update_ms <= current["exchange_update_ms"]
        ):
            authoritative = {
                "exchange_order_id": exchange_order_id,
                "side": side,
                "position_side": position_side,
                "price": None if price is None else _decimal(price),
                "quantity_contracts": original_contracts,
                "reduce_only": (
                    None if reduce_only is None else int(reduce_only)
                ),
                "order_type": order_type,
                "time_in_force": time_in_force,
            }
            missing_identity = [
                field for field, value in authoritative.items() if value is None
            ]
            differences = [
                "exchange_status"
                if current["exchange_status"] != exchange_status
                else None,
                "cumulative_filled_contracts"
                if current["cumulative_filled_contracts"]
                != cumulative_filled_contracts
                else None,
            ]
            differences.extend(
                field
                for field, value in authoritative.items()
                if value is not None and current[field] != value
            )
            conflicts = sorted(
                set(missing_identity).union(
                    difference for difference in differences if difference is not None
                )
            )
            if conflicts:
                raise InvariantViolation(
                    "stale exchange observation cannot be proven equivalent: {}".format(
                        ", ".join(conflicts)
                    )
                )
            return current
        local_state = "terminal" if exchange_status in TERMINAL_EXCHANGE_STATUSES else "active"
        terminal_at_ms = updated_at_ms if local_state == "terminal" else None
        avg_text = None if avg_fill_price is None else _decimal(avg_fill_price)
        try:
            self.connection.execute(
                """
                UPDATE orders
                   SET exchange_order_id = COALESCE(?, exchange_order_id),
                       local_state = ?, exchange_status = ?, cumulative_filled_contracts = ?,
                       avg_fill_price = COALESCE(?, avg_fill_price), exchange_update_ms = ?,
                       terminal_at_ms = COALESCE(?, terminal_at_ms), last_source = ?,
                       updated_at_ms = ?, version = version + 1
                 WHERE local_order_id = ?
                """,
                (
                    exchange_order_id, local_state, exchange_status, cumulative_filled_contracts,
                    avg_text, exchange_update_ms, terminal_at_ms, last_source,
                    updated_at_ms, local_order_id,
                ),
            )
        except sqlite3.IntegrityError as exc:
            raise ConstraintViolation(str(exc)) from exc
        return self.require(local_order_id)


class FillRepository(BaseRepository):
    table = "fills"
    primary_key = "fill_id"
    allowed_fields = {
        "fill_id", "order_id", "account_id", "symbol", "binance_trade_id",
        "exchange_order_id", "client_order_id", "side", "price", "fill_contracts",
        "commission", "commission_asset", "realized_pnl", "is_maker", "trade_time_ms",
        "source", "event_id", "created_at_ms",
    }
    required_fields = {
        "fill_id", "order_id", "account_id", "symbol", "binance_trade_id", "side",
        "price", "fill_contracts", "trade_time_ms", "source", "created_at_ms",
    }
    financial_fields = {"price", "commission", "realized_pnl"}
    _identity_fields = {
        "order_id", "account_id", "symbol", "binance_trade_id", "side", "price",
        "fill_contracts", "trade_time_ms",
    }
    _enrichment_fields = {
        "exchange_order_id",
        "client_order_id",
        "commission",
        "commission_asset",
        "realized_pnl",
        "is_maker",
        "event_id",
    }

    def get_by_trade_id(
        self,
        account_id: str,
        symbol: str,
        binance_trade_id: str,
    ) -> Optional[Record]:
        return _row(self.connection.execute(
            """
            SELECT * FROM fills
             WHERE account_id = ? AND symbol = ? AND binance_trade_id = ?
            """,
            (account_id, symbol, binance_trade_id),
        ).fetchone())

    def list_for_account_symbol(
        self,
        account_id: str,
        symbol: str,
    ) -> List[Record]:
        return [
            dict(row)
            for row in self.connection.execute(
                """
                SELECT * FROM fills
                 WHERE account_id = ? AND symbol = ?
                 ORDER BY trade_time_ms, binance_trade_id
                """,
                (account_id, symbol),
            )
        ]

    def list_for_strategy_symbol(
        self,
        strategy_id: str,
        account_id: str,
        symbol: str,
    ) -> List[Record]:
        """Return fills whose durable order ownership belongs to one strategy.

        Position reconstruction must not use every account fill for a symbol:
        another strategy (or an external/manual order) may trade the same
        contract.  The order foreign key is the ownership proof.  The joined
        position side also restores information that Binance trade rows carry
        but the Phase-1 ``fills`` table did not persist directly.
        """

        return [
            dict(row)
            for row in self.connection.execute(
                """
                SELECT f.*,
                       o.position_side AS position_side,
                       o.strategy_id AS strategy_id,
                       o.ownership AS ownership,
                       o.client_order_id AS owned_client_order_id,
                       o.exchange_order_id AS owned_exchange_order_id
                  FROM fills AS f
                  JOIN orders AS o ON o.local_order_id = f.order_id
                 WHERE o.strategy_id = ?
                   AND o.ownership = 'BOT'
                   AND f.account_id = ?
                   AND f.symbol = ?
                 ORDER BY f.trade_time_ms, f.binance_trade_id
                """,
                (strategy_id, account_id, symbol),
            )
        ]

    def add_idempotent(self, record: Mapping[str, Any]) -> Tuple[Record, bool]:
        values = self._prepare(record)
        existing = self.get_by_trade_id(
            str(values["account_id"]),
            str(values["symbol"]),
            str(values["binance_trade_id"]),
        )
        if existing is not None:
            self._assert_same_fill(existing, values)
            return self._merge_enrichment(existing, values), False
        try:
            inserted = super().add(values)
            return inserted, True
        except ConstraintViolation:
            existing = self.get_by_trade_id(
                str(values["account_id"]),
                str(values["symbol"]),
                str(values["binance_trade_id"]),
            )
            if existing is None:
                raise
            self._assert_same_fill(existing, values)
            return self._merge_enrichment(existing, values), False

    def _assert_same_fill(self, existing: Mapping[str, Any], candidate: Mapping[str, Any]) -> None:
        differences = [
            field for field in self._identity_fields
            if field in candidate and existing.get(field) != candidate.get(field)
        ]
        if differences:
            raise InvariantViolation(
                "Binance trade id {} was reused with different {}".format(
                    candidate.get("binance_trade_id"), ", ".join(sorted(differences))
                )
            )

    def _merge_enrichment(
        self,
        existing: Mapping[str, Any],
        candidate: Mapping[str, Any],
    ) -> Record:
        updates: Record = {}
        conflicts: List[str] = []
        for field in self._enrichment_fields:
            candidate_value = candidate.get(field)
            existing_value = existing.get(field)
            if candidate_value is None:
                continue
            if existing_value is None:
                updates[field] = candidate_value
            elif existing_value != candidate_value:
                conflicts.append(field)
        if conflicts:
            raise InvariantViolation(
                "duplicate Binance trade has conflicting enrichment fields: {}".format(
                    ", ".join(sorted(conflicts))
                )
            )
        if updates:
            assignments = ", ".join("{} = ?".format(field) for field in updates)
            self.connection.execute(
                "UPDATE fills SET {} WHERE fill_id = ?".format(assignments),
                tuple(updates.values()) + (existing["fill_id"],),
            )
            return self.require(existing["fill_id"])
        return dict(existing)


class ExchangeTradeObservationRepository(BaseRepository):
    """Complete order-independent REST trade evidence for an account."""

    table = "exchange_trade_observations"
    primary_key = "binance_trade_id"
    allowed_fields = {
        "account_id", "symbol", "binance_trade_id", "exchange_order_id",
        "client_order_id", "side", "position_side", "price",
        "fill_contracts", "commission", "commission_asset", "realized_pnl",
        "is_maker", "trade_time_ms", "first_checkpoint_id",
        "last_checkpoint_id", "first_observed_at_ms", "last_observed_at_ms",
        "payload_hash",
    }
    required_fields = allowed_fields.difference(
        {"client_order_id", "is_maker", "payload_hash"}
    )
    financial_fields = {"price", "commission", "realized_pnl"}
    _fact_fields = {
        "account_id", "symbol", "binance_trade_id", "exchange_order_id",
        "side", "position_side", "price", "fill_contracts", "commission",
        "commission_asset", "realized_pnl", "trade_time_ms",
    }

    def get(self, record_id: Any) -> Optional[Record]:
        """Resolve only the full Binance trade scope, never a bare trade ID."""

        if (
            not isinstance(record_id, tuple)
            or len(record_id) != 3
            or not all(isinstance(value, str) and value for value in record_id)
        ):
            raise ValueError(
                "exchange trade key must be (account_id, symbol, binance_trade_id)"
            )
        return self.get_by_trade_id(*record_id)

    def require(self, record_id: Any) -> Record:
        record = self.get(record_id)
        if record is None:
            raise NotFound("exchange trade observation was not found")
        return record

    def add(self, record: Mapping[str, Any]) -> Record:
        del record
        raise NotImplementedError("use record_observation for exchange trade facts")

    def get_by_trade_id(
        self,
        account_id: str,
        symbol: str,
        binance_trade_id: str,
    ) -> Optional[Record]:
        return _row(self.connection.execute(
            """
            SELECT * FROM exchange_trade_observations
             WHERE account_id = ? AND symbol = ? AND binance_trade_id = ?
            """,
            (account_id, symbol, binance_trade_id),
        ).fetchone())

    def record_observation(
        self,
        record: Mapping[str, Any],
    ) -> Tuple[Record, bool]:
        values = self._prepare(record)
        canonical_fact = json.dumps(
            {field: values.get(field) for field in sorted(self._fact_fields)},
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        values["payload_hash"] = hashlib.sha256(
            canonical_fact.encode("utf-8")
        ).hexdigest()
        existing = self.get_by_trade_id(
            str(values["account_id"]),
            str(values["symbol"]),
            str(values["binance_trade_id"]),
        )
        if existing is None:
            columns = list(values)
            try:
                self.connection.execute(
                    "INSERT INTO exchange_trade_observations ({}) VALUES ({})".format(
                        ", ".join(columns),
                        ", ".join("?" for _ in columns),
                    ),
                    tuple(values[column] for column in columns),
                )
            except sqlite3.IntegrityError as exc:
                raise ConstraintViolation(str(exc)) from exc
            return (
                self.get_by_trade_id(
                    str(values["account_id"]),
                    str(values["symbol"]),
                    str(values["binance_trade_id"]),
                ),
                True,
            )  # type: ignore[return-value]

        conflicts = [
            field
            for field in self._fact_fields
            if existing[field] != values[field]
        ]
        if (
            existing["client_order_id"] is not None
            and values.get("client_order_id") is not None
            and existing["client_order_id"] != values["client_order_id"]
        ):
            conflicts.append("client_order_id")
        if (
            existing["is_maker"] is not None
            and values.get("is_maker") is not None
            and existing["is_maker"] != values["is_maker"]
        ):
            conflicts.append("is_maker")
        if conflicts:
            raise InvariantViolation(
                "exchange trade observation conflicts: "
                + ", ".join(sorted(set(conflicts)))
            )
        self.connection.execute(
            """
            UPDATE exchange_trade_observations
               SET client_order_id = COALESCE(client_order_id, ?),
                   is_maker = COALESCE(is_maker, ?),
                   last_checkpoint_id = ?, last_observed_at_ms = ?,
                   payload_hash = ?
             WHERE account_id = ? AND symbol = ? AND binance_trade_id = ?
            """,
            (
                values.get("client_order_id"),
                values.get("is_maker"),
                values["last_checkpoint_id"],
                values["last_observed_at_ms"],
                values["payload_hash"],
                values["account_id"],
                values["symbol"],
                values["binance_trade_id"],
            ),
        )
        return self.get_by_trade_id(
            str(values["account_id"]),
            str(values["symbol"]),
            str(values["binance_trade_id"]),
        ), False  # type: ignore[return-value]


class PositionRepository:
    financial_fields = {
        "entry_price",
        "break_even_price",
        "mark_price",
        "unrealized_pnl",
        "liquidation_price",
    }
    allowed_fields = {
        "account_id", "symbol", "position_side", "quantity_contracts", "entry_price",
        "break_even_price", "mark_price", "unrealized_pnl", "leverage", "margin_type",
        "margin_asset", "isolated", "liquidation_price", "exchange_update_ms",
        "observed_at_ms", "checkpoint_id", "source",
    }
    required_fields = {
        "account_id", "symbol", "position_side", "quantity_contracts", "exchange_update_ms",
        "observed_at_ms", "source",
    }

    def __init__(self, connection: sqlite3.Connection) -> None:
        self.connection = connection

    def get(self, account_id: str, symbol: str, position_side: str = "both") -> Optional[Record]:
        return _row(self.connection.execute(
            """
            SELECT * FROM positions
             WHERE account_id = ? AND symbol = ? AND position_side = ?
            """,
            (account_id, symbol, position_side),
        ).fetchone())

    def upsert(self, record: Mapping[str, Any]) -> Record:
        unknown = set(record).difference(self.allowed_fields)
        missing = self.required_fields.difference(record)
        if unknown:
            raise ValueError("unsupported positions fields: {}".format(sorted(unknown)))
        if missing:
            raise ValueError("missing positions fields: {}".format(sorted(missing)))
        values = dict(record)
        for field in self.financial_fields:
            if field in values and values[field] is not None:
                values[field] = _decimal(values[field])
        columns = list(values)
        assignments = ", ".join(
            "{0} = excluded.{0}".format(column)
            for column in columns
            if column not in {"account_id", "symbol", "position_side"}
        )
        sql = """
            INSERT INTO positions ({columns}) VALUES ({placeholders})
            ON CONFLICT(account_id, symbol, position_side) DO UPDATE SET {assignments}
            WHERE excluded.exchange_update_ms >= positions.exchange_update_ms
               OR (
                    excluded.source = 'REST'
                AND excluded.quantity_contracts = 0
                AND excluded.observed_at_ms >= positions.observed_at_ms
               )
        """.format(
            columns=", ".join(columns),
            placeholders=", ".join("?" for _ in columns),
            assignments=assignments,
        )
        try:
            self.connection.execute(sql, tuple(values[column] for column in columns))
        except sqlite3.IntegrityError as exc:
            raise ConstraintViolation(str(exc)) from exc
        return self.get(  # type: ignore[return-value]
            values["account_id"],
            values["symbol"],
            values["position_side"],
        )

    def list_for_account(self, account_id: str) -> List[Record]:
        return [dict(row) for row in self.connection.execute(
            "SELECT * FROM positions WHERE account_id = ? ORDER BY symbol, position_side",
            (account_id,),
        )]


class InstrumentRulesRepository(BaseRepository):
    table = "instrument_rules"
    primary_key = "instrument_rule_id"
    allowed_fields = {
        "instrument_rule_id", "symbol", "pair", "contract_type", "status",
        "contract_size", "margin_asset", "tick_size", "quantity_step",
        "min_qty", "max_qty", "min_price", "max_price",
        "supported_order_types_json", "observed_at_ms", "payload_hash",
        "rules_hash",
    }
    required_fields = {
        "symbol", "pair", "contract_type", "status", "contract_size",
        "margin_asset", "tick_size", "quantity_step", "min_qty",
        "supported_order_types_json", "observed_at_ms", "payload_hash",
        "rules_hash",
    }
    financial_fields = {"contract_size", "tick_size", "min_price", "max_price"}

    def latest_for_symbol(self, symbol: str) -> Optional[Record]:
        return _row(self.connection.execute(
            """
            SELECT * FROM instrument_rules
             WHERE symbol = ?
             ORDER BY observed_at_ms DESC, instrument_rule_id DESC LIMIT 1
            """,
            (symbol,),
        ).fetchone())

    def record_observation(self, record: Mapping[str, Any]) -> Tuple[Record, bool]:
        values = self._prepare(record)
        values.pop("instrument_rule_id", None)
        existing = _row(self.connection.execute(
            "SELECT * FROM instrument_rules WHERE symbol = ? AND rules_hash = ?",
            (values["symbol"], values["rules_hash"]),
        ).fetchone())
        if existing is not None:
            if values["observed_at_ms"] > existing["observed_at_ms"]:
                self.connection.execute(
                    """
                    UPDATE instrument_rules SET observed_at_ms = ?, payload_hash = ?
                     WHERE instrument_rule_id = ?
                    """,
                    (
                        values["observed_at_ms"],
                        values["payload_hash"],
                        existing["instrument_rule_id"],
                    ),
                )
                existing = self.require(existing["instrument_rule_id"])
            return existing, False
        columns = list(values)
        try:
            cursor = self.connection.execute(
                "INSERT INTO instrument_rules ({}) VALUES ({})".format(
                    ", ".join(columns), ", ".join("?" for _ in columns)
                ),
                tuple(values[column] for column in columns),
            )
        except sqlite3.IntegrityError as exc:
            raise ConstraintViolation(str(exc)) from exc
        return self.require(cursor.lastrowid), True


class PositionModeObservationRepository(BaseRepository):
    table = "position_mode_observations"
    primary_key = "position_mode_observation_id"
    allowed_fields = {
        "position_mode_observation_id", "account_id", "mode", "observed_at_ms",
        "checkpoint_id", "source",
    }
    required_fields = {"account_id", "mode", "observed_at_ms", "checkpoint_id"}

    def latest_for_account(self, account_id: str) -> Optional[Record]:
        return _row(self.connection.execute(
            """
            SELECT * FROM position_mode_observations
             WHERE account_id = ?
             ORDER BY observed_at_ms DESC, position_mode_observation_id DESC LIMIT 1
            """,
            (account_id,),
        ).fetchone())

    def add_idempotent(self, record: Mapping[str, Any]) -> Tuple[Record, bool]:
        values = self._prepare(record)
        values.pop("position_mode_observation_id", None)
        existing = _row(self.connection.execute(
            """
            SELECT * FROM position_mode_observations
             WHERE checkpoint_id = ? AND account_id = ?
            """,
            (values["checkpoint_id"], values["account_id"]),
        ).fetchone())
        if existing is not None:
            if existing["mode"] != values["mode"]:
                raise InvariantViolation(
                    "position mode changed within one recovery checkpoint"
                )
            return existing, False
        columns = list(values)
        cursor = self.connection.execute(
            "INSERT INTO position_mode_observations ({}) VALUES ({})".format(
                ", ".join(columns), ", ".join("?" for _ in columns)
            ),
            tuple(values[column] for column in columns),
        )
        return self.require(cursor.lastrowid), True


class ExchangeObservationRepository(BaseRepository):
    table = "exchange_observations"
    primary_key = "exchange_observation_id"
    allowed_fields = {
        "exchange_observation_id", "checkpoint_id", "observation_type",
        "account_id", "symbol", "observed_at_ms", "server_time_ms",
        "item_count", "complete", "next_cursor", "pagination_watermark",
        "payload_hash", "metadata_json",
    }
    required_fields = {
        "checkpoint_id", "observation_type", "account_id", "observed_at_ms",
        "payload_hash", "metadata_json",
    }

    def append_idempotent(self, record: Mapping[str, Any]) -> Tuple[Record, bool]:
        values = self._prepare(record)
        values.pop("exchange_observation_id", None)
        metadata = values["metadata_json"]
        values["metadata_json"] = (
            _safe_json(metadata)
            if not isinstance(metadata, str)
            else _safe_json(json.loads(metadata))
        )
        existing = _row(self.connection.execute(
            """
            SELECT * FROM exchange_observations
             WHERE checkpoint_id = ? AND observation_type = ?
               AND symbol IS ? AND server_time_ms IS ?
               AND pagination_watermark IS ?
               AND next_cursor IS ? AND payload_hash = ?
            """,
            (
                values["checkpoint_id"], values["observation_type"],
                values.get("symbol"), values.get("server_time_ms"),
                values.get("pagination_watermark"),
                values.get("next_cursor"),
                values["payload_hash"],
            ),
        ).fetchone())
        if existing is not None:
            comparable = {
                "item_count", "complete", "next_cursor", "payload_hash", "metadata_json"
            }
            if any(existing[field] != values.get(field, existing[field]) for field in comparable):
                raise InvariantViolation(
                    "exchange observation identity was reused with different evidence"
                )
            return existing, False
        columns = list(values)
        cursor = self.connection.execute(
            "INSERT INTO exchange_observations ({}) VALUES ({})".format(
                ", ".join(columns), ", ".join("?" for _ in columns)
            ),
            tuple(values[column] for column in columns),
        )
        return self.require(cursor.lastrowid), True


class RecoveryCheckpointRepository(BaseRepository):
    table = "recovery_checkpoints"
    primary_key = "checkpoint_id"
    allowed_fields = {
        "checkpoint_id", "run_id", "strategy_id", "account_id", "symbol", "reason",
        "status", "rest_server_time_ms", "open_orders_observed_at_ms",
        "position_observed_at_ms", "margin_observed_at_ms", "fills_from_ms", "fills_through_ms",
        "last_binance_trade_id", "ws_buffer_from_ms", "ws_buffer_through_ms",
        "orders_seen", "fills_seen", "mismatch_count", "started_at_ms",
        "completed_at_ms", "error", "recovery_epoch", "snapshot_observed_at_ms",
        "trades_complete", "next_trade_cursor", "pagination_watermark",
        "position_mode", "rules_hash",
    }
    required_fields = {"run_id", "account_id", "reason", "status", "started_at_ms"}

    def add(self, record: Mapping[str, Any]) -> Record:
        values = self._prepare(record)
        if values["status"] != "STARTED":
            raise InvariantViolation("a recovery checkpoint must begin in STARTED")
        values.pop("checkpoint_id", None)
        values["error"] = _safe_text(values.get("error"), "checkpoint.error")
        columns = list(values)
        try:
            cursor = self.connection.execute(
                "INSERT INTO recovery_checkpoints ({}) VALUES ({})".format(
                    ", ".join(columns), ", ".join("?" for _ in columns)
                ),
                tuple(values[column] for column in columns),
            )
        except sqlite3.IntegrityError as exc:
            raise ConstraintViolation(str(exc)) from exc
        return self.require(cursor.lastrowid)

    def latest_for_strategy(self, strategy_id: str) -> Optional[Record]:
        return _row(self.connection.execute(
            """
            SELECT * FROM recovery_checkpoints
             WHERE strategy_id = ?
             ORDER BY started_at_ms DESC, checkpoint_id DESC LIMIT 1
            """,
            (strategy_id,),
        ).fetchone())

    def record_pagination_progress(
        self,
        checkpoint_id: int,
        *,
        next_trade_cursor: Optional[str],
        pagination_watermark: Optional[str],
        fills_through_ms: Optional[int],
        trades_complete: bool,
    ) -> Record:
        cursor = self.connection.execute(
            """
            UPDATE recovery_checkpoints
               SET next_trade_cursor = ?, pagination_watermark = ?,
                   fills_through_ms = ?, trades_complete = ?
             WHERE checkpoint_id = ?
               AND status NOT IN ('COMPLETE', 'FAILED', 'BLOCKED')
            """,
            (
                next_trade_cursor,
                pagination_watermark,
                fills_through_ms,
                int(trades_complete),
                checkpoint_id,
            ),
        )
        if cursor.rowcount != 1:
            raise InvariantViolation("checkpoint is missing or no longer writable")
        return self.require(checkpoint_id)

    def advance(
        self,
        checkpoint_id: int,
        *,
        status: str,
        rest_server_time_ms: Optional[int] = None,
        open_orders_observed_at_ms: Optional[int] = None,
        position_observed_at_ms: Optional[int] = None,
        margin_observed_at_ms: Optional[int] = None,
        fills_from_ms: Optional[int] = None,
        fills_through_ms: Optional[int] = None,
        last_binance_trade_id: Optional[str] = None,
        ws_buffer_from_ms: Optional[int] = None,
        ws_buffer_through_ms: Optional[int] = None,
        snapshot_observed_at_ms: Optional[int] = None,
        trades_complete: Optional[bool] = None,
        next_trade_cursor: Optional[str] = None,
        pagination_watermark: Optional[str] = None,
        position_mode: Optional[str] = None,
        rules_hash: Optional[str] = None,
    ) -> Record:
        current = self.require(checkpoint_id)
        expected_next = {
            "STARTED": "SNAPSHOT_COMPLETE",
            "SNAPSHOT_COMPLETE": "REPLAY_COMPLETE",
            "REPLAY_COMPLETE": "RECONCILED",
        }.get(current["status"])
        if status != expected_next:
            raise InvariantViolation(
                "invalid checkpoint transition: {} -> {}".format(
                    current["status"], status
                )
            )
        observations = {
            "rest_server_time_ms": rest_server_time_ms,
            "open_orders_observed_at_ms": open_orders_observed_at_ms,
            "position_observed_at_ms": position_observed_at_ms,
            "margin_observed_at_ms": margin_observed_at_ms,
            "fills_from_ms": fills_from_ms,
            "fills_through_ms": fills_through_ms,
            "last_binance_trade_id": last_binance_trade_id,
            "ws_buffer_from_ms": ws_buffer_from_ms,
            "ws_buffer_through_ms": ws_buffer_through_ms,
            "snapshot_observed_at_ms": snapshot_observed_at_ms,
            "trades_complete": (
                None if trades_complete is None else int(trades_complete)
            ),
            "next_trade_cursor": next_trade_cursor,
            "pagination_watermark": pagination_watermark,
            "position_mode": position_mode,
            "rules_hash": rules_hash,
        }
        merged = {
            field: value if value is not None else current[field]
            for field, value in observations.items()
        }
        if status == "SNAPSHOT_COMPLETE" and any(
            merged[field] is None
            for field in (
                "rest_server_time_ms",
                "open_orders_observed_at_ms",
                "position_observed_at_ms",
                "margin_observed_at_ms",
            )
        ):
            raise InvariantViolation(
                "snapshot completion requires order, position, margin and server timestamps"
            )
        if status == "REPLAY_COMPLETE" and merged["fills_through_ms"] is None:
            raise InvariantViolation("replay completion requires a fills high-water mark")
        if (
            status == "REPLAY_COMPLETE"
            and current["recovery_epoch"] > 0
            and trades_complete is not True
        ):
            raise InvariantViolation(
                "Phase-2 replay completion requires explicit complete pagination"
            )
        if status == "REPLAY_COMPLETE" and trades_complete is False:
            raise InvariantViolation("replay completion requires complete pagination")
        if status == "REPLAY_COMPLETE" and trades_complete is None:
            # Backward-compatible inference for Phase-1 callers.  Phase-2
            # RecoveryManager always passes explicit completeness evidence.
            merged["trades_complete"] = 1
        assignments = ", ".join(
            "{} = ?".format(field) for field in observations
        )
        cursor = self.connection.execute(
            "UPDATE recovery_checkpoints SET status = ?, {} "
            "WHERE checkpoint_id = ? AND status = ?".format(assignments),
            (status, *merged.values(), checkpoint_id, current["status"]),
        )
        if cursor.rowcount != 1:
            raise InvariantViolation("checkpoint changed concurrently")
        return self.require(checkpoint_id)

    def complete(
        self,
        checkpoint_id: int,
        *,
        status: str,
        completed_at_ms: int,
        orders_seen: int,
        fills_seen: int,
        mismatch_count: int,
        error: Optional[str] = None,
    ) -> Record:
        if status not in {"COMPLETE", "FAILED", "BLOCKED"}:
            raise ValueError("checkpoint completion requires a terminal status")
        current = self.require(checkpoint_id)
        if current["status"] in {"COMPLETE", "FAILED", "BLOCKED"}:
            raise InvariantViolation("checkpoint is already terminal")
        if status == "COMPLETE":
            if current["status"] != "RECONCILED":
                raise InvariantViolation(
                    "only a reconciled checkpoint can become complete"
                )
            required_evidence = (
                "rest_server_time_ms",
                "open_orders_observed_at_ms",
                "position_observed_at_ms",
                "margin_observed_at_ms",
                "fills_through_ms",
            )
            if current["recovery_epoch"] > 0:
                required_evidence += ("snapshot_observed_at_ms", "rules_hash")
            if any(current[field] is None for field in required_evidence):
                raise InvariantViolation(
                    "complete checkpoint is missing mandatory REST evidence"
                )
            if current["recovery_epoch"] > 0 and (
                not current["position_mode"] or not current["rules_hash"]
            ):
                raise InvariantViolation(
                    "complete checkpoint is missing position-mode or rules evidence"
                )
            if current["trades_complete"] != 1:
                raise InvariantViolation(
                    "complete checkpoint requires complete trade pagination"
                )
            if current["next_trade_cursor"] is not None:
                raise InvariantViolation(
                    "complete checkpoint requires an exhausted trade cursor"
                )
        error = _safe_text(error, "checkpoint.error")
        cursor = self.connection.execute(
            """
            UPDATE recovery_checkpoints
               SET status = ?, completed_at_ms = ?, orders_seen = ?, fills_seen = ?,
                   mismatch_count = ?, error = ?
             WHERE checkpoint_id = ?
               AND status NOT IN ('COMPLETE', 'FAILED', 'BLOCKED')
            """,
            (
                status, completed_at_ms, orders_seen, fills_seen, mismatch_count,
                error, checkpoint_id,
            ),
        )
        if cursor.rowcount != 1:
            raise InvariantViolation("checkpoint is missing or already complete")
        return self.require(checkpoint_id)

    def latest_complete(self, strategy_id: str) -> Optional[Record]:
        return _row(self.connection.execute(
            """
            SELECT * FROM recovery_checkpoints
             WHERE strategy_id = ? AND status = 'COMPLETE'
             ORDER BY completed_at_ms DESC, checkpoint_id DESC LIMIT 1
            """,
            (strategy_id,),
        ).fetchone())

    def latest_phase2_replay_complete(self, strategy_id: str) -> Optional[Record]:
        """Return only a checkpoint that proves a complete Phase-2 REST replay."""

        return _row(self.connection.execute(
            """
            SELECT * FROM recovery_checkpoints
             WHERE strategy_id = ? AND status = 'COMPLETE'
               AND recovery_epoch > 0
               AND trades_complete = 1
               AND next_trade_cursor IS NULL
               AND fills_through_ms IS NOT NULL
               AND snapshot_observed_at_ms IS NOT NULL
               AND position_mode IS NOT NULL
               AND rules_hash IS NOT NULL
             ORDER BY completed_at_ms DESC, checkpoint_id DESC LIMIT 1
            """,
            (strategy_id,),
        ).fetchone())


class EventRepository(BaseRepository):
    table = "events"
    primary_key = "event_id"
    allowed_fields = {
        "event_id", "source", "dedupe_key", "event_type", "aggregate_type",
        "aggregate_id", "strategy_id", "order_id", "exchange_event_ms",
        "received_at_ms", "payload_json", "payload_hash", "processing_state",
        "processed_at_ms", "correlation_id", "causation_event_id", "error",
    }
    required_fields = {"source", "dedupe_key", "event_type", "received_at_ms", "payload_json"}

    def append(self, record: Mapping[str, Any]) -> Tuple[Record, bool]:
        values = self._prepare(record)
        values.pop("event_id", None)
        payload = values["payload_json"]
        if not isinstance(payload, str):
            payload = _safe_json(payload)
        else:
            # Parse string payloads so sensitive keys cannot bypass recursive validation.
            import json

            payload = _safe_json(json.loads(payload))
        values["payload_json"] = payload
        computed_hash = hashlib.sha256(payload.encode("utf-8")).hexdigest()
        supplied_hash = values.get("payload_hash")
        if supplied_hash is not None and supplied_hash != computed_hash:
            raise InvariantViolation("payload_hash does not match canonical payload_json")
        values["payload_hash"] = computed_hash
        values["error"] = _safe_text(values.get("error"), "event.error")
        existing = _row(self.connection.execute(
            "SELECT * FROM events WHERE source = ? AND dedupe_key = ?",
            (values["source"], values["dedupe_key"]),
        ).fetchone())
        if existing is not None:
            if existing["payload_hash"] != values["payload_hash"]:
                raise InvariantViolation("event dedupe key was reused with different payload")
            return existing, False
        columns = list(values)
        try:
            cursor = self.connection.execute(
                "INSERT INTO events ({}) VALUES ({})".format(
                    ", ".join(columns), ", ".join("?" for _ in columns)
                ),
                tuple(values[column] for column in columns),
            )
        except sqlite3.IntegrityError as exc:
            raise ConstraintViolation(str(exc)) from exc
        return self.require(cursor.lastrowid), True

    def mark_processed(
        self,
        event_id: int,
        state: str,
        processed_at_ms: int,
        error: Optional[str] = None,
    ) -> Record:
        error = _safe_text(error, "event.error")
        cursor = self.connection.execute(
            """
            UPDATE events SET processing_state = ?, processed_at_ms = ?, error = ?
             WHERE event_id = ? AND processing_state = 'RECEIVED'
            """,
            (state, processed_at_ms, error, event_id),
        )
        if cursor.rowcount != 1:
            raise InvariantViolation("event was already processed or is missing")
        return self.require(event_id)


class BotRunRepository(BaseRepository):
    table = "bot_runs"
    primary_key = "run_id"
    allowed_fields = {
        "run_id", "instance_id", "status", "started_at_ms", "heartbeat_at_ms",
        "ended_at_ms", "recovery_required", "error",
    }
    required_fields = {"run_id", "instance_id", "status", "started_at_ms", "heartbeat_at_ms"}

    def add(self, record: Mapping[str, Any]) -> Record:
        values = dict(record)
        values["error"] = _safe_text(values.get("error"), "bot_run.error")
        return super().add(values)

    def heartbeat(self, run_id: str, heartbeat_at_ms: int, status: Optional[str] = None) -> Record:
        if status is None:
            cursor = self.connection.execute(
                "UPDATE bot_runs SET heartbeat_at_ms = ? WHERE run_id = ?",
                (heartbeat_at_ms, run_id),
            )
        else:
            cursor = self.connection.execute(
                "UPDATE bot_runs SET heartbeat_at_ms = ?, status = ? WHERE run_id = ?",
                (heartbeat_at_ms, status, run_id),
            )
        if cursor.rowcount != 1:
            raise NotFound("bot run {} was not found".format(run_id))
        return self.require(run_id)

    def finish(
        self,
        run_id: str,
        *,
        status: str,
        ended_at_ms: int,
        recovery_required: bool,
        error: Optional[str] = None,
    ) -> Record:
        error = _safe_text(error, "bot_run.error")
        cursor = self.connection.execute(
            """
            UPDATE bot_runs
               SET status = ?, heartbeat_at_ms = ?, ended_at_ms = ?,
                   recovery_required = ?, error = ?
             WHERE run_id = ?
            """,
            (status, ended_at_ms, ended_at_ms, int(recovery_required), error, run_id),
        )
        if cursor.rowcount != 1:
            raise NotFound("bot run {} was not found".format(run_id))
        return self.require(run_id)


class StrategyLeaseRepository:
    def __init__(self, connection: sqlite3.Connection) -> None:
        self.connection = connection

    def get(self, strategy_id: str) -> Optional[Record]:
        return _row(self.connection.execute(
            "SELECT * FROM strategy_leases WHERE strategy_id = ?", (strategy_id,)
        ).fetchone())

    def acquire(
        self,
        *,
        strategy_id: str,
        run_id: str,
        now_ms: int,
        expires_at_ms: int,
    ) -> Record:
        if expires_at_ms <= now_ms:
            raise ValueError("lease expiry must be later than acquisition time")
        existing = self.get(strategy_id)
        if existing is None:
            try:
                self.connection.execute(
                    """
                    INSERT INTO strategy_leases(
                        strategy_id, run_id, fencing_token, acquired_at_ms,
                        renewed_at_ms, expires_at_ms
                    ) VALUES (?, ?, 1, ?, ?, ?)
                    """,
                    (strategy_id, run_id, now_ms, now_ms, expires_at_ms),
                )
            except sqlite3.IntegrityError as exc:
                raise ConstraintViolation(str(exc)) from exc
            return self.get(strategy_id)  # type: ignore[return-value]

        if existing["released"] == 0 and existing["expires_at_ms"] > now_ms:
            raise LeaseUnavailable(
                "strategy {} is leased by run {}".format(strategy_id, existing["run_id"])
            )

        cursor = self.connection.execute(
            """
            UPDATE strategy_leases
                SET run_id = ?, fencing_token = fencing_token + 1,
                    acquired_at_ms = ?, renewed_at_ms = ?, expires_at_ms = ?,
                    released = 0
              WHERE strategy_id = ? AND fencing_token = ?
                AND (released = 1 OR expires_at_ms <= ?)
            """,
            (
                run_id, now_ms, now_ms, expires_at_ms, strategy_id,
                existing["fencing_token"], now_ms,
            ),
        )
        if cursor.rowcount != 1:
            raise LeaseUnavailable("strategy lease changed concurrently")
        return self.get(strategy_id)  # type: ignore[return-value]

    def require_owned(
        self,
        *,
        strategy_id: str,
        run_id: str,
        fencing_token: int,
        now_ms: int,
    ) -> Record:
        lease = self.get(strategy_id)
        if (
            lease is None
            or lease["run_id"] != run_id
            or lease["fencing_token"] != fencing_token
            or lease["released"] != 0
            or lease["expires_at_ms"] <= now_ms
        ):
            raise LeaseUnavailable(
                "strategy lease is missing, expired, or fenced by another owner"
            )
        return lease

    def renew(
        self,
        *,
        strategy_id: str,
        run_id: str,
        fencing_token: int,
        now_ms: int,
        expires_at_ms: int,
    ) -> Record:
        if expires_at_ms <= now_ms:
            raise ValueError("lease expiry must be later than renewal time")
        cursor = self.connection.execute(
            """
            UPDATE strategy_leases
               SET renewed_at_ms = ?, expires_at_ms = ?
             WHERE strategy_id = ? AND run_id = ? AND fencing_token = ?
               AND released = 0 AND expires_at_ms > ? AND renewed_at_ms <= ?
            """,
            (
                now_ms,
                expires_at_ms,
                strategy_id,
                run_id,
                fencing_token,
                now_ms,
                now_ms,
            ),
        )
        if cursor.rowcount != 1:
            raise LeaseUnavailable(
                "strategy lease is missing, expired, or fenced by another owner"
            )
        return self.get(strategy_id)  # type: ignore[return-value]

    def release(self, *, strategy_id: str, run_id: str, fencing_token: int) -> None:
        cursor = self.connection.execute(
            """
            UPDATE strategy_leases SET released = 1
             WHERE strategy_id = ? AND run_id = ? AND fencing_token = ?
               AND released = 0
            """,
            (strategy_id, run_id, fencing_token),
        )
        if cursor.rowcount != 1:
            raise LeaseUnavailable("lease ownership or fencing token does not match")


class SQLiteRepositories:
    """All repositories bound to one SQLite transaction/connection."""

    def __init__(self, connection: sqlite3.Connection) -> None:
        self.strategies = StrategyRepository(connection)
        self.grid_generations = GridGenerationRepository(connection)
        self.grid_levels = GridLevelRepository(connection)
        self.orders = OrderRepository(connection)
        self.fills = FillRepository(connection)
        self.positions = PositionRepository(connection)
        self.recovery_checkpoints = RecoveryCheckpointRepository(connection)
        self.events = EventRepository(connection)
        self.bot_runs = BotRunRepository(connection)
        self.strategy_leases = StrategyLeaseRepository(connection)
        self.instrument_rules = InstrumentRulesRepository(connection)
        self.position_mode_observations = PositionModeObservationRepository(connection)
        self.exchange_observations = ExchangeObservationRepository(connection)
        self.exchange_trade_observations = ExchangeTradeObservationRepository(connection)


__all__ = [
    "BotRunRepository",
    "EventRepository",
    "ExchangeTradeObservationRepository",
    "FillRepository",
    "GridGenerationRepository",
    "GridLevelRepository",
    "InstrumentRulesRepository",
    "OrderRepository",
    "PositionRepository",
    "PositionModeObservationRepository",
    "RecoveryCheckpointRepository",
    "SQLiteRepositories",
    "StrategyLeaseRepository",
    "StrategyRepository",
    "ExchangeObservationRepository",
]
