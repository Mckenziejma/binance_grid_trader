"""Single-flight, epoch-fenced REST recovery orchestration."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field, is_dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from threading import Condition, Lock, RLock
from types import MappingProxyType
from typing import Any, Callable, Mapping, Optional

from gridtrader.core.enums import ExchangeOrderStatus, ReadinessState
from gridtrader.core.readiness import ReadinessGate, RecoveryCheck
from gridtrader.exchange.errors import ExchangeNotFoundError, redact_message
from gridtrader.exchange.models import (
    ExchangeFill,
    ExchangeOrderSnapshot,
    InstrumentRules,
    PositionMode,
    TradePage,
)
from gridtrader.exchange.ports import ExchangePort
from gridtrader.orders.idempotency import (
    logical_slot_key_text,
    make_client_order_id,
)
from gridtrader.storage.ports import LeaseUnavailable, LedgerPort, NotFound

from .models import ReconciliationKind, ReconciliationReport
from .reconciler import Reconciler


class RecoveryInProgressError(RuntimeError):
    pass


class StaleRecoveryEpochError(RuntimeError):
    pass


class StaleRecoveryLeaseError(StaleRecoveryEpochError):
    pass


class RecoveryBlockedError(RuntimeError):
    pass


@dataclass(frozen=True)
class RecoveryRequest:
    run_id: str
    strategy_id: str
    account_id: str
    symbol: str
    expected_position_mode: PositionMode
    reason: str = "STARTUP"
    cursor: Optional[str] = None
    from_time: Optional[datetime] = None
    claimable_orders: Mapping[str, Mapping[str, Any]] = field(default_factory=dict)
    max_trade_pages: int = 10_000

    def __post_init__(self) -> None:
        for name in ("run_id", "strategy_id", "account_id", "symbol"):
            value = getattr(self, name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{name} must not be empty")
        if self.reason not in {"STARTUP", "WS_RECONNECT", "PERIODIC", "MANUAL"}:
            raise ValueError("unsupported recovery reason")
        if not isinstance(self.expected_position_mode, PositionMode):
            raise TypeError("expected_position_mode must be PositionMode")
        if self.from_time is not None and (
            self.from_time.tzinfo is None or self.from_time.utcoffset() is None
        ):
            raise ValueError("from_time must be timezone-aware")
        if self.max_trade_pages < 1:
            raise ValueError("max_trade_pages must be positive")
        if not isinstance(self.claimable_orders, Mapping):
            raise TypeError("claimable_orders must be a mapping")
        frozen_claims: dict[str, Mapping[str, Any]] = {}
        for client_id, claim in self.claimable_orders.items():
            if not isinstance(client_id, str) or not client_id:
                raise ValueError("claimable order key must not be empty")
            if not isinstance(claim, Mapping):
                raise TypeError("each claimable order must be a mapping")
            frozen_claims[client_id] = MappingProxyType(dict(claim))
        object.__setattr__(
            self,
            "claimable_orders",
            MappingProxyType(frozen_claims),
        )


@dataclass(frozen=True)
class RecoveryResult:
    recovery_epoch: int
    checkpoint_id: int
    complete: bool
    state: ReadinessState
    report: ReconciliationReport
    blockers: tuple[str, ...]


@dataclass(frozen=True)
class _RecoveryLease:
    strategy_id: str
    run_id: str
    fencing_token: int


class RecoveryManager:
    """Observe Binance facts and atomically reconcile them into the ledger."""

    _registry_guard = Lock()
    _singleflight: dict[tuple[str, str], Lock] = {}

    def __init__(
        self,
        *,
        exchange: ExchangePort,
        ledger: LedgerPort,
        readiness: ReadinessGate,
        reconciler: Reconciler | None = None,
        clock: Callable[[], datetime] | None = None,
        lease_ttl: timedelta = timedelta(seconds=60),
    ) -> None:
        if not isinstance(lease_ttl, timedelta) or lease_ttl <= timedelta(0):
            raise ValueError("lease_ttl must be a positive timedelta")
        lease_ttl_ms = int(lease_ttl.total_seconds() * 1_000)
        if lease_ttl_ms < 1:
            raise ValueError("lease_ttl must be at least one millisecond")
        self.exchange = exchange
        self.ledger = ledger
        self.readiness = readiness
        self.reconciler = reconciler or Reconciler()
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._lease_ttl_ms = lease_ttl_ms
        self._lifecycle_guard = RLock()
        self._drain_condition = Condition(self._lifecycle_guard)
        self._active_recoveries = 0
        self._stopping = False

    @classmethod
    def _lock_for(cls, key: tuple[str, str]) -> Lock:
        with cls._registry_guard:
            return cls._singleflight.setdefault(key, Lock())

    def recover(self, request: RecoveryRequest) -> RecoveryResult:
        key = (request.account_id, request.strategy_id)
        flight = self._lock_for(key)
        if not flight.acquire(blocking=False):
            raise RecoveryInProgressError(
                "recovery is already running for this account/strategy"
            )
        checkpoint_id: int | None = None
        pages: list[TradePage] = []
        registered_active = False
        lease: _RecoveryLease | None = None
        try:
            with self._lifecycle_guard:
                if self._stopping:
                    raise RecoveryBlockedError("shutdown drain has started")
                self._active_recoveries += 1
                registered_active = True
            lease = self._acquire_recovery_lease(request)
            with self._lifecycle_guard:
                if self._stopping:
                    raise StaleRecoveryLeaseError(
                        "shutdown started before recovery acquired its epoch"
                    )
                epoch = self.readiness.begin_recovery(request.reason)

            (
                local_orders,
                local_positions,
                persisted_fills,
                resume_cursor,
                replay_from_time,
                prior_rules_hash,
                local_state_digest,
            ) = (
                self._load_local_state(request)
            )
            if request.cursor is not None:
                if resume_cursor is None or request.cursor != resume_cursor:
                    raise RecoveryBlockedError(
                        "trade cursor does not match the durable incomplete checkpoint"
                    )
                cursor = request.cursor
            else:
                cursor = resume_cursor

            requested_from_time = (
                None
                if request.from_time is None
                else request.from_time.astimezone(timezone.utc)
            )
            if (
                requested_from_time is not None
                and requested_from_time > replay_from_time
            ):
                raise RecoveryBlockedError(
                    "from_time cannot skip the durable replay boundary"
                )
            replay_start = (
                requested_from_time
                if requested_from_time is not None
                else replay_from_time
            )
            replay_from_boundary = replay_start - timedelta(minutes=1)
            resuming_old_snapshot = cursor is not None
            if cursor is not None:
                cursor = self._overlap_cursor(cursor)

            checkpoint_id = self._start_checkpoint(
                request,
                recovery_epoch=lease.fencing_token,
                lease=lease,
            )
            self._record_check(RecoveryCheck.LOCAL_STATE_LOADED, epoch)

            position_mode = self._fenced_exchange_read(
                request, lease, epoch, self.exchange.get_position_mode
            )
            rules = self._fenced_exchange_read(
                request,
                lease,
                epoch,
                lambda: self.exchange.get_instrument_rules(request.symbol),
            )
            rules_fingerprint = self._semantic_rules_hash(rules)
            self._record_check(RecoveryCheck.INSTRUMENT_RULES_SYNCED, epoch)
            open_orders = tuple(
                self._fenced_exchange_read(
                    request,
                    lease,
                    epoch,
                    lambda: self.exchange.get_open_orders(request.symbol),
                )
            )
            self._record_check(RecoveryCheck.OPEN_ORDERS_SYNCED, epoch)
            positions = tuple(
                self._fenced_exchange_read(
                    request,
                    lease,
                    epoch,
                    lambda: self.exchange.get_positions(request.symbol),
                )
            )
            self._record_check(RecoveryCheck.POSITIONS_SYNCED, epoch)
            margin = self._fenced_exchange_read(
                request, lease, epoch, self.exchange.get_margin_account_snapshot
            )

            while True:
                if len(pages) >= request.max_trade_pages:
                    raise RecoveryBlockedError("trade pagination exceeded its page bound")
                page = self._fenced_exchange_read(
                    request,
                    lease,
                    epoch,
                    lambda: self.exchange.get_user_trades(
                        request.symbol,
                        cursor=cursor,
                        from_time=(
                            None
                            if cursor is not None
                            else replay_start - timedelta(minutes=1)
                        ),
                    ),
                )
                pages.append(page)
                if page.complete:
                    if resuming_old_snapshot:
                        # A durable cursor belongs to the fixed REST snapshot
                        # that was interrupted.  Completing only that old
                        # snapshot would leave a time gap before this recovery
                        # and could miss offsetting fills.  Finish it first,
                        # then start one fresh bounded replay with overlap.
                        replay_start = (
                            requested_from_time
                            if requested_from_time is not None
                            and requested_from_time < page.snapshot_time
                            else page.snapshot_time
                        )
                        cursor = None
                        resuming_old_snapshot = False
                        continue
                    break
                if page.next_cursor is None or page.next_cursor == cursor:
                    raise RecoveryBlockedError("trade pagination did not advance")
                cursor = page.next_cursor
            # A second read fences a snapshot assembled while account-wide mode
            # or symbol filters were changing.
            confirmed_mode = self._fenced_exchange_read(
                request, lease, epoch, self.exchange.get_position_mode
            )
            confirmed_rules = self._fenced_exchange_read(
                request,
                lease,
                epoch,
                lambda: self.exchange.get_instrument_rules(request.symbol),
            )
            confirmed_rules_fingerprint = self._semantic_rules_hash(confirmed_rules)
            confirmed_open_orders = tuple(
                self._fenced_exchange_read(
                    request,
                    lease,
                    epoch,
                    lambda: self.exchange.get_open_orders(request.symbol),
                )
            )
            confirmed_positions = tuple(
                self._fenced_exchange_read(
                    request,
                    lease,
                    epoch,
                    lambda: self.exchange.get_positions(request.symbol),
                )
            )
            confirmed_margin = self._fenced_exchange_read(
                request, lease, epoch, self.exchange.get_margin_account_snapshot
            )

            # Close the trade-history tail that opened after the first fixed
            # userTrades snapshot.  This second replay has its own fixed
            # server watermark and overlaps the prior boundary deliberately.
            tail_cursor: Optional[str] = None
            tail_from_time = pages[-1].snapshot_time - timedelta(minutes=1)
            while True:
                if len(pages) >= request.max_trade_pages:
                    raise RecoveryBlockedError(
                        "trade pagination exceeded its page bound"
                    )
                tail_page = self._fenced_exchange_read(
                    request,
                    lease,
                    epoch,
                    lambda: self.exchange.get_user_trades(
                        request.symbol,
                        cursor=tail_cursor,
                        from_time=(
                            None if tail_cursor is not None else tail_from_time
                        ),
                    ),
                )
                pages.append(tail_page)
                if tail_page.complete:
                    break
                if (
                    tail_page.next_cursor is None
                    or tail_page.next_cursor == tail_cursor
                ):
                    raise RecoveryBlockedError(
                        "trade pagination did not advance"
                    )
                tail_cursor = tail_page.next_cursor
            # Facts after the final trade watermark prove the replay and the
            # account snapshot converged.  Any movement requires a fresh
            # recovery instead of mixing observations from different states.
            final_mode = self._fenced_exchange_read(
                request, lease, epoch, self.exchange.get_position_mode
            )
            final_rules = self._fenced_exchange_read(
                request,
                lease,
                epoch,
                lambda: self.exchange.get_instrument_rules(request.symbol),
            )
            final_rules_fingerprint = self._semantic_rules_hash(final_rules)
            final_open_orders = tuple(
                self._fenced_exchange_read(
                    request,
                    lease,
                    epoch,
                    lambda: self.exchange.get_open_orders(request.symbol),
                )
            )
            final_positions = tuple(
                self._fenced_exchange_read(
                    request,
                    lease,
                    epoch,
                    lambda: self.exchange.get_positions(request.symbol),
                )
            )
            final_margin = self._fenced_exchange_read(
                request, lease, epoch, self.exchange.get_margin_account_snapshot
            )

            # Put the committed REST boundary after the final account-fact
            # reads.  This closing overlap captures fills that happened while
            # those reads were in flight (including economically offsetting
            # fills that a net position snapshot cannot reveal).
            closing_cursor: Optional[str] = None
            closing_from_time = pages[-1].snapshot_time - timedelta(minutes=1)
            while True:
                if len(pages) >= request.max_trade_pages:
                    raise RecoveryBlockedError(
                        "trade pagination exceeded its page bound"
                    )
                closing_page = self._fenced_exchange_read(
                    request,
                    lease,
                    epoch,
                    lambda: self.exchange.get_user_trades(
                        request.symbol,
                        cursor=closing_cursor,
                        from_time=(
                            None
                            if closing_cursor is not None
                            else closing_from_time
                        ),
                    ),
                )
                pages.append(closing_page)
                if closing_page.complete:
                    break
                if (
                    closing_page.next_cursor is None
                    or closing_page.next_cursor == closing_cursor
                ):
                    raise RecoveryBlockedError(
                        "trade pagination did not advance"
                    )
                closing_cursor = closing_page.next_cursor

            # Re-read every account fact after the closing trade watermark.
            # The closing replay may itself observe a fill that happened after
            # ``final_positions`` (or another final account read).  Reconciling
            # that fill against the pre-closing facts could otherwise publish a
            # false READY result.  These settled facts are both compared with
            # the pre-closing set and used as the authoritative persistence
            # input below.
            settled_mode = self._fenced_exchange_read(
                request, lease, epoch, self.exchange.get_position_mode
            )
            settled_rules = self._fenced_exchange_read(
                request,
                lease,
                epoch,
                lambda: self.exchange.get_instrument_rules(request.symbol),
            )
            settled_rules_fingerprint = self._semantic_rules_hash(settled_rules)
            settled_open_orders = tuple(
                self._fenced_exchange_read(
                    request,
                    lease,
                    epoch,
                    lambda: self.exchange.get_open_orders(request.symbol),
                )
            )
            settled_positions = tuple(
                self._fenced_exchange_read(
                    request,
                    lease,
                    epoch,
                    lambda: self.exchange.get_positions(request.symbol),
                )
            )
            settled_margin = self._fenced_exchange_read(
                request, lease, epoch, self.exchange.get_margin_account_snapshot
            )
            self._record_check(RecoveryCheck.USER_TRADES_SYNCED, epoch)

            snapshot_blockers: list[str] = []
            if confirmed_mode.mode is not position_mode.mode:
                snapshot_blockers.append("POSITION_MODE_CHANGED_DURING_RECOVERY")
            if final_mode.mode is not confirmed_mode.mode:
                snapshot_blockers.append("POSITION_MODE_CHANGED_DURING_TAIL_REPLAY")
            if settled_mode.mode is not final_mode.mode:
                snapshot_blockers.append(
                    "POSITION_MODE_CHANGED_DURING_CLOSING_REPLAY"
                )
            if confirmed_rules_fingerprint != rules_fingerprint:
                snapshot_blockers.append("INSTRUMENT_RULES_CHANGED_DURING_RECOVERY")
            if final_rules_fingerprint != confirmed_rules_fingerprint:
                snapshot_blockers.append(
                    "INSTRUMENT_RULES_CHANGED_DURING_TAIL_REPLAY"
                )
            if settled_rules_fingerprint != final_rules_fingerprint:
                snapshot_blockers.append(
                    "INSTRUMENT_RULES_CHANGED_DURING_CLOSING_REPLAY"
                )
            if self._order_facts_hash(confirmed_open_orders) != self._order_facts_hash(
                open_orders
            ):
                snapshot_blockers.append("OPEN_ORDERS_CHANGED_DURING_RECOVERY")
            if self._order_facts_hash(final_open_orders) != self._order_facts_hash(
                confirmed_open_orders
            ):
                snapshot_blockers.append("OPEN_ORDERS_CHANGED_DURING_TAIL_REPLAY")
            if self._order_facts_hash(
                settled_open_orders
            ) != self._order_facts_hash(final_open_orders):
                snapshot_blockers.append(
                    "OPEN_ORDERS_CHANGED_DURING_CLOSING_REPLAY"
                )
            if self._position_facts_hash(
                confirmed_positions
            ) != self._position_facts_hash(positions):
                snapshot_blockers.append("POSITIONS_CHANGED_DURING_RECOVERY")
            if self._position_facts_hash(
                final_positions
            ) != self._position_facts_hash(confirmed_positions):
                snapshot_blockers.append("POSITIONS_CHANGED_DURING_TAIL_REPLAY")
            if self._position_facts_hash(
                settled_positions
            ) != self._position_facts_hash(final_positions):
                snapshot_blockers.append(
                    "POSITIONS_CHANGED_DURING_CLOSING_REPLAY"
                )
            if self._margin_facts_hash(
                confirmed_margin
            ) != self._margin_facts_hash(margin):
                snapshot_blockers.append("MARGIN_ACCOUNT_CHANGED_DURING_RECOVERY")
            if self._margin_facts_hash(
                final_margin
            ) != self._margin_facts_hash(confirmed_margin):
                snapshot_blockers.append(
                    "MARGIN_ACCOUNT_CHANGED_DURING_TAIL_REPLAY"
                )
            if self._margin_facts_hash(
                settled_margin
            ) != self._margin_facts_hash(final_margin):
                snapshot_blockers.append(
                    "MARGIN_ACCOUNT_CHANGED_DURING_CLOSING_REPLAY"
                )

            all_fills = tuple(fill for page in pages for fill in page.items)
            exact_orders = self._resolve_missing_orders(
                local_orders,
                settled_open_orders,
                symbol=request.symbol,
                claimable_orders=request.claimable_orders,
                fills=all_fills,
                recovery_epoch=epoch,
                request=request,
                lease=lease,
            )
            report = self.reconciler.reconcile(
                local_orders=local_orders,
                open_orders=settled_open_orders,
                exact_orders=exact_orders,
                fills=all_fills,
                local_positions=local_positions,
                exchange_positions=settled_positions,
                claimable_orders=request.claimable_orders,
                persisted_fills=persisted_fills,
                project_bot_positions=True,
            )

            blockers = list(report.blockers)
            blockers.extend(snapshot_blockers)
            blockers.extend(self._instrument_rules_blockers(request, settled_rules))
            if settled_mode.mode is not request.expected_position_mode:
                blockers.append(
                    "POSITION_MODE_MISMATCH:expected={} observed={}".format(
                        request.expected_position_mode.value,
                        settled_mode.mode.value,
                    )
                )
            if request.expected_position_mode is PositionMode.HEDGE:
                blockers.append("HEDGE_POSITION_BASELINE_UNSUPPORTED_PHASE2")
            if (
                prior_rules_hash is not None
                and prior_rules_hash != settled_rules_fingerprint
            ):
                blockers.append("INSTRUMENT_RULES_CHANGED")
            if any(
                item.kind is ReconciliationKind.AMBIGUOUS
                for item in report.orders
            ):
                blockers.append("ACK_UNKNOWN_OR_ORDER_HISTORY_UNRESOLVED")
            blockers.extend(
                self._position_mode_blockers(settled_mode.mode, report=report)
            )

            self._assert_epoch(epoch)
            checkpoint = self._persist_complete_snapshot(
                request=request,
                epoch=epoch,
                checkpoint_id=checkpoint_id,
                rules=settled_rules,
                position_mode=settled_mode,
                open_orders=settled_open_orders,
                positions=settled_positions,
                margin=settled_margin,
                pages=pages,
                report=report,
                blockers=tuple(dict.fromkeys(blockers)),
                replay_from_time=replay_from_boundary,
                lease=lease,
                local_state_digest=local_state_digest,
            )
            final_blockers = tuple(dict.fromkeys(blockers))
            # The SQLite commit can outlive an earlier lease heartbeat.  A
            # fenced/expired owner must never publish READY from memory after
            # another process has taken over the same strategy.
            self._renew_recovery_lease(lease)
            with self._lifecycle_guard:
                self._assert_epoch(epoch)
                if final_blockers:
                    self.readiness.degrade("; ".join(final_blockers))
                else:
                    self.readiness.record_check(
                        RecoveryCheck.ACK_UNKNOWN_RESOLVED, recovery_epoch=epoch
                    )
                    self.readiness.record_check(
                        RecoveryCheck.INVARIANTS_VALIDATED, recovery_epoch=epoch
                    )
                    self.readiness.record_check(
                        RecoveryCheck.CHECKPOINT_COMMITTED, recovery_epoch=epoch
                    )
                    self.readiness.mark_ready(recovery_epoch=epoch)
            return RecoveryResult(
                recovery_epoch=lease.fencing_token,
                checkpoint_id=int(checkpoint["checkpoint_id"]),
                complete=not final_blockers,
                state=self.readiness.state,
                report=report,
                blockers=final_blockers,
            )
        except Exception as exc:
            if checkpoint_id is not None and lease is not None:
                self._persist_interrupted_pages(
                    request, checkpoint_id, pages, exc, lease=lease
                )
            if self.readiness.state is ReadinessState.RECOVERING:
                self.readiness.degrade(self._safe_error(exc))
            raise
        finally:
            if registered_active:
                with self._drain_condition:
                    self._active_recoveries -= 1
                    self._drain_condition.notify_all()
            try:
                if lease is not None:
                    self._release_recovery_lease(lease)
            finally:
                flight.release()

    def begin_shutdown(self) -> None:
        """Fence in-flight recovery; this method never cancels exchange orders."""

        with self._lifecycle_guard:
            self._stopping = True
            if self.readiness.state not in {
                ReadinessState.STOPPING,
                ReadinessState.STOPPED,
            }:
                self.readiness.begin_stopping("shutdown drain")

    def wait_for_shutdown_drain(self, timeout: float | None = None) -> bool:
        """Wait until fenced recovery work exits without performing writes."""

        with self._drain_condition:
            if not self._stopping:
                raise RecoveryBlockedError("shutdown drain has not started")
            return self._drain_condition.wait_for(
                lambda: self._active_recoveries == 0,
                timeout=timeout,
            )

    def finish_shutdown(self, timeout: float | None = None) -> bool:
        """Drain recovery and move STOPPING to STOPPED; never cancels orders."""

        if not self.wait_for_shutdown_drain(timeout):
            return False
        with self._lifecycle_guard:
            if self.readiness.state is ReadinessState.STOPPING:
                self.readiness.mark_stopped()
        return True

    def _load_local_state(
        self, request: RecoveryRequest
    ) -> tuple[
        list[dict[str, Any]],
        list[dict[str, Any]],
        list[dict[str, Any]],
        Optional[str],
        datetime,
        Optional[str],
        str,
    ]:
        with self.ledger.unit_of_work(immediate=False) as uow:
            strategy = uow.strategies.require(request.strategy_id)
            if (
                strategy["account_id"] != request.account_id
                or strategy["symbol"] != request.symbol
            ):
                raise RecoveryBlockedError(
                    "recovery request does not match the persisted strategy scope"
                )
            exchange_account = getattr(self.exchange, "account_id", None)
            if exchange_account is not None and exchange_account != request.account_id:
                raise RecoveryBlockedError(
                    "recovery request account does not match the exchange adapter"
                )
            orders = uow.orders.list_for_strategy(request.strategy_id)
            for order in orders:
                if (
                    order["strategy_id"] != request.strategy_id
                    or order["account_id"] != request.account_id
                    or order["symbol"] != request.symbol
                ):
                    raise RecoveryBlockedError(
                        "local order does not match the recovery strategy scope"
                    )
                try:
                    generation = uow.grid_generations.require(
                        str(order["generation_id"])
                    )
                    level = uow.grid_levels.require(str(order["level_id"]))
                except NotFound as exc:
                    raise RecoveryBlockedError(
                        "local order references an unknown generation or level"
                    ) from exc
                if (
                    generation["strategy_id"] != request.strategy_id
                    or level["generation_id"] != generation["generation_id"]
                ):
                    raise RecoveryBlockedError(
                        "local order generation or level belongs to another strategy"
                    )
                if (
                    order["ownership"] == "BOT"
                    and order["local_state"] != "terminal"
                ):
                    self._validate_local_owned_order(
                        request,
                        strategy,
                        generation,
                        level,
                        order,
                    )
            self._validate_claimable_orders(uow, request)
            # ``positions`` is only a cache of Binance observations.  The
            # strategy expectation starts from its immutable configured
            # baseline and is rebuilt from owned fills by Reconciler.
            positions = [
                {
                    "account_id": request.account_id,
                    "symbol": request.symbol,
                    "position_side": "both",
                    "quantity_contracts": int(
                        strategy["initial_position_contracts"]
                    ),
                }
            ]
            fills = uow.fills.list_for_strategy_symbol(
                request.strategy_id,
                request.account_id,
                request.symbol,
            )
            latest = uow.recovery_checkpoints.latest_for_strategy(request.strategy_id)
            latest_complete = (
                uow.recovery_checkpoints.latest_phase2_replay_complete(
                    request.strategy_id
                )
            )
            if latest is not None and (
                latest["account_id"] != request.account_id
                or latest["symbol"] != request.symbol
            ):
                latest = None
            if latest_complete is not None and (
                latest_complete["account_id"] != request.account_id
                or latest_complete["symbol"] != request.symbol
            ):
                latest_complete = None
            resume = (
                latest["next_trade_cursor"]
                if latest is not None and latest["trades_complete"] == 0
                else None
            )
            replay_from_ms = (
                latest_complete["fills_through_ms"]
                if latest_complete is not None
                and latest_complete["fills_through_ms"] is not None
                else strategy["created_at_ms"]
            )
            replay_from_time = datetime.fromtimestamp(
                int(replay_from_ms) / 1_000,
                tz=timezone.utc,
            )
            rules_hash = None if latest_complete is None else latest_complete["rules_hash"]
            local_state_digest = self._local_recovery_state_hash(uow, request)
        return (
            orders,
            positions,
            fills,
            resume,
            replay_from_time,
            rules_hash,
            local_state_digest,
        )

    @classmethod
    def _local_recovery_state_hash(
        cls,
        uow: Any,
        request: RecoveryRequest,
    ) -> str:
        """Fingerprint every durable strategy input used by reconciliation."""

        strategy = uow.strategies.require(request.strategy_id)
        generations = sorted(
            (
                item
                for item in uow.grid_generations.list_all()
                if item["strategy_id"] == request.strategy_id
            ),
            key=lambda item: str(item["generation_id"]),
        )
        generation_ids = {
            str(item["generation_id"])
            for item in generations
        }
        levels = sorted(
            (
                item
                for item in uow.grid_levels.list_all()
                if str(item["generation_id"]) in generation_ids
            ),
            key=lambda item: str(item["level_id"]),
        )
        orders = uow.orders.list_for_strategy(
            request.strategy_id,
            symbol=request.symbol,
        )
        fills = uow.fills.list_for_strategy_symbol(
            request.strategy_id,
            request.account_id,
            request.symbol,
        )
        return cls._hash(
            {
                "strategy": strategy,
                "generations": generations,
                "levels": levels,
                "orders": orders,
                "fills": fills,
            }
        )

    @staticmethod
    def _validate_local_owned_order(
        request: RecoveryRequest,
        strategy: Mapping[str, Any],
        generation: Mapping[str, Any],
        level: Mapping[str, Any],
        order: Mapping[str, Any],
    ) -> None:
        """Fail before REST I/O if a live BOT slot has lost its meaning."""

        if generation["status"] not in {"active", "draining"}:
            raise RecoveryBlockedError(
                "non-terminal BOT order generation is not active or draining"
            )
        if level["state"] not in {"armed", "active"}:
            raise RecoveryBlockedError(
                "non-terminal BOT order level is not armed or active"
            )
        cycle_no = order["cycle_no"]
        if (
            isinstance(cycle_no, bool)
            or not isinstance(cycle_no, int)
            or cycle_no != int(level["cycle_no"])
        ):
            raise RecoveryBlockedError(
                "non-terminal BOT order cycle does not match its grid level"
            )
        expected_client_id = make_client_order_id(
            str(order["strategy_id"]),
            str(order["generation_id"]),
            str(order["level_id"]),
            cycle_no,
            str(order["leg_role"]),
        )
        expected_slot = logical_slot_key_text(
            str(order["strategy_id"]),
            str(order["generation_id"]),
            str(order["level_id"]),
            cycle_no,
            str(order["leg_role"]),
        )
        if (
            order["client_order_id"] != expected_client_id
            or order["logical_slot_key"] != expected_slot
        ):
            raise RecoveryBlockedError(
                "non-terminal BOT order identity is not its deterministic slot"
            )
        try:
            order_price = Decimal(str(order["price"]))
            level_price = Decimal(str(level["price"]))
        except (InvalidOperation, TypeError, ValueError) as exc:
            raise RecoveryBlockedError(
                "non-terminal BOT order price is invalid"
            ) from exc
        if not order_price.is_finite() or order_price != level_price:
            raise RecoveryBlockedError(
                "non-terminal BOT order price does not match its grid level"
            )
        quantity = order["quantity_contracts"]
        if (
            isinstance(quantity, bool)
            or not isinstance(quantity, int)
            or quantity != int(level["planned_contracts"])
        ):
            raise RecoveryBlockedError(
                "non-terminal BOT order quantity does not match its grid level"
            )
        if (
            str(order["order_type"]).upper() != "LIMIT"
            or str(order["time_in_force"]).upper() != "GTC"
        ):
            raise RecoveryBlockedError(
                "non-terminal BOT grid order must be LIMIT/GTC"
            )
        expected_economics = {
            "grid_buy": ("GRID_BUY", "buy"),
            "grid_sell": ("GRID_SELL", "sell"),
        }.get(str(order["leg_role"]).lower())
        if expected_economics is None:
            raise RecoveryBlockedError(
                "non-terminal BOT order role has no Phase-2 economic proof"
            )
        expected_intent, expected_side = expected_economics
        if (
            str(order["intent"]).upper() != expected_intent
            or str(order["side"]).lower() != expected_side
            or str(order["position_side"]).lower() != "both"
            or bool(order["reduce_only"])
        ):
            raise RecoveryBlockedError(
                "non-terminal BOT order economics conflict with its grid role"
            )
        if generation["strategy_mode"] != strategy["mode"]:
            raise RecoveryBlockedError(
                "non-terminal BOT order generation mode conflicts with strategy"
            )
        if (
            strategy["mode"] != "neutral"
            or request.expected_position_mode is not PositionMode.ONE_WAY
        ):
            raise RecoveryBlockedError(
                "Phase-2 cannot prove this non-terminal BOT order mode"
            )

    @staticmethod
    def _validate_claimable_orders(uow: Any, request: RecoveryRequest) -> None:
        """Prove every recovery claim belongs to this persisted logical slot."""

        strategy = uow.strategies.require(request.strategy_id)
        required = {
            "local_order_id",
            "strategy_id",
            "generation_id",
            "level_id",
            "account_id",
            "symbol",
            "logical_slot_key",
            "cycle_no",
            "leg_role",
            "intent",
            "client_order_id",
            "side",
            "position_side",
            "order_type",
            "time_in_force",
            "reduce_only",
            "price",
            "quantity_contracts",
        }
        for client_id, source in request.claimable_orders.items():
            if not isinstance(client_id, str) or not client_id:
                raise RecoveryBlockedError("claimable order key must not be empty")
            if not isinstance(source, Mapping):
                raise RecoveryBlockedError("claimable order must be a mapping")
            claim = dict(source)
            missing = sorted(field for field in required if claim.get(field) is None)
            if missing:
                raise RecoveryBlockedError(
                    "claimable order is missing required fields: " + ", ".join(missing)
                )
            if (
                claim["strategy_id"] != request.strategy_id
                or claim["account_id"] != request.account_id
                or claim["symbol"] != request.symbol
            ):
                raise RecoveryBlockedError(
                    "claimable order does not match the recovery strategy scope"
                )
            cycle_no = claim["cycle_no"]
            if (
                isinstance(cycle_no, bool)
                or not isinstance(cycle_no, int)
                or cycle_no < 0
            ):
                raise RecoveryBlockedError(
                    "claimable order cycle_no must be a non-negative integer"
                )
            expected_client_id = make_client_order_id(
                str(claim["strategy_id"]),
                str(claim["generation_id"]),
                str(claim["level_id"]),
                cycle_no,
                str(claim["leg_role"]),
            )
            expected_slot = logical_slot_key_text(
                str(claim["strategy_id"]),
                str(claim["generation_id"]),
                str(claim["level_id"]),
                cycle_no,
                str(claim["leg_role"]),
            )
            if (
                client_id != expected_client_id
                or claim["client_order_id"] != expected_client_id
                or claim["logical_slot_key"] != expected_slot
            ):
                raise RecoveryBlockedError(
                    "claimable order identity is not the deterministic logical slot"
                )
            if (
                str(claim["order_type"]).upper() != "LIMIT"
                or str(claim["time_in_force"]).upper() != "GTC"
            ):
                raise RecoveryBlockedError(
                    "claimable grid order must be LIMIT with GTC time in force"
                )
            try:
                generation = uow.grid_generations.require(
                    str(claim["generation_id"])
                )
                level = uow.grid_levels.require(str(claim["level_id"]))
            except NotFound as exc:
                raise RecoveryBlockedError(
                    "claimable order references an unknown generation or level"
                ) from exc
            if (
                generation["strategy_id"] != request.strategy_id
                or level["generation_id"] != generation["generation_id"]
            ):
                raise RecoveryBlockedError(
                    "claimable order generation or level belongs to another strategy"
                )
            if generation["status"] not in {"active", "draining"}:
                raise RecoveryBlockedError(
                    "claimable order generation is not active or draining"
                )
            if level["state"] not in {"armed", "active"}:
                raise RecoveryBlockedError(
                    "claimable order level is not armed or active"
                )
            if cycle_no != int(level["cycle_no"]):
                raise RecoveryBlockedError(
                    "claimable order cycle does not match the durable grid level"
                )
            try:
                claim_price = Decimal(str(claim["price"]))
                level_price = Decimal(str(level["price"]))
            except (InvalidOperation, TypeError, ValueError) as exc:
                raise RecoveryBlockedError(
                    "claimable order price is not a valid decimal"
                ) from exc
            if not claim_price.is_finite() or claim_price != level_price:
                raise RecoveryBlockedError(
                    "claimable order price does not match the durable grid level"
                )
            quantity = claim["quantity_contracts"]
            if (
                isinstance(quantity, bool)
                or not isinstance(quantity, int)
                or quantity != int(level["planned_contracts"])
            ):
                raise RecoveryBlockedError(
                    "claimable order quantity does not match the durable grid level"
                )
            if generation["strategy_mode"] != strategy["mode"]:
                raise RecoveryBlockedError(
                    "claimable order generation mode conflicts with the strategy"
                )
            expected_economics = {
                "grid_buy": ("GRID_BUY", "buy"),
                "grid_sell": ("GRID_SELL", "sell"),
            }.get(str(claim["leg_role"]).lower())
            if expected_economics is None:
                raise RecoveryBlockedError(
                    "claimable order leg role has no Phase-2 economic proof"
                )
            expected_intent, expected_side = expected_economics
            reduce_only = claim["reduce_only"]
            reduce_only_is_false = reduce_only is False or (
                isinstance(reduce_only, int)
                and not isinstance(reduce_only, bool)
                and reduce_only == 0
            )
            if (
                str(claim["intent"]).upper() != expected_intent
                or str(claim["side"]).lower() != expected_side
                or str(claim["position_side"]).lower() != "both"
                or not reduce_only_is_false
            ):
                raise RecoveryBlockedError(
                    "claimable order economics do not match its durable grid role"
                )
            if (
                strategy["mode"] != "neutral"
                or request.expected_position_mode is not PositionMode.ONE_WAY
            ):
                raise RecoveryBlockedError(
                    "Phase-2 cannot prove claim economics for this strategy/mode"
                )

    def _instrument_rules_blockers(
        self,
        request: RecoveryRequest,
        rules: InstrumentRules,
    ) -> tuple[str, ...]:
        with self.ledger.unit_of_work(immediate=False) as uow:
            return self._instrument_rules_blockers_in_uow(
                uow,
                request,
                rules,
            )

    @staticmethod
    def _instrument_rules_blockers_in_uow(
        uow: Any,
        request: RecoveryRequest,
        rules: InstrumentRules,
    ) -> tuple[str, ...]:
        """Validate every runnable local intent against current COIN-M rules."""

        blockers: list[str] = []

        def block(scope: str, identity: str, detail: str) -> None:
            blockers.append(
                "INSTRUMENT_RULES_INCOMPATIBLE:{}:{}:{}".format(
                    scope,
                    identity,
                    detail.replace(";", ","),
                )
            )

        if rules.symbol != request.symbol:
            block("INSTRUMENT", request.symbol, "symbol")
        if rules.contract_type.upper() != "PERPETUAL":
            block("INSTRUMENT", request.symbol, "contract_type")
        if rules.status.upper() != "TRADING":
            block("INSTRUMENT", request.symbol, "status")
        if "LIMIT" not in {value.upper() for value in rules.supported_order_types}:
            block("INSTRUMENT", request.symbol, "LIMIT unsupported")

        strategy = uow.strategies.require(request.strategy_id)
        generations = [
            item
            for item in uow.grid_generations.list_all()
            if item["strategy_id"] == request.strategy_id
            and item["status"] in {"active", "draining"}
        ]
        generation_by_id = {
            str(item["generation_id"]): item for item in generations
        }
        levels_by_id: dict[str, Mapping[str, Any]] = {}
        for generation in generations:
            generation_id = str(generation["generation_id"])
            order_contracts = generation["order_contracts"]
            for field_name in ("lower_price", "upper_price"):
                try:
                    rules.validate_order(
                        Decimal(str(generation[field_name])),
                        order_contracts,
                    )
                except (TypeError, ValueError) as exc:
                    block(
                        "GRID_GENERATION",
                        generation_id,
                        f"{field_name}:{exc}",
                    )
            if generation["spacing_mode"] == "arithmetic":
                try:
                    step = Decimal(str(generation["arithmetic_step"]))
                    if not step.is_finite() or step <= 0:
                        raise ValueError("arithmetic_step must be positive")
                    if step % rules.price_tick != 0:
                        raise ValueError(
                            "arithmetic_step is not aligned to price_tick"
                        )
                except (InvalidOperation, TypeError, ValueError) as exc:
                    block(
                        "GRID_GENERATION",
                        generation_id,
                        f"arithmetic_step:{exc}",
                    )
            if generation["strategy_mode"] != strategy["mode"]:
                block("GRID_GENERATION", generation_id, "strategy_mode")

            for level in uow.grid_levels.list_for_generation(generation_id):
                levels_by_id[str(level["level_id"])] = level
                if level["state"] not in {"armed", "active"}:
                    continue
                try:
                    rules.validate_order(
                        Decimal(str(level["price"])),
                        level["planned_contracts"],
                    )
                except (InvalidOperation, TypeError, ValueError) as exc:
                    block(
                        "GRID_LEVEL",
                        str(level["level_id"]),
                        str(exc),
                    )

        for order in uow.orders.list_for_strategy(request.strategy_id):
            if order["ownership"] != "BOT" or order["local_state"] == "terminal":
                continue
            client_id = str(order["client_order_id"])
            generation = generation_by_id.get(str(order["generation_id"]))
            level = levels_by_id.get(str(order["level_id"]))
            if generation is None:
                block("ORDER", client_id, "generation is not active or draining")
            if level is None or level["state"] not in {"armed", "active"}:
                block("ORDER", client_id, "level is not armed or active")
            try:
                rules.validate_order(
                    Decimal(str(order["price"])),
                    order["quantity_contracts"],
                )
            except (InvalidOperation, TypeError, ValueError) as exc:
                block("ORDER", client_id, str(exc))

        for client_id, claim in request.claimable_orders.items():
            try:
                rules.validate_order(
                    Decimal(str(claim["price"])),
                    claim["quantity_contracts"],
                )
            except (InvalidOperation, KeyError, TypeError, ValueError) as exc:
                block("CLAIM", client_id, str(exc))

        return tuple(dict.fromkeys(blockers))

    def _start_checkpoint(
        self,
        request: RecoveryRequest,
        *,
        recovery_epoch: int,
        lease: _RecoveryLease,
    ) -> int:
        now_ms = self._milliseconds(self._now())
        with self.ledger.unit_of_work() as uow:
            self._renew_lease_in_uow(uow, lease, now_ms=now_ms)
            checkpoint = uow.recovery_checkpoints.add(
                {
                    "run_id": request.run_id,
                    "strategy_id": request.strategy_id,
                    "account_id": request.account_id,
                    "symbol": request.symbol,
                    "reason": request.reason,
                    "status": "STARTED",
                    "recovery_epoch": recovery_epoch,
                    "started_at_ms": now_ms,
                }
            )
            uow.commit()
        return int(checkpoint["checkpoint_id"])

    def _resolve_missing_orders(
        self,
        local_orders: list[dict[str, Any]],
        open_orders: tuple[ExchangeOrderSnapshot, ...],
        *,
        symbol: str,
        claimable_orders: Mapping[str, Mapping[str, Any]],
        fills: tuple[ExchangeFill, ...],
        recovery_epoch: int,
        request: RecoveryRequest,
        lease: _RecoveryLease,
    ) -> dict[str, ExchangeOrderSnapshot | None]:
        open_ids = {item.client_order_id for item in open_orders}
        exact: dict[str, ExchangeOrderSnapshot | None] = {}
        local_ids = {str(item["client_order_id"]) for item in local_orders}
        for local in local_orders:
            client_id = str(local["client_order_id"])
            if client_id in open_ids:
                continue
            try:
                exact[client_id] = self._fenced_exchange_read(
                    request,
                    lease,
                    recovery_epoch,
                    lambda: self.exchange.get_order_by_client_id(
                        str(local["symbol"]), client_id
                    ),
                )
            except ExchangeNotFoundError:
                exact[client_id] = None
        for client_id in sorted(claimable_orders):
            if client_id in open_ids or client_id in local_ids:
                continue
            try:
                exact[client_id] = self._fenced_exchange_read(
                    request,
                    lease,
                    recovery_epoch,
                    lambda: self.exchange.get_order_by_client_id(
                        symbol,
                        client_id,
                    ),
                )
            except ExchangeNotFoundError:
                exact[client_id] = None

        known_exchange_ids = {
            order.exchange_order_id for order in open_orders
        }.union(
            order.exchange_order_id
            for order in exact.values()
            if order is not None
        )
        for exchange_order_id in sorted(
            {
                fill.exchange_order_id
                for fill in fills
                if fill.exchange_order_id not in known_exchange_ids
            }
        ):
            try:
                order = self._fenced_exchange_read(
                    request,
                    lease,
                    recovery_epoch,
                    lambda: self.exchange.get_order_by_exchange_id(
                        symbol,
                        exchange_order_id,
                    ),
                )
            except ExchangeNotFoundError:
                continue
            if order.client_order_id in exact:
                previous = exact[order.client_order_id]
                if previous is None or previous != order:
                    raise RecoveryBlockedError(
                        "exact order identity queries returned conflicting evidence"
                    )
            exact[order.client_order_id] = order
            known_exchange_ids.add(exchange_order_id)
        return exact

    def _persist_complete_snapshot(
        self,
        *,
        request: RecoveryRequest,
        epoch: int,
        checkpoint_id: int,
        rules: InstrumentRules,
        position_mode: Any,
        open_orders: tuple[ExchangeOrderSnapshot, ...],
        positions: tuple[Any, ...],
        margin: Any,
        pages: list[TradePage],
        report: ReconciliationReport,
        blockers: tuple[str, ...],
        replay_from_time: datetime,
        lease: _RecoveryLease,
        local_state_digest: str,
    ) -> Mapping[str, Any]:
        self._assert_epoch(epoch)
        observed_at = max(
            [rules.observed_at or self._now(), position_mode.observed_at, margin.observed_at]
            + [page.snapshot_time for page in pages]
        )
        observed_ms = self._milliseconds(observed_at)
        # A completed replay proves the interval through the final REST page's
        # snapshot boundary, even when that interval contains no fills.
        fills_through_ms = self._milliseconds(pages[-1].snapshot_time)
        replay_from_ms = self._milliseconds(replay_from_time)
        final_page = pages[-1]
        with self.ledger.unit_of_work() as uow:
            self._require_lease_in_uow(uow, lease)
            if self._local_recovery_state_hash(uow, request) != local_state_digest:
                raise RecoveryBlockedError(
                    "durable local recovery state changed during recovery"
                )
            self._validate_claimable_orders(uow, request)
            current_rule_blockers = self._instrument_rules_blockers_in_uow(
                uow,
                request,
                rules,
            )
            unrecorded_rule_blockers = [
                item for item in current_rule_blockers if item not in blockers
            ]
            if unrecorded_rule_blockers:
                raise RecoveryBlockedError(
                    "runnable grid intent changed during instrument-rule validation"
                )
            uow.instrument_rules.record_observation(
                self._rules_record(rules, observed_ms)
            )
            uow.position_mode_observations.add_idempotent(
                {
                    "account_id": request.account_id,
                    "mode": position_mode.mode.value,
                    "observed_at_ms": self._milliseconds(position_mode.observed_at),
                    "checkpoint_id": checkpoint_id,
                    "source": "REST",
                }
            )
            self._persist_observations(
                uow,
                request=request,
                checkpoint_id=checkpoint_id,
                observed_ms=observed_ms,
                rules=rules,
                position_mode=position_mode,
                open_orders=open_orders,
                positions=positions,
                margin=margin,
                pages=pages,
            )
            order_ids = self._persist_orders(uow, request, report, observed_ms)
            self._persist_exchange_trade_observations(
                uow,
                report.fills,
                checkpoint_id=checkpoint_id,
                observed_ms=observed_ms,
            )
            persistence_unmatched = self._persist_fills(
                uow,
                report.fills,
                order_ids,
                observed_ms,
            )
            reconciler_unmatched = {
                fill.deduplication_key for fill in report.unmatched_fills
            }
            unexpected_unmatched = [
                fill
                for fill in persistence_unmatched
                if fill.deduplication_key not in reconciler_unmatched
            ]
            if unexpected_unmatched:
                raise RecoveryBlockedError(
                    "reconciled fill could not be attached to one durable order"
                )
            cached_positions = tuple(
                item
                for item in uow.positions.list_for_account(request.account_id)
                if item["symbol"] == request.symbol
            )
            persisted_position_keys: set[tuple[str, str]] = set()
            for resolution in report.positions:
                persisted_position_keys.add(
                    (resolution.symbol, resolution.position_side.value)
                )
                position = resolution.exchange
                if position is None:
                    # Absence from a complete authoritative position snapshot
                    # means zero contracts for this previously persisted key.
                    uow.positions.upsert(
                        {
                            "account_id": request.account_id,
                            "symbol": resolution.symbol,
                            "position_side": resolution.position_side.value,
                            "quantity_contracts": 0,
                            "exchange_update_ms": observed_ms,
                            "observed_at_ms": observed_ms,
                            "checkpoint_id": checkpoint_id,
                            "source": "REST",
                        }
                    )
                    continue
                uow.positions.upsert(
                    {
                        "account_id": request.account_id,
                        "symbol": position.symbol,
                        "position_side": position.position_side.value,
                        "quantity_contracts": position.contracts,
                        "entry_price": position.entry_price,
                        "mark_price": position.mark_price,
                        "unrealized_pnl": position.unrealized_pnl,
                        "leverage": position.leverage,
                        "margin_type": "isolated" if position.isolated else "cross",
                        "margin_asset": position.margin_asset,
                        "isolated": int(position.isolated),
                        "liquidation_price": position.liquidation_price,
                        "exchange_update_ms": self._milliseconds(position.update_time),
                        "observed_at_ms": observed_ms,
                        "checkpoint_id": checkpoint_id,
                        "source": "REST",
                    }
                )
            # A complete REST position snapshot is authoritative for every
            # previously cached side.  Binance commonly omits zero positions,
            # so stale ONE_WAY/HEDGE rows must be explicitly zeroed rather than
            # surviving from an earlier account mode or process run.
            for cached in cached_positions:
                key = (str(cached["symbol"]), str(cached["position_side"]))
                if key in persisted_position_keys:
                    continue
                uow.positions.upsert(
                    {
                        "account_id": request.account_id,
                        "symbol": key[0],
                        "position_side": key[1],
                        "quantity_contracts": 0,
                        "exchange_update_ms": observed_ms,
                        "observed_at_ms": observed_ms,
                        "checkpoint_id": checkpoint_id,
                        "source": "REST",
                    }
                )
            checkpoint = uow.recovery_checkpoints.advance(
                checkpoint_id,
                status="SNAPSHOT_COMPLETE",
                rest_server_time_ms=fills_through_ms,
                open_orders_observed_at_ms=observed_ms,
                position_observed_at_ms=observed_ms,
                margin_observed_at_ms=self._milliseconds(margin.observed_at),
                snapshot_observed_at_ms=observed_ms,
                position_mode=position_mode.mode.value,
                rules_hash=self._semantic_rules_hash(rules),
            )
            checkpoint = uow.recovery_checkpoints.advance(
                checkpoint_id,
                status="REPLAY_COMPLETE",
                fills_from_ms=replay_from_ms,
                fills_through_ms=fills_through_ms,
                last_binance_trade_id=final_page.pagination_watermark,
                trades_complete=final_page.complete,
                next_trade_cursor=final_page.next_cursor,
                pagination_watermark=final_page.pagination_watermark,
            )
            checkpoint = uow.recovery_checkpoints.advance(
                checkpoint_id, status="RECONCILED"
            )
            checkpoint = uow.recovery_checkpoints.complete(
                checkpoint_id,
                status="BLOCKED" if blockers else "COMPLETE",
                completed_at_ms=self._milliseconds(self._now()),
                orders_seen=len(open_orders),
                fills_seen=len(report.fills),
                mismatch_count=len(blockers),
                error="; ".join(blockers) if blockers else None,
            )
            # Serialize the final fence and commit with begin_shutdown().  A
            # shutdown that wins this lock prevents the old epoch committing;
            # a commit that wins is complete before STOPPING begins.
            with self._lifecycle_guard:
                self._assert_epoch(epoch)
                self._renew_lease_in_uow(
                    uow,
                    lease,
                    now_ms=self._milliseconds(self._now()),
                )
                uow.commit()
        return checkpoint

    def _persist_orders(
        self,
        uow: Any,
        request: RecoveryRequest,
        report: ReconciliationReport,
        observed_ms: int,
    ) -> dict[tuple[Optional[str], str], Mapping[str, Any]]:
        order_ids: dict[tuple[Optional[str], str], Mapping[str, Any]] = {}
        for resolution in report.orders:
            local = resolution.local
            exchange = resolution.exchange
            if resolution.kind in {
                ReconciliationKind.AMBIGUOUS,
                ReconciliationKind.UNCLAIMED,
            }:
                # Conflicting identity evidence must never mutate or enrich a
                # durable local order.  The raw REST observation and blocker
                # are persisted instead.
                continue
            if resolution.kind is ReconciliationKind.LOCAL_ONLY:
                if local is not None:
                    order_ids[
                        (
                            local.get("exchange_order_id"),
                            str(local["client_order_id"]),
                        )
                    ] = local
                continue
            if local is not None:
                status = resolution.effective_status
                if (
                    exchange is not None
                    or status is ExchangeOrderStatus.FILLED
                    or resolution.cumulative_filled_contracts
                    != int(local["cumulative_filled_contracts"])
                ):
                    exchange_order_id = (
                        exchange.exchange_order_id if exchange is not None else None
                    )
                    update_ms = (
                        self._milliseconds(exchange.update_time)
                        if exchange is not None
                        else observed_ms
                    )
                    local = uow.orders.record_exchange_update(
                        str(local["local_order_id"]),
                        exchange_status=status.value,
                        cumulative_filled_contracts=resolution.cumulative_filled_contracts,
                        exchange_update_ms=update_ms,
                        updated_at_ms=observed_ms,
                        exchange_order_id=exchange_order_id,
                        avg_fill_price=(
                            None if exchange is None else exchange.average_fill_price
                        ),
                        side=(None if exchange is None else exchange.side.value),
                        position_side=(
                            None if exchange is None else exchange.position_side.value
                        ),
                        price=(None if exchange is None else exchange.price),
                        original_contracts=(
                            None if exchange is None else exchange.original_contracts
                        ),
                        reduce_only=(
                            None if exchange is None else exchange.reduce_only
                        ),
                        order_type=(
                            None if exchange is None else exchange.order_type
                        ),
                        time_in_force=(
                            None if exchange is None else exchange.time_in_force
                        ),
                        last_source="BINANCE_REST",
                    )
            if local is not None:
                order_ids[
                    (local.get("exchange_order_id"), str(local["client_order_id"]))
                ] = local
        return order_ids

    def _persist_fills(
        self,
        uow: Any,
        fills: tuple[ExchangeFill, ...],
        order_ids: Mapping[
            tuple[Optional[str], str],
            Mapping[str, Any],
        ],
        observed_ms: int,
    ) -> tuple[ExchangeFill, ...]:
        unmatched: list[ExchangeFill] = []
        for fill in fills:
            candidates: set[str] = set()
            identity_conflict = False
            for (
                known_exchange_id,
                known_client_id,
            ), candidate_order in order_ids.items():
                matches_client = (
                    fill.client_order_id is not None
                    and fill.client_order_id == known_client_id
                )
                matches_exchange = (
                    known_exchange_id is not None
                    and fill.exchange_order_id == known_exchange_id
                )
                if not matches_client and not matches_exchange:
                    continue
                # Matching either identifier is enough to find a candidate, but
                # every identifier present on both sides must agree before the
                # fill may be attached to a durable local order.
                if (
                    fill.client_order_id is not None
                    and fill.client_order_id != known_client_id
                ):
                    identity_conflict = True
                    continue
                if (
                    known_exchange_id is not None
                    and fill.exchange_order_id != known_exchange_id
                ):
                    identity_conflict = True
                    continue
                if (
                    fill.account_id != str(candidate_order["account_id"])
                    or fill.symbol != str(candidate_order["symbol"])
                    or fill.side.value != str(candidate_order["side"]).lower()
                    or fill.position_side.value
                    != str(candidate_order["position_side"]).lower()
                ):
                    identity_conflict = True
                    continue
                candidates.add(str(candidate_order["local_order_id"]))
            if identity_conflict or len(candidates) != 1:
                unmatched.append(fill)
                continue
            local_id = next(iter(candidates))
            uow.fills.add_idempotent(
                {
                    "fill_id": self._fill_id(fill),
                    "order_id": local_id,
                    "account_id": fill.account_id,
                    "symbol": fill.symbol,
                    "binance_trade_id": fill.trade_id,
                    "exchange_order_id": fill.exchange_order_id,
                    "client_order_id": fill.client_order_id,
                    "side": fill.side.value,
                    "price": fill.price,
                    "fill_contracts": fill.contracts,
                    "commission": fill.commission,
                    "commission_asset": fill.commission_asset,
                    "realized_pnl": fill.realized_pnl,
                    "trade_time_ms": self._milliseconds(fill.trade_time),
                    "source": "REST_BACKFILL",
                    "created_at_ms": observed_ms,
                }
            )
        return tuple(unmatched)

    def _persist_exchange_trade_observations(
        self,
        uow: Any,
        fills: tuple[ExchangeFill, ...],
        *,
        checkpoint_id: int,
        observed_ms: int,
    ) -> None:
        """Persist every REST trade fact, independent of strategy ownership."""

        for fill in fills:
            uow.exchange_trade_observations.record_observation(
                {
                    "account_id": fill.account_id,
                    "symbol": fill.symbol,
                    "binance_trade_id": fill.trade_id,
                    "exchange_order_id": fill.exchange_order_id,
                    "client_order_id": fill.client_order_id,
                    "side": fill.side.value,
                    "position_side": fill.position_side.value,
                    "price": fill.price,
                    "fill_contracts": fill.contracts,
                    "commission": fill.commission,
                    "commission_asset": fill.commission_asset,
                    "realized_pnl": fill.realized_pnl,
                    # ExchangeFill intentionally exposes only facts needed by
                    # Phase 2.  Maker/taker may be enriched by a later DTO.
                    "is_maker": None,
                    "trade_time_ms": self._milliseconds(fill.trade_time),
                    "first_checkpoint_id": checkpoint_id,
                    "last_checkpoint_id": checkpoint_id,
                    "first_observed_at_ms": observed_ms,
                    "last_observed_at_ms": observed_ms,
                }
            )

    def _persist_observations(
        self,
        uow: Any,
        *,
        request: RecoveryRequest,
        checkpoint_id: int,
        observed_ms: int,
        rules: Any,
        position_mode: Any,
        open_orders: tuple[Any, ...],
        positions: tuple[Any, ...],
        margin: Any,
        pages: list[TradePage],
    ) -> None:
        snapshot_server_ms = self._milliseconds(pages[-1].snapshot_time)
        observations = [
            (
                "INSTRUMENT_RULES",
                rules,
                1,
                True,
                None,
                self._semantic_rules_hash(rules),
            ),
            ("POSITION_MODE", position_mode, 1, True, None, None),
            ("OPEN_ORDERS", open_orders, len(open_orders), True, None, None),
            ("POSITIONS", positions, len(positions), True, None, None),
            ("MARGIN_ACCOUNT", margin, len(margin.balances), True, None, None),
        ]
        for kind, value, count, complete, cursor, watermark in observations:
            uow.exchange_observations.append_idempotent(
                {
                    "checkpoint_id": checkpoint_id,
                    "observation_type": kind,
                    "account_id": request.account_id,
                    "symbol": request.symbol,
                    "observed_at_ms": observed_ms,
                    "server_time_ms": snapshot_server_ms,
                    "item_count": count,
                    "complete": int(complete),
                    "next_cursor": cursor,
                    "pagination_watermark": watermark,
                    "payload_hash": self._hash(value),
                    "metadata_json": {"phase": 2, "read_only": True},
                }
            )
        for page in pages:
            uow.exchange_observations.append_idempotent(
                {
                    "checkpoint_id": checkpoint_id,
                    "observation_type": "USER_TRADES",
                    "account_id": request.account_id,
                    "symbol": request.symbol,
                    "observed_at_ms": self._milliseconds(page.snapshot_time),
                    "server_time_ms": self._milliseconds(page.snapshot_time),
                    "item_count": len(page.items),
                    "complete": int(page.complete),
                    "next_cursor": page.next_cursor,
                    "pagination_watermark": page.pagination_watermark,
                    "payload_hash": self._hash(page),
                    "metadata_json": {"phase": 2, "read_only": True},
                }
            )

    def _persist_interrupted_pages(
        self,
        request: RecoveryRequest,
        checkpoint_id: int,
        pages: list[TradePage],
        error: Exception,
        *,
        lease: _RecoveryLease,
    ) -> None:
        try:
            now_ms = self._milliseconds(self._now())
            with self.ledger.unit_of_work() as uow:
                self._renew_lease_in_uow(uow, lease, now_ms=now_ms)
                local = uow.orders.list_for_strategy(
                    request.strategy_id, symbol=request.symbol
                )
                order_ids = {
                    (
                        item.get("exchange_order_id"),
                        str(item["client_order_id"]),
                    ): item
                    for item in local
                }
                fills = tuple(fill for page in pages for fill in page.items)
                self._persist_exchange_trade_observations(
                    uow,
                    fills,
                    checkpoint_id=checkpoint_id,
                    observed_ms=now_ms,
                )
                unmatched = self._persist_fills(uow, fills, order_ids, now_ms)
                # Cursor progress is safe only when every fact on the partial
                # page is also attached to durable strategy ownership.  Raw
                # external facts are retained above, but an ambiguous BOT-like
                # fill must be replayed until its ownership is resolved.
                if pages and not unmatched:
                    page = pages[-1]
                    uow.recovery_checkpoints.record_pagination_progress(
                        checkpoint_id,
                        next_trade_cursor=page.next_cursor,
                        pagination_watermark=page.pagination_watermark,
                        fills_through_ms=max(
                            (self._milliseconds(fill.trade_time) for fill in fills),
                            default=now_ms,
                        ),
                        trades_complete=page.complete,
                    )
                checkpoint = uow.recovery_checkpoints.get(checkpoint_id)
                if checkpoint is not None and checkpoint["status"] not in {
                    "COMPLETE", "FAILED", "BLOCKED"
                }:
                    uow.recovery_checkpoints.complete(
                        checkpoint_id,
                        status="BLOCKED",
                        completed_at_ms=now_ms,
                        orders_seen=0,
                        fills_seen=len(fills),
                        mismatch_count=1,
                        error=self._safe_error(error),
                    )
                uow.commit()
        except Exception:
            # Preserve the original recovery failure.  The STARTED checkpoint
            # still proves that another full REST recovery is required.
            return

    @classmethod
    def _rules_record(
        cls,
        rules: InstrumentRules,
        observed_ms: int,
    ) -> dict[str, Any]:
        order_types = json.dumps(list(rules.supported_order_types), separators=(",", ":"))
        semantic_hash = cls._semantic_rules_hash(rules)
        return {
            "symbol": rules.symbol,
            "pair": rules.pair or rules.symbol.removesuffix("_PERP"),
            "contract_type": rules.contract_type,
            "status": rules.status,
            "contract_size": rules.contract_size,
            "margin_asset": rules.margin_asset,
            "tick_size": rules.price_tick,
            "quantity_step": rules.contract_step,
            "min_qty": rules.min_contracts,
            "max_qty": rules.max_contracts,
            "min_price": rules.min_price,
            "max_price": rules.max_price,
            "supported_order_types_json": order_types,
            "observed_at_ms": observed_ms,
            "payload_hash": rules.rules_hash or cls._hash(rules),
            "rules_hash": semantic_hash,
        }

    _ACTIVE_RUN_STATUSES = frozenset({"STARTING", "RUNNING", "RECOVERING"})

    def _acquire_recovery_lease(
        self,
        request: RecoveryRequest,
    ) -> _RecoveryLease:
        now_ms = self._milliseconds(self._now())
        try:
            with self.ledger.unit_of_work() as uow:
                strategy = uow.strategies.require(request.strategy_id)
                if (
                    strategy["account_id"] != request.account_id
                    or strategy["symbol"] != request.symbol
                ):
                    raise RecoveryBlockedError(
                        "recovery request does not match the durable strategy scope"
                    )
                run = uow.bot_runs.require(request.run_id)
                if run["status"] not in self._ACTIVE_RUN_STATUSES:
                    raise RecoveryBlockedError(
                        "recovery requires an active registered bot run"
                    )
                record = uow.strategy_leases.acquire(
                    strategy_id=request.strategy_id,
                    run_id=request.run_id,
                    now_ms=now_ms,
                    expires_at_ms=now_ms + self._lease_ttl_ms,
                )
                uow.bot_runs.heartbeat(request.run_id, now_ms)
                uow.commit()
        except LeaseUnavailable as exc:
            raise RecoveryInProgressError(
                "recovery is already running for this account/strategy"
            ) from exc
        except NotFound as exc:
            raise RecoveryBlockedError(
                "recovery requires a persisted strategy and registered bot run"
            ) from exc
        return _RecoveryLease(
            strategy_id=request.strategy_id,
            run_id=request.run_id,
            fencing_token=int(record["fencing_token"]),
        )

    def _require_lease_in_uow(self, uow: Any, lease: _RecoveryLease) -> None:
        now_ms = self._milliseconds(self._now())
        try:
            uow.strategy_leases.require_owned(
                strategy_id=lease.strategy_id,
                run_id=lease.run_id,
                fencing_token=lease.fencing_token,
                now_ms=now_ms,
            )
        except LeaseUnavailable as exc:
            raise StaleRecoveryLeaseError(
                "recovery lease expired or was fenced by another process"
            ) from exc

    def _renew_lease_in_uow(
        self,
        uow: Any,
        lease: _RecoveryLease,
        *,
        now_ms: int,
    ) -> None:
        try:
            run = uow.bot_runs.require(lease.run_id)
            if run["status"] not in self._ACTIVE_RUN_STATUSES:
                raise StaleRecoveryLeaseError(
                    "registered bot run is no longer active"
                )
            uow.strategy_leases.renew(
                strategy_id=lease.strategy_id,
                run_id=lease.run_id,
                fencing_token=lease.fencing_token,
                now_ms=now_ms,
                expires_at_ms=now_ms + self._lease_ttl_ms,
            )
            uow.bot_runs.heartbeat(lease.run_id, now_ms)
        except (LeaseUnavailable, NotFound) as exc:
            raise StaleRecoveryLeaseError(
                "recovery lease expired or was fenced by another process"
            ) from exc

    def _renew_recovery_lease(self, lease: _RecoveryLease) -> None:
        now_ms = self._milliseconds(self._now())
        with self.ledger.unit_of_work() as uow:
            self._renew_lease_in_uow(uow, lease, now_ms=now_ms)
            uow.commit()

    def _release_recovery_lease(self, lease: _RecoveryLease) -> None:
        try:
            with self.ledger.unit_of_work() as uow:
                uow.strategy_leases.release(
                    strategy_id=lease.strategy_id,
                    run_id=lease.run_id,
                    fencing_token=lease.fencing_token,
                )
                uow.commit()
        except LeaseUnavailable:
            # Expiry takeover is expected after a stalled/crashed recovery.  An
            # old owner must never release the new owner's lease.
            return

    def _fenced_exchange_read(
        self,
        request: RecoveryRequest,
        lease: _RecoveryLease,
        epoch: int,
        operation: Callable[[], Any],
    ) -> Any:
        del request  # Scope was validated atomically when the lease was acquired.
        self._assert_epoch(epoch)
        try:
            result = operation()
        except Exception:
            self._assert_epoch(epoch)
            self._renew_recovery_lease(lease)
            raise
        self._assert_epoch(epoch)
        self._renew_recovery_lease(lease)
        return result

    def _assert_epoch(self, epoch: int) -> None:
        if (
            self.readiness.state is not ReadinessState.RECOVERING
            or self.readiness.recovery_epoch != epoch
        ):
            raise StaleRecoveryEpochError("stale recovery epoch cannot commit")
        with self._lifecycle_guard:
            if self._stopping:
                raise StaleRecoveryEpochError("shutdown fenced the recovery epoch")

    def _record_check(self, check: RecoveryCheck, epoch: int) -> None:
        """Record recovery evidence atomically with the shutdown fence."""

        with self._lifecycle_guard:
            self._assert_epoch(epoch)
            self.readiness.record_check(check, recovery_epoch=epoch)

    @staticmethod
    def _position_mode_blockers(
        mode: PositionMode,
        *,
        report: ReconciliationReport,
    ) -> tuple[str, ...]:
        orders = {
            (item.exchange.symbol, item.exchange.exchange_order_id): item.exchange
            for item in report.orders
            if item.exchange is not None
        }.values()
        positions = (
            item.exchange for item in report.positions if item.exchange is not None
        )
        blockers: list[str] = []
        if mode is PositionMode.ONE_WAY:
            if any(order.position_side.value != "both" for order in orders):
                blockers.append("POSITION_MODE_ORDER_SIDE_INCONSISTENT")
            if any(position.position_side.value != "both" for position in positions):
                blockers.append("POSITION_MODE_POSITION_SIDE_INCONSISTENT")
        else:
            if any(order.position_side.value == "both" for order in orders):
                blockers.append("POSITION_MODE_ORDER_SIDE_INCONSISTENT")
            if any(
                position.position_side.value == "both" and position.contracts != 0
                for position in positions
            ):
                blockers.append("POSITION_MODE_POSITION_SIDE_INCONSISTENT")
        return tuple(blockers)

    @classmethod
    def _order_facts_hash(
        cls,
        orders: tuple[ExchangeOrderSnapshot, ...],
    ) -> str:
        return cls._hash(
            tuple(
                {
                    "symbol": item.symbol,
                    "client_order_id": item.client_order_id,
                    "exchange_order_id": item.exchange_order_id,
                    "status": item.status.value,
                    "side": item.side.value,
                    "position_side": item.position_side.value,
                    "original_contracts": item.original_contracts,
                    "filled_contracts": item.filled_contracts,
                    "price": item.price,
                    "average_fill_price": item.average_fill_price,
                    "reduce_only": item.reduce_only,
                    "order_type": item.order_type,
                    "time_in_force": item.time_in_force,
                }
                for item in sorted(
                    orders,
                    key=lambda order: (
                        order.symbol,
                        order.client_order_id,
                        order.exchange_order_id,
                    ),
                )
            )
        )

    @classmethod
    def _position_facts_hash(cls, positions: tuple[Any, ...]) -> str:
        return cls._hash(
            tuple(
                {
                    "symbol": item.symbol,
                    "position_side": item.position_side.value,
                    "contracts": item.contracts,
                    "entry_price": item.entry_price,
                    "leverage": item.leverage,
                    "margin_asset": item.margin_asset,
                    "isolated": item.isolated,
                }
                for item in sorted(
                    positions,
                    key=lambda position: (
                        position.symbol,
                        position.position_side.value,
                    ),
                )
            )
        )

    @classmethod
    def _margin_facts_hash(cls, snapshot: Any) -> str:
        return cls._hash(
            {
                "balances": tuple(
                    (
                        item.asset,
                        item.wallet_balance,
                        item.available_balance,
                    )
                    for item in sorted(
                        snapshot.balances,
                        key=lambda balance: balance.asset,
                    )
                ),
                "total_wallet_balance": snapshot.total_wallet_balance,
                "available_balance": snapshot.available_balance,
            }
        )

    @classmethod
    def _semantic_rules_hash(cls, rules: InstrumentRules) -> str:
        """Hash tradability semantics, excluding observation/provider metadata."""

        def decimal_key(value: Optional[Decimal]) -> Optional[str]:
            return None if value is None else str(value.normalize())

        return cls._hash(
            {
                "symbol": rules.symbol,
                "pair": rules.pair,
                "base_asset": rules.base_asset,
                "quote_asset": rules.quote_asset,
                "margin_asset": rules.margin_asset,
                "contract_type": rules.contract_type.upper(),
                "status": rules.status.upper(),
                "price_tick": decimal_key(rules.price_tick),
                "contract_size": decimal_key(rules.contract_size),
                "contract_step": rules.contract_step,
                "min_contracts": rules.min_contracts,
                "max_contracts": rules.max_contracts,
                "min_price": decimal_key(rules.min_price),
                "max_price": decimal_key(rules.max_price),
                "supported_order_types": tuple(
                    sorted(item.upper() for item in rules.supported_order_types)
                ),
            }
        )

    def _now(self) -> datetime:
        value = self._clock()
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("clock must return an aware datetime")
        return value.astimezone(timezone.utc)

    @staticmethod
    def _milliseconds(value: datetime) -> int:
        return int(value.timestamp() * 1000)

    @staticmethod
    def _overlap_cursor(cursor: str) -> str:
        try:
            return str(max(0, int(cursor) - 1))
        except ValueError:
            return cursor

    @staticmethod
    def _fill_id(fill: ExchangeFill) -> str:
        value = "\0".join(fill.deduplication_key).encode("utf-8")
        return "rest-" + hashlib.sha256(value).hexdigest()[:32]

    @staticmethod
    def _hash(value: Any) -> str:
        if is_dataclass(value):
            value = asdict(value)
        elif isinstance(value, tuple):
            value = [asdict(item) if is_dataclass(item) else item for item in value]
        encoded = json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        ).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()

    @staticmethod
    def _safe_error(error: BaseException) -> str:
        return f"{type(error).__name__}: {redact_message(error)}"[:1000]


__all__ = [
    "RecoveryBlockedError",
    "RecoveryInProgressError",
    "RecoveryManager",
    "RecoveryRequest",
    "RecoveryResult",
    "StaleRecoveryEpochError",
    "StaleRecoveryLeaseError",
]
