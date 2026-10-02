"""Readiness gate that prevents trading before deterministic recovery."""

from enum import Enum
from threading import RLock
from typing import FrozenSet, Iterable, Optional, Set

from .enums import ReadinessState
from .errors import InvalidStateTransition, NotReadyError


class RecoveryCheck(str, Enum):
    """Evidence required before RECOVERING may advance to READY."""

    LOCAL_STATE_LOADED = "local_state_loaded"
    INSTRUMENT_RULES_SYNCED = "instrument_rules_synced"
    OPEN_ORDERS_SYNCED = "open_orders_synced"
    POSITIONS_SYNCED = "positions_synced"
    USER_TRADES_SYNCED = "user_trades_synced"
    ACK_UNKNOWN_RESOLVED = "ack_unknown_resolved"
    INVARIANTS_VALIDATED = "invariants_validated"
    CHECKPOINT_COMMITTED = "checkpoint_committed"


DEFAULT_RECOVERY_CHECKS: FrozenSet[RecoveryCheck] = frozenset(RecoveryCheck)


class ReadinessGate:
    """Thread-safe lifecycle gate for side-effecting trading commands.

    WebSocket connectivity alone can never make this gate READY.  A caller
    must begin a recovery epoch, record every required REST reconciliation
    check, commit the recovery checkpoint, and then explicitly mark READY.
    """

    def __init__(
        self,
        required_checks: Iterable[RecoveryCheck] = DEFAULT_RECOVERY_CHECKS,
    ) -> None:
        checks = frozenset(required_checks)
        if not all(isinstance(check, RecoveryCheck) for check in checks):
            raise TypeError("required_checks must contain only RecoveryCheck values")
        missing_mandatory = DEFAULT_RECOVERY_CHECKS.difference(checks)
        if missing_mandatory:
            names = ", ".join(sorted(check.value for check in missing_mandatory))
            raise ValueError(f"required_checks cannot omit mandatory checks: {names}")
        self._required_checks: FrozenSet[RecoveryCheck] = checks
        self._completed_checks: Set[RecoveryCheck] = set()
        self._state = ReadinessState.CLOSED
        self._reason: Optional[str] = None
        self._recovery_epoch = 0
        self._lock = RLock()

    @property
    def state(self) -> ReadinessState:
        with self._lock:
            return self._state

    @property
    def reason(self) -> Optional[str]:
        with self._lock:
            return self._reason

    @property
    def recovery_epoch(self) -> int:
        with self._lock:
            return self._recovery_epoch

    @property
    def completed_checks(self) -> FrozenSet[RecoveryCheck]:
        with self._lock:
            return frozenset(self._completed_checks)

    @property
    def missing_checks(self) -> FrozenSet[RecoveryCheck]:
        with self._lock:
            return self._required_checks.difference(self._completed_checks)

    @property
    def can_submit_orders(self) -> bool:
        return self.state is ReadinessState.READY

    def begin_recovery(self, reason: str = "") -> int:
        """Start a fresh recovery epoch and invalidate prior evidence."""

        with self._lock:
            if self._state not in {
                ReadinessState.CLOSED,
                ReadinessState.READY,
                ReadinessState.DEGRADED,
            }:
                raise InvalidStateTransition(
                    f"cannot enter recovering from {self._state.value}"
                )
            self._state = ReadinessState.RECOVERING
            self._completed_checks.clear()
            self._reason = reason.strip() or None
            self._recovery_epoch += 1
            return self._recovery_epoch

    def record_check(self, check: RecoveryCheck, *, recovery_epoch: int) -> None:
        with self._lock:
            if self._state is not ReadinessState.RECOVERING:
                raise InvalidStateTransition(
                    "recovery checks may only be recorded while recovering"
                )
            if isinstance(recovery_epoch, bool) or not isinstance(recovery_epoch, int):
                raise TypeError("recovery_epoch must be an integer")
            if recovery_epoch != self._recovery_epoch:
                raise InvalidStateTransition(
                    "recovery evidence belongs to a stale recovery epoch"
                )
            if not isinstance(check, RecoveryCheck):
                raise TypeError("check must be RecoveryCheck")
            if check not in self._required_checks:
                raise ValueError(f"unexpected recovery check: {check}")
            self._completed_checks.add(check)

    def mark_ready(self, *, recovery_epoch: int) -> None:
        with self._lock:
            if self._state is not ReadinessState.RECOVERING:
                raise InvalidStateTransition(
                    f"cannot enter ready from {self._state.value}"
                )
            if isinstance(recovery_epoch, bool) or not isinstance(recovery_epoch, int):
                raise TypeError("recovery_epoch must be an integer")
            if recovery_epoch != self._recovery_epoch:
                raise InvalidStateTransition(
                    "cannot complete a stale recovery epoch"
                )
            missing = self._required_checks.difference(self._completed_checks)
            if missing:
                names = ", ".join(sorted(check.value for check in missing))
                raise InvalidStateTransition(
                    f"recovery is incomplete; missing checks: {names}"
                )
            self._state = ReadinessState.READY
            self._reason = None

    def degrade(self, reason: str) -> None:
        with self._lock:
            if self._state not in {
                ReadinessState.READY,
                ReadinessState.RECOVERING,
            }:
                raise InvalidStateTransition(
                    f"cannot enter degraded from {self._state.value}"
                )
            reason = reason.strip()
            if not reason:
                raise ValueError("degraded state requires a reason")
            self._state = ReadinessState.DEGRADED
            self._reason = reason

    def begin_stopping(self, reason: str = "") -> None:
        with self._lock:
            if self._state in {ReadinessState.STOPPING, ReadinessState.STOPPED}:
                raise InvalidStateTransition(
                    f"cannot enter stopping from {self._state.value}"
                )
            self._state = ReadinessState.STOPPING
            self._reason = reason.strip() or None

    def mark_stopped(self) -> None:
        with self._lock:
            if self._state is not ReadinessState.STOPPING:
                raise InvalidStateTransition(
                    f"cannot enter stopped from {self._state.value}"
                )
            self._state = ReadinessState.STOPPED

    def require_ready(self) -> None:
        state = self.state
        if state is not ReadinessState.READY:
            raise NotReadyError(f"trading side effect requires READY, got {state.value}")


__all__ = [
    "DEFAULT_RECOVERY_CHECKS",
    "ReadinessGate",
    "RecoveryCheck",
]
