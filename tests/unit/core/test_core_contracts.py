import unittest
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from gridtrader.core.clock import SystemClock, require_utc
from gridtrader.core.enums import (
    ExchangeOrderStatus,
    GridGenerationStatus,
    GridLevelState,
    OrderLocalState,
    ReadinessState,
    SpacingMode,
    StrategyMode,
    TERMINAL_EXCHANGE_ORDER_STATUSES,
    UNRESOLVED_LOCAL_ORDER_STATES,
)
from gridtrader.core.errors import (
    DomainValidationError,
    InvalidStateTransition,
    NotReadyError,
)
from gridtrader.core.readiness import (
    DEFAULT_RECOVERY_CHECKS,
    ReadinessGate,
    RecoveryCheck,
)
from gridtrader.core.types import (
    GridBounds,
    require_contracts,
    require_finite_decimal,
)


class EnumContractTests(unittest.TestCase):
    def test_required_enum_members_are_stable(self) -> None:
        self.assertEqual(
            {member.name for member in ReadinessState},
            {"CLOSED", "RECOVERING", "READY", "DEGRADED", "STOPPING", "STOPPED"},
        )
        self.assertEqual(
            {member.name for member in OrderLocalState},
            {
                "PLANNED",
                "SUBMITTING",
                "ACK_UNKNOWN",
                "ACTIVE",
                "CANCEL_PENDING",
                "TERMINAL",
                "BLOCKED",
            },
        )
        self.assertEqual(
            {member.name for member in ExchangeOrderStatus},
            {
                "UNKNOWN",
                "NEW",
                "PARTIALLY_FILLED",
                "FILLED",
                "CANCELED",
                "REJECTED",
                "EXPIRED",
                "EXPIRED_IN_MATCH",
            },
        )
        self.assertEqual(
            {member.name for member in StrategyMode},
            {"NEUTRAL", "LONG", "SHORT"},
        )
        self.assertEqual(
            {member.name for member in SpacingMode},
            {"ARITHMETIC", "GEOMETRIC"},
        )
        self.assertEqual(
            {member.name for member in GridGenerationStatus},
            {"DRAFT", "PREPARING", "ACTIVE", "DRAINING", "RETIRED", "FAILED"},
        )
        self.assertEqual(
            {member.name for member in GridLevelState},
            {"DORMANT", "ARMED", "ACTIVE", "BLOCKED", "RETIRED"},
        )

    def test_terminal_sets_do_not_treat_unknown_or_blocked_as_terminal(self) -> None:
        self.assertIn(ExchangeOrderStatus.FILLED, TERMINAL_EXCHANGE_ORDER_STATUSES)
        self.assertNotIn(
            ExchangeOrderStatus.UNKNOWN, TERMINAL_EXCHANGE_ORDER_STATUSES
        )
        self.assertIn(OrderLocalState.ACK_UNKNOWN, UNRESOLVED_LOCAL_ORDER_STATES)
        self.assertIn(OrderLocalState.BLOCKED, UNRESOLVED_LOCAL_ORDER_STATES)
        self.assertNotIn(OrderLocalState.TERMINAL, UNRESOLVED_LOCAL_ORDER_STATES)


class ValueTypeTests(unittest.TestCase):
    def test_grid_bounds_require_decimal_and_ordering(self) -> None:
        bounds = GridBounds(Decimal("100"), Decimal("200"))
        self.assertEqual(bounds.lower_price, Decimal("100"))

        with self.assertRaises(DomainValidationError):
            GridBounds(100, Decimal("200"))  # type: ignore[arg-type]
        with self.assertRaises(DomainValidationError):
            GridBounds(Decimal("200"), Decimal("200"))

    def test_numeric_helpers_reject_float_bool_nan_and_fractional_contracts(self) -> None:
        with self.assertRaises(DomainValidationError):
            require_finite_decimal(1.25, "price")  # type: ignore[arg-type]
        with self.assertRaises(DomainValidationError):
            require_finite_decimal(Decimal("NaN"), "price")
        with self.assertRaises(DomainValidationError):
            require_contracts(True, "contracts")
        with self.assertRaises(DomainValidationError):
            require_contracts(1.5, "contracts")  # type: ignore[arg-type]

    def test_clock_values_are_timezone_aware_utc(self) -> None:
        now = SystemClock().now()
        self.assertEqual(now.utcoffset(), timedelta(0))

        source = datetime(2026, 1, 1, 8, tzinfo=timezone(timedelta(hours=8)))
        normalized = require_utc(source)
        self.assertEqual(normalized, datetime(2026, 1, 1, tzinfo=timezone.utc))

        with self.assertRaises(DomainValidationError):
            require_utc(datetime(2026, 1, 1))


class ReadinessGateTests(unittest.TestCase):
    def test_startup_requires_all_recovery_evidence_before_ready(self) -> None:
        gate = ReadinessGate()
        self.assertEqual(gate.state, ReadinessState.CLOSED)
        self.assertFalse(gate.can_submit_orders)

        with self.assertRaises(InvalidStateTransition):
            gate.mark_ready(recovery_epoch=0)
        with self.assertRaises(NotReadyError):
            gate.require_ready()

        epoch = gate.begin_recovery("startup")
        self.assertEqual(epoch, 1)
        self.assertEqual(gate.state, ReadinessState.RECOVERING)

        for check in DEFAULT_RECOVERY_CHECKS:
            gate.record_check(check, recovery_epoch=epoch)

        self.assertEqual(gate.missing_checks, frozenset())
        gate.mark_ready(recovery_epoch=epoch)
        gate.require_ready()
        self.assertTrue(gate.can_submit_orders)

    def test_missing_checkpoint_prevents_ready(self) -> None:
        gate = ReadinessGate()
        epoch = gate.begin_recovery()
        for check in DEFAULT_RECOVERY_CHECKS - {
            RecoveryCheck.CHECKPOINT_COMMITTED
        }:
            gate.record_check(check, recovery_epoch=epoch)

        with self.assertRaises(InvalidStateTransition):
            gate.mark_ready(recovery_epoch=epoch)
        self.assertEqual(
            gate.missing_checks, frozenset({RecoveryCheck.CHECKPOINT_COMMITTED})
        )

    def test_degraded_must_run_a_fresh_recovery_epoch(self) -> None:
        gate = ReadinessGate()
        epoch = gate.begin_recovery()
        for check in DEFAULT_RECOVERY_CHECKS:
            gate.record_check(check, recovery_epoch=epoch)
        gate.mark_ready(recovery_epoch=epoch)

        gate.degrade("private websocket disconnected")
        self.assertEqual(gate.state, ReadinessState.DEGRADED)
        self.assertFalse(gate.can_submit_orders)
        with self.assertRaises(InvalidStateTransition):
            gate.mark_ready(recovery_epoch=epoch)

        self.assertEqual(gate.begin_recovery("REST gap recovery"), 2)
        self.assertEqual(gate.completed_checks, frozenset())

    def test_stale_recovery_evidence_cannot_complete_a_new_epoch(self) -> None:
        gate = ReadinessGate()
        first_epoch = gate.begin_recovery()
        gate.degrade("restart recovery")
        second_epoch = gate.begin_recovery()

        with self.assertRaises(InvalidStateTransition):
            gate.record_check(
                RecoveryCheck.OPEN_ORDERS_SYNCED,
                recovery_epoch=first_epoch,
            )
        for check in DEFAULT_RECOVERY_CHECKS:
            gate.record_check(check, recovery_epoch=second_epoch)
        with self.assertRaises(InvalidStateTransition):
            gate.mark_ready(recovery_epoch=first_epoch)
        gate.mark_ready(recovery_epoch=second_epoch)

    def test_mandatory_recovery_checks_cannot_be_removed(self) -> None:
        with self.assertRaises(ValueError):
            ReadinessGate({RecoveryCheck.CHECKPOINT_COMMITTED})

    def test_stopped_is_terminal_for_gate_instance(self) -> None:
        gate = ReadinessGate()
        gate.begin_stopping("shutdown")
        gate.mark_stopped()
        self.assertEqual(gate.state, ReadinessState.STOPPED)
        with self.assertRaises(InvalidStateTransition):
            gate.begin_recovery()


if __name__ == "__main__":
    unittest.main()
