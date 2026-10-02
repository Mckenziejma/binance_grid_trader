"""Phase-2 shutdown policy model and drain coordination."""

from dataclasses import dataclass
from enum import Enum


class ShutdownPolicy(str, Enum):
    LEAVE_ORDERS = "leave_orders"
    CANCEL_BOT_ORDERS = "cancel_bot_orders"


@dataclass(frozen=True)
class ShutdownPlan:
    policy: ShutdownPolicy
    execute_exchange_writes: bool = False

    def __post_init__(self) -> None:
        if self.execute_exchange_writes:
            raise ValueError("Phase 2 shutdown plans cannot execute exchange writes")


__all__ = ["ShutdownPlan", "ShutdownPolicy"]
