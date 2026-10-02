"""Phase-scoped runtime safety switches.

Phase 2 is deliberately observation-only.  Exchange adapters share this
guard so reaching application ``READY`` can never, by itself, enable a write.
"""

from .errors import TradingDisabledError


READ_ONLY_MODE = True
TRADING_ENABLED = False


def require_trading_enabled() -> None:
    """Fail closed for every exchange mutation during Phase 2."""

    # This is a phase boundary, not a feature flag.  Keeping the exported
    # constants documents the runtime mode, while making the guard
    # unconditional prevents a caller from monkey-patching module globals and
    # accidentally exposing a write path before Phase 3 supplies one.
    raise TradingDisabledError("Trading disabled in Phase 2")


__all__ = ["READ_ONLY_MODE", "TRADING_ENABLED", "require_trading_enabled"]
