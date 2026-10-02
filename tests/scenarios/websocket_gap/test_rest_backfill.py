from __future__ import annotations

import gc
import tempfile
import unittest
import weakref
from decimal import Decimal
from pathlib import Path

from gridtrader.core.enums import ReadinessState

from tests.scenarios.support import (
    CLIENT_ORDER_ID,
    LOCAL_ORDER_ID,
    FixedClock,
    build_runtime,
    make_backend,
    make_exchange_order,
    make_request,
    register_run,
    rows,
    seed_local_position,
    seed_strategy_and_order,
)


class RestBackfillScenarioTests(unittest.TestCase):
    def test_ws_gap_is_backfilled_from_rest_after_runtime_rebuild(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            db_path = Path(temporary_directory) / "websocket-gap.sqlite3"
            clock = FixedClock()
            backend = make_backend(clock, default_page_size=1)
            backend.seed_order(make_exchange_order(contracts=4))
            backend.record_fill(
                "BTCUSD_PERP",
                CLIENT_ORDER_ID,
                contracts=2,
                price=Decimal("65000"),
                trade_id="rest-gap-1",
            )
            backend.record_fill(
                "BTCUSD_PERP",
                CLIENT_ORDER_ID,
                contracts=2,
                price=Decimal("65010"),
                trade_id="rest-gap-2",
            )

            disconnected = build_runtime(db_path, backend, clock)
            seed_strategy_and_order(disconnected.ledger, contracts=4)
            seed_local_position(disconnected.ledger, contracts=4)
            stale_refs = (
                weakref.ref(disconnected.ledger),
                weakref.ref(disconnected.adapter),
                weakref.ref(disconnected.manager),
            )
            del disconnected
            gc.collect()
            self.assertTrue(all(reference() is None for reference in stale_refs))

            reconnected = build_runtime(db_path, backend, clock)
            register_run(reconnected.ledger, "run-ws-reconnect")
            result = reconnected.manager.recover(
                make_request("run-ws-reconnect", reason="WS_RECONNECT")
            )

            self.assertTrue(result.complete)
            self.assertIs(result.state, ReadinessState.READY)
            with reconnected.ledger.unit_of_work(immediate=False) as uow:
                order = uow.orders.require(LOCAL_ORDER_ID)
                fills = rows(uow.fills)
            self.assertEqual(order["local_state"], "terminal")
            self.assertEqual(order["exchange_status"], "filled")
            self.assertEqual(order["cumulative_filled_contracts"], 4)
            self.assertEqual(
                {fill["binance_trade_id"] for fill in fills},
                {"rest-gap-1", "rest-gap-2"},
            )
            self.assertEqual({fill["source"] for fill in fills}, {"REST_BACKFILL"})
            # Two paginated backfill reads, the overlapping two-page tail,
            # and a final two-page replay after account-fact confirmation.
            self.assertEqual(backend.operation_call_count("get_user_trades"), 6)
