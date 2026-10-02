# Recovery and readiness invariants

Recovery is a mandatory state-reconstruction phase, not a best-effort startup
task. No strategy may submit a new order until local intent and Binance facts
have been reconciled and the readiness gate is `READY`.

## Lifecycle

```text
CLOSED -> RECOVERING -> READY
                         |
                         v
                      DEGRADED -> RECOVERING -> READY

any live state -> STOPPING -> STOPPED
```

There is no direct `CLOSED -> READY` or `DEGRADED -> READY` transition.
WebSocket reconnection alone never makes the process ready.

## Required startup sequence

1. Open the local repository and load strategy intent, active/draining grid
   generations, logical levels, client-order ownership, non-terminal orders,
   unique fills and the last recovery checkpoint.
2. Enter `RECOVERING` and block all new submit side effects.
3. For every in-scope symbol, obtain current `InstrumentRules` by REST.
4. Obtain Binance open orders with `get_open_orders`.
5. Obtain Binance positions with `get_positions` and margin balances from the
   account endpoint.
6. Replay Binance user trades from a safe overlap before the last checkpoint
   using `get_user_trades`; deduplicate by `(account_id, symbol, trade_id)`.
7. Resolve each locally owned `SUBMITTING`, `ACK_UNKNOWN`, `ACTIVE` and
   `CANCEL_PENDING` order. Use `get_order_by_client_id` whenever open orders do
   not provide a definitive answer.
8. Rebuild projections from committed real fills, compare them with cumulative
   order snapshots and compare strategy exposure with the real position.
9. Validate all invariants below. Ambiguous slots become `BLOCKED`; a material
   account-level ambiguity keeps the service `DEGRADED`.
10. Atomically persist imported observations, derived runtime state, and the
    new recovery checkpoint.
11. Mark all readiness checks complete and transition `RECOVERING -> READY`.

The first strategy evaluation occurs only after step 11.

## Readiness checks

The core gate requires explicit evidence for:

- local state loaded;
- instrument rules synchronized;
- open orders synchronized;
- positions synchronized;
- user trades synchronized;
- every `ACK_UNKNOWN` resolved or deliberately blocked;
- invariants validated; and
- recovery checkpoint committed.

Missing any one check forbids `READY`.

Every check result carries the epoch returned by `begin_recovery`. Evidence
from an older epoch is rejected, so a delayed REST task from a prior reconnect
cannot make the current recovery attempt ready.

## Order and slot invariants

1. A client order ID has exactly one local owner.
2. One five-part logical slot `(strategy_id, generation_id, level_id,
   cycle_no, leg_role)` has at most one order whose local state is not
   `TERMINAL`.
3. `ACK_UNKNOWN` occupies its slot and is never treated as safe absence.
4. Terminal exchange/local state never returns to a non-terminal state.
5. Filled contracts never decrease and never exceed original contracts.
6. Every applied fill has a unique real Binance trade ID; synthesized blank
   trade IDs are forbidden.
7. Order symbol, side, original contracts and ownership are immutable.
8. A Binance order is adopted as strategy-owned only by exact persisted
   client-order ownership, never by matching price/quantity.
9. An owned order absent from open orders is unresolved until exact-order and
   user-trade REST queries establish its outcome.
10. An unowned Binance order is reported and governed by explicit external
    order policy; recovery never silently cancels or adopts it.

## Position and fill invariants

1. The Binance REST position is the final current-position authority.
2. Strategy projected exposure is rebuilt from unique persisted fills and must
   be compared with the Binance position before `READY`.
3. A configured custom initial position is intent/baseline metadata. It cannot
   overwrite a real position. Any mismatch requires an explicit adoption,
   rebalance or stop decision.
4. Partial fills are applied fill by fill. A cumulative order status is a
   checksum, not a substitute for fill records.
5. Commission and realized PnL use exchange-provided Decimal values and assets.
6. COIN-M order/position quantity is integer contracts. Contract face value
   and base/quote amounts remain separate Decimal fields.

## Grid-generation invariants

1. Grid generation parameters are immutable after activation.
2. Editing bounds, spacing, mode or grid count creates a new generation.
3. At most one generation for a strategy is `ACTIVE`; an older generation may
   be `DRAINING` while its owned orders become terminal.
4. Logical levels have durable IDs independent of list indexes. The design may
   hold more than 169 levels and uses no fixed-size bitmask or exchange-order
   count as the logical-grid limit.
5. Retiring a generation does not delete its levels, orders, fills or ownership
   records.

## WebSocket disconnect and gap recovery

Phase 2 is deliberately read-only and does not open a private user stream.
Its `READY` therefore means only that one bounded, overlapping REST recovery
completed and committed; it is not a continuously fenced account view and it
never grants trading permission.  Phase 3 must add the private-stream
subscribe/buffer/drain barrier described below, plus a runtime fencing lease,
before any exchange write can be enabled.

The bounded Phase-2 recovery closes its trade-history replay with one final
overlapping `userTrades` pass, then re-reads position mode, instrument rules,
open orders, positions, and margin. Any difference from the account facts read
immediately before that closing replay blocks `READY`; reconciliation and the
checkpoint use only the post-closing account facts.

- Loss of the private user stream immediately moves a `READY` process to
  `DEGRADED` and prevents new order submissions. Risk-reducing actions may be
  allowed only by an explicit policy and must still be reconciled.
- Reconnect resubscribes streams, but the process then enters a fresh
  `RECOVERING` epoch and performs REST reconciliation from an overlapping
  checkpoint window.
- Sequence gaps, listen-key expiration, stale heartbeats, parse failures or an
  event timestamp regression trigger the same recovery path.
- WebSocket events received during recovery may be buffered/deduplicated, but
  they do not replace the REST snapshot.
- After the atomic checkpoint, buffered events newer than the snapshot are
  applied idempotently before `READY`.

## Crash consistency

- Persist intent/client-order ownership before sending network bytes.
- Persist cancel intent before sending cancel bytes.
- Import fills idempotently before updating projections that cause follow-up
  orders.
- Commit recovery observations, projections and checkpoint in one local
  transaction. A crash before commit repeats the overlap safely.
- Never advance a checkpoint past an unpersisted or unvalidated observation.
- Recovery and replay must be safe to run any number of times.

## Failure posture

Fail closed. If the exchange is unavailable, pagination is incomplete, an
`ACK_UNKNOWN` cannot be resolved, instrument rules changed incompatibly, or
position/fill invariants fail, remain `RECOVERING` or enter `DEGRADED`. Do not
guess, reset the strategy, recycle the slot, or place a compensating order
automatically.

## Stop semantics

`STOPPING` forbids new strategy orders. The stop policy explicitly chooses
whether to leave, cancel, or reduce existing exposure; it is not implied by the
readiness state. Once required persistence and exchange actions finish, the
process enters `STOPPED`. A new process instance must recover again before it
can become `READY`.
