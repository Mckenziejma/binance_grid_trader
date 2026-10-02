# Source-of-truth policy

This document is normative for the COIN-M dynamic grid architecture. It
separates exchange facts from local strategy intent so recovery never guesses
which copy is authoritative.

## Authorities

| Fact | Final authority | Local handling |
|---|---|---|
| Whether an order exists at Binance, its exchange status, cumulative executed contracts and exchange order ID | Binance REST | Store the latest observed snapshot and its observation time. Never manufacture an exchange fact from desired state. |
| Individual fills, trade IDs, fill price, fill contracts, commission and realized PnL | Binance user trades REST | Persist each real fill idempotently. WebSocket fills are low-latency hints and must converge to the REST trade history. |
| Current position, entry price, leverage, liquidation price and unrealized PnL | Binance REST | Cache for decisions only after reconciliation. Never restore a position from a saved local counter. |
| Wallet, available margin and margin-asset balances | Binance REST | Treat local values as display caches, never as permission to trade. |
| Instrument status, tick size, integer contract step, minimum/maximum contracts and contract size | Binance REST | Persist the rules used by a generation for audit, but re-fetch current rules before entering `READY`. |
| Strategy intent and requested mode | SQLite | Binance cannot explain why an order exists or what strategy should do next. |
| Grid generation and its immutable parameters | SQLite | Bounds changes create a new generation; active history is not rewritten. |
| Logical levels/slots and their desired roles | SQLite | Logical level count is data-driven and has no fixed 169-level ceiling. |
| Ownership of a `clientOrderId` | SQLite | Only an exact persisted ownership record assigns an exchange order to a strategy/slot. Price, side and quantity similarity are not ownership proof. |
| Recovery checkpoint and REST replay high-water marks | SQLite | A checkpoint advances only after reconciled facts and derived runtime state commit atomically. |
| Runtime workflow state, including `ACK_UNKNOWN`, blocked slots and readiness | SQLite | Runtime state is durable; process memory is only a cache. |

In short: **Binance is authoritative for real orders, real fills, real
positions and margin. SQLite is authoritative for strategy intent, grid
generations, logical levels, `clientOrderId` ownership, recovery checkpoints
and durable runtime state.** Neither authority may overwrite facts owned by the
other.

## Non-authoritative inputs

- WebSocket events are an optimization, not a ledger. They may be delayed,
  duplicated, reordered or missed across a disconnect.
- In-memory dictionaries, counters and UI values are disposable caches.
- A successful local method return without an exchange identifier is not proof
  that an order exists.
- Absence from `get_open_orders` is not proof that an order never existed; it
  may already be terminal. Resolve by exact client order ID and user trades.
- A configured initial position is strategy intent, not proof of the current
  Binance position.

## Conflict-resolution rules

1. For exchange facts, import the Binance observation and append an audit
   record. Do not edit Binance facts to match local expectations.
2. For ownership, require the exact locally persisted `clientOrderId`. Unknown
   Binance orders remain external/unowned and are never silently adopted.
3. If a locally owned non-terminal order is absent from open orders, query it
   by client order ID and replay user trades before deciding its terminal state.
4. If a submit response is lost, persist `ACK_UNKNOWN`. The slot remains
   occupied. Reconcile the same client order ID; never issue a new ID merely
   because a timeout occurred.
5. If local and exchange fills disagree, the unique Binance trade records win.
   Repair local projections by idempotent fill replay.
6. If the Binance position differs from the strategy projection, enter or
   remain `RECOVERING`/`DEGRADED`. A compensating trade requires an explicit
   strategy/risk decision; reconciliation itself does not trade.
7. An unresolved contradiction blocks its logical slot. It must not be hidden
   by deleting history or recycling an identity.

## Durable identity and ownership

A client order ID is allocated and committed to SQLite **before** the network
submit begins. Its ownership tuple is:

```text
(strategy_id, generation_id, level_id, cycle_no, leg_role,
 client_order_id)
```

`leg_role` is a validated non-empty string so roles can evolve without a core
schema enum migration. The five-part logical slot key is `(strategy_id,
generation_id, level_id, cycle_no, leg_role)`; `client_order_id` is the owned
order identity attached to that slot. A logical slot may have at most one
unresolved order.
`SUBMITTING`, `ACK_UNKNOWN`, `ACTIVE`, `CANCEL_PENDING`, `PLANNED` and
`BLOCKED` all occupy the slot; only `TERMINAL` releases it according to the
order-domain policy. A later economic order uses a new `cycle_no`; a historical
semantic key and its client order ID are never reassigned.

Client IDs must be globally unique within the Binance account for the required
retention horizon. They are derived deterministically from the complete,
versioned semantic slot key; random UUIDs and timestamps are not identity
inputs. Parsing the string is not ownership proof; the SQLite ownership row is.

## Numeric and time rules

- Price, contract face value, balances, commission, PnL and all monetary
  amounts use `Decimal`; binary floats are rejected at domain boundaries.
- COIN-M quantities use integer contract counts. Base-asset amount is a
  separate `Decimal` observation and must never be sent as `quantity`.
- Timestamps crossing a domain boundary are timezone-aware UTC values.
- Exchange adapters perform wire-format conversion. Domain code never imports
  Binance SDK types.

## Grid intent changes

Changing upper/lower bounds, spacing, mode or grid count creates a new immutable
grid generation. The previous generation moves to `DRAINING`, its owned orders
are reconciled/canceled according to policy, and only then is it retired. This
preserves order ownership and prevents an order from silently changing logical
meaning while it is live.

## Boundary rule

The core and exchange-port packages are pure Python domain contracts. They do
not import Binance SDKs, SQLite drivers, Qt, or any legacy gateway/strategy
module. Concrete adapters and repositories translate at the outer boundary.
