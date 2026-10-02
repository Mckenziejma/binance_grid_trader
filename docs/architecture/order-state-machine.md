# Order state machine

This document defines the durable local order lifecycle and how Binance order
facts are projected into it. Local workflow state and exchange status are
stored separately; one must never be used as a lossy substitute for the other.

## Local states

| State | Meaning | Occupies logical slot |
|---|---|---:|
| `PLANNED` | Intent and client order ID are durably allocated, but no submit has started. | Yes |
| `SUBMITTING` | The submit call is in flight. | Yes |
| `ACK_UNKNOWN` | The call may have reached Binance, but a definitive ACK was not received. | Yes |
| `ACTIVE` | Binance confirms a non-terminal order (`NEW` or `PARTIALLY_FILLED`). | Yes |
| `CANCEL_PENDING` | A cancel was requested, but terminal exchange state is not yet proven. | Yes |
| `TERMINAL` | Binance proves `FILLED`, `CANCELED`, `REJECTED`, `EXPIRED` or `EXPIRED_IN_MATCH`. | No |
| `BLOCKED` | An invariant or ambiguity needs recovery/operator resolution. It is deliberately not terminal. | Yes |

At most one non-`TERMINAL` order may exist for a slot identified by
`(strategy_id, generation_id, level_id, cycle_no, leg_role)`.

## Exchange statuses

The normalized exchange statuses are `UNKNOWN`, `NEW`, `PARTIALLY_FILLED`,
`FILLED`, `CANCELED`, `REJECTED`, `EXPIRED` and `EXPIRED_IN_MATCH`.

Terminal exchange statuses are:

```text
FILLED, CANCELED, REJECTED, EXPIRED, EXPIRED_IN_MATCH
```

Once a terminal exchange status is accepted, that order record never returns
to a non-terminal state. A later contradictory payload is retained for audit
and triggers recovery; it does not mutate history backwards.

## Normal transitions

```text
PLANNED
  -> SUBMITTING
       -> ACTIVE             positive NEW/PARTIALLY_FILLED ACK
       -> TERMINAL           immediate terminal ACK
       -> ACK_UNKNOWN        timeout, disconnect, or undecodable response

ACK_UNKNOWN
  -> ACTIVE                  exact client ID found non-terminal by REST
  -> TERMINAL                exact client ID/fills prove terminal
  -> BLOCKED                 ambiguity cannot be resolved safely

ACTIVE
  -> ACTIVE                  more unique fills or fresher non-terminal snapshot
  -> CANCEL_PENDING          durable cancel intent before network cancel
  -> TERMINAL                terminal REST/WS exchange fact
  -> BLOCKED                 invariant violation

CANCEL_PENDING
  -> TERMINAL                cancel/fill/expiry wins the race
  -> ACTIVE                  REST proves cancel was not accepted and order is live
  -> BLOCKED                 outcome remains ambiguous after bounded recovery

BLOCKED
  -> ACTIVE or TERMINAL      recovery with authoritative evidence
```

`TERMINAL` has no outbound transition to a different state; an identical
terminal fact may be replayed idempotently.

## Submit protocol and unknown acknowledgements

The transaction/order sequence is:

1. Allocate a globally unique client order ID.
2. In one local transaction, persist ownership, `PLANNED`, requested price and
   integer contracts, and acquire the slot uniqueness constraint.
3. Persist `SUBMITTING` before the network call.
4. Submit using that exact client order ID.
5. On a definitive response, persist the exchange snapshot and transition to
   `ACTIVE` or `TERMINAL`.
6. On timeout, connection loss, cancellation of the local task, or malformed
   response after bytes may have been sent, transition to `ACK_UNKNOWN`.

`ACK_UNKNOWN` is not rejection. It forbids a replacement order. Recovery calls
`get_order_by_client_id`, `get_open_orders` and `get_user_trades`. If a retry is
needed, it is not performed by the generic state machine: the ambiguous order
is first resolved by authoritative evidence or remains `BLOCKED`. This avoids
assuming that a provider's client-ID retention and duplicate behavior make a
network replay safe.

## Partial fills

- Every Binance fill is stored individually using the unique `(account_id,
  symbol, trade_id)` key.
- A duplicate WebSocket or REST fill is a no-op.
- `PARTIALLY_FILLED` does not create a synthetic fill. It updates the exchange
  snapshot, then any missing contracts are recovered from `get_user_trades`.
- Filled contracts are the sum of unique persisted fills. Cumulative exchange
  executed contracts are a reconciliation checksum.
- A canceled or expired order may legitimately have fills. Those fills remain
  applied even though the final exchange status is terminal.
- Replenishment/opposite-leg logic reacts only to newly committed fill deltas,
  not repeatedly to cumulative order status.
- A gap where exchange cumulative contracts exceed persisted fill contracts
  prevents `READY` until real fills are fetched or the slot is blocked.

This treatment is required for correct commission, realized PnL, average fill
price and partial-fill follow-up behavior.

## Cancel/fill races

A cancel acknowledgement and a fill can cross in flight. The engine therefore
does not assume `CANCEL_PENDING` means no more fills. It continues ingesting
unique fills until Binance gives a terminal snapshot and the REST trade history
has caught up. `FILLED` takes precedence if cumulative filled contracts equal
the original contracts, even if an earlier cancel request timed out.

## Ordering and monotonicity

- Terminal state never regresses.
- Filled contracts never decrease.
- Older snapshots may enrich missing immutable identifiers but cannot lower
  status or cumulative fill progress.
- `UNKNOWN` cannot overwrite a known exchange status.
- Conflicting original quantity, symbol, side or ownership is an invariant
  violation and moves the slot to `BLOCKED`/recovery.
- Corrections are append-only observations or compensating workflow records;
  historical exchange facts are not edited away.

## Restart behavior

All non-terminal local states survive process exit. On restart the engine does
not call strategy start hooks or create grid orders immediately. It enters
`RECOVERING`, reconciles every occupied slot against Binance REST, imports
missing fills, validates slot uniqueness, commits a recovery checkpoint, and
only then enters `READY`.
