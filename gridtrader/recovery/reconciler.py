"""Pure observation/compare/merge/classify logic; never performs I/O."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping, Optional, Sequence

from gridtrader.core.enums import ExchangeOrderStatus, OrderLocalState, PositionSide
from gridtrader.exchange.models import ExchangeFill, ExchangeOrderSnapshot, ExchangePosition

from .models import (
    OrderOwnership,
    OrderResolution,
    PositionResolution,
    ReconciliationKind,
    ReconciliationReport,
)


def _local_value(record: Mapping[str, Any], name: str, default: Any = None) -> Any:
    return record[name] if name in record else default


@dataclass(frozen=True)
class _FillEvidence:
    key: tuple[str, str, str]
    account_id: str
    symbol: str
    client_order_id: Optional[str]
    exchange_order_id: Optional[str]
    side: Optional[str]
    position_side: Optional[str]
    contracts: int


class Reconciler:
    """Derive a deterministic reconciliation report from immutable inputs."""

    def reconcile(
        self,
        *,
        local_orders: Sequence[Mapping[str, Any]],
        open_orders: Sequence[ExchangeOrderSnapshot],
        exact_orders: Mapping[str, Optional[ExchangeOrderSnapshot]],
        fills: Sequence[ExchangeFill],
        local_positions: Sequence[Mapping[str, Any]],
        exchange_positions: Sequence[ExchangePosition],
        claimable_orders: Mapping[str, Mapping[str, Any]] | None = None,
        persisted_fills: Sequence[Mapping[str, Any]] = (),
        project_bot_positions: bool = False,
    ) -> ReconciliationReport:
        # In-memory claims are not ownership evidence.  Phase 1 requires the
        # clientOrderId/slot tuple to be committed before network submission;
        # therefore an exchange order without that durable row stays
        # UNCLAIMED even if a caller supplies a matching transient mapping.
        claims: Mapping[str, Mapping[str, Any]] = {}
        unique_fills = self._deduplicate_fills(fills)
        fill_evidence = self._merge_fill_evidence(persisted_fills, unique_fills)
        local_by_client = {
            str(record["client_order_id"]): record for record in local_orders
        }
        exchange_by_client: dict[str, ExchangeOrderSnapshot] = {}
        open_client_ids: set[str] = set()
        for item in open_orders:
            previous = exchange_by_client.get(item.client_order_id)
            if previous is not None and previous != item:
                raise ValueError(
                    "duplicate clientOrderId carries conflicting open-order data"
                )
            exchange_by_client[item.client_order_id] = item
            open_client_ids.add(item.client_order_id)
        for key, value in exact_orders.items():
            if value is None:
                continue
            if key != value.client_order_id:
                raise ValueError("exact-order map key does not match clientOrderId")
            previous = exchange_by_client.get(key)
            if previous is not None and previous != value:
                raise ValueError(
                    "open and exact order observations conflict for one clientOrderId"
                )
            exchange_by_client[key] = value

        resolutions: list[OrderResolution] = []
        all_ids = set(local_by_client).union(exchange_by_client)
        for client_id in sorted(all_ids):
            local = local_by_client.get(client_id)
            exchange = exchange_by_client.get(client_id)
            exchange_order_ids = {
                value
                for value in (
                    None if local is None else local.get("exchange_order_id"),
                    None if exchange is None else exchange.exchange_order_id,
                )
                if value is not None
            }
            linked_fill_contracts, fill_identity_conflicts = (
                self._local_fill_evidence(
                    local if local is not None else claims.get(client_id),
                    client_id=client_id,
                    exchange_order_ids=exchange_order_ids,
                    fill_evidence=fill_evidence,
                )
            )
            if local is None:
                resolutions.append(
                    self._exchange_only(
                        exchange,
                        claims.get(client_id),
                        exchange_was_open=client_id in open_client_ids,
                        linked_fill_contracts=linked_fill_contracts,
                        fill_identity_conflicts=fill_identity_conflicts,
                    )
                )
            else:
                resolutions.append(
                    self._local_resolution(
                        local,
                        exchange,
                        exchange_was_open=client_id in open_client_ids,
                        exact_was_queried=client_id in exact_orders,
                        linked_fill_contracts=linked_fill_contracts,
                        fill_identity_conflicts=fill_identity_conflicts,
                    )
                )

        linked_keys: set[tuple[str, str, str]] = set()
        for resolution in resolutions:
            if (
                resolution.kind is ReconciliationKind.AMBIGUOUS
                or (resolution.local is None and resolution.claim is None)
            ):
                continue
            exchange_order_ids = {
                value
                for value in (
                    None
                    if resolution.local is None
                    else resolution.local.get("exchange_order_id"),
                    None
                    if resolution.exchange is None
                    else resolution.exchange.exchange_order_id,
                )
                if value is not None
            }
            linked_keys.update(
                fill.deduplication_key
                for fill in unique_fills
                if self._observed_fill_matches_resolution(
                    fill,
                    resolution,
                    exchange_order_ids,
                )
            )
        unmatched = tuple(
            fill for fill in unique_fills if fill.deduplication_key not in linked_keys
        )
        resolved_exchange_order_ids = {
            resolution.exchange.exchange_order_id
            for resolution in resolutions
            if resolution.exchange is not None
        }
        projected_positions = local_positions
        projection_blockers: tuple[str, ...] = ()
        if project_bot_positions:
            projected_positions, projection_blockers = self._project_bot_positions(
                local_positions,
                resolutions,
                fill_evidence,
            )
        positions = self.reconcile_positions(projected_positions, exchange_positions)
        blockers = tuple(
            [
                f"{item.kind.value}:{item.client_order_id}:{item.reason}"
                for item in resolutions
                if item.blocks_ready
            ]
            + [
                f"POSITION_MISMATCH:{item.symbol}:{item.position_side.value}"
                for item in positions
                if item.blocks_ready
            ]
            + [
                f"UNCLAIMED:{fill.client_order_id}:bot-shaped fill could not be mapped safely"
                for fill in unmatched
                if fill.client_order_id is not None
                and fill.client_order_id.startswith("dg1_")
            ]
            + [
                (
                    "AMBIGUOUS_FILL:{}:clientOrderId is absent and order "
                    "ownership could not be established"
                ).format(fill.exchange_order_id)
                for fill in unmatched
                if fill.client_order_id is None
                and fill.exchange_order_id not in resolved_exchange_order_ids
            ]
            + list(projection_blockers)
        )
        return ReconciliationReport(
            orders=tuple(resolutions),
            fills=unique_fills,
            positions=positions,
            unmatched_fills=unmatched,
            blockers=blockers,
        )

    @staticmethod
    def _deduplicate_fills(fills: Sequence[ExchangeFill]) -> tuple[ExchangeFill, ...]:
        seen: dict[tuple[str, str, str], ExchangeFill] = {}
        for fill in fills:
            previous = seen.get(fill.deduplication_key)
            if previous is not None and previous != fill:
                raise ValueError(
                    "duplicate Binance trade ID carries conflicting fill data"
                )
            seen[fill.deduplication_key] = fill
        return tuple(
            sorted(seen.values(), key=lambda item: (item.trade_time, item.trade_id))
        )

    @staticmethod
    def _merge_fill_evidence(
        persisted_fills: Sequence[Mapping[str, Any]],
        observed_fills: Sequence[ExchangeFill],
    ) -> tuple[_FillEvidence, ...]:
        evidence: dict[tuple[str, str, str], _FillEvidence] = {}
        for record in persisted_fills:
            key = (
                str(record["account_id"]),
                str(record["symbol"]),
                str(record["binance_trade_id"]),
            )
            contracts = record["fill_contracts"]
            if (
                isinstance(contracts, bool)
                or not isinstance(contracts, int)
                or contracts <= 0
            ):
                raise ValueError("persisted fill contracts must be a positive integer")
            candidate = _FillEvidence(
                key=key,
                account_id=key[0],
                symbol=key[1],
                client_order_id=(
                    None
                    if record.get("client_order_id") is None
                    and record.get("owned_client_order_id") is None
                    else str(
                        record.get("client_order_id")
                        or record["owned_client_order_id"]
                    )
                ),
                exchange_order_id=(
                    None
                    if record.get("exchange_order_id") is None
                    and record.get("owned_exchange_order_id") is None
                    else str(
                        record.get("exchange_order_id")
                        or record["owned_exchange_order_id"]
                    )
                ),
                side=(
                    None
                    if record.get("side") is None
                    else str(record["side"]).lower()
                ),
                position_side=(
                    None
                    if record.get("position_side") is None
                    else str(record["position_side"]).lower()
                ),
                contracts=contracts,
            )
            previous = evidence.get(key)
            if previous is not None and previous != candidate:
                raise ValueError(
                    "persisted Binance trade ID carries conflicting fill data"
                )
            evidence[key] = candidate

        for fill in observed_fills:
            candidate = _FillEvidence(
                key=fill.deduplication_key,
                account_id=fill.account_id,
                symbol=fill.symbol,
                client_order_id=fill.client_order_id,
                exchange_order_id=fill.exchange_order_id,
                side=fill.side.value,
                position_side=fill.position_side.value,
                contracts=fill.contracts,
            )
            previous = evidence.get(candidate.key)
            if previous is not None:
                if previous.contracts != candidate.contracts:
                    raise ValueError(
                        "persisted and observed Binance trade quantities conflict"
                    )
                if (
                    previous.client_order_id is not None
                    and candidate.client_order_id is not None
                    and previous.client_order_id != candidate.client_order_id
                ):
                    raise ValueError(
                        "persisted and observed Binance trade client IDs conflict"
                    )
                if (
                    previous.exchange_order_id is not None
                    and previous.exchange_order_id != candidate.exchange_order_id
                ):
                    raise ValueError(
                        "persisted and observed Binance trade order IDs conflict"
                    )
                if previous.side is not None and previous.side != candidate.side:
                    raise ValueError(
                        "persisted and observed Binance trade sides conflict"
                    )
                if (
                    previous.position_side is not None
                    and previous.position_side != candidate.position_side
                ):
                    raise ValueError(
                        "persisted and observed Binance trade position sides conflict"
                    )
                candidate = _FillEvidence(
                    key=candidate.key,
                    account_id=candidate.account_id,
                    symbol=candidate.symbol,
                    client_order_id=(
                        candidate.client_order_id or previous.client_order_id
                    ),
                    exchange_order_id=(
                        candidate.exchange_order_id or previous.exchange_order_id
                    ),
                    side=candidate.side or previous.side,
                    position_side=(
                        candidate.position_side or previous.position_side
                    ),
                    contracts=candidate.contracts,
                )
            evidence[candidate.key] = candidate
        return tuple(evidence.values())

    @staticmethod
    def _exchange_only(
        exchange: ExchangeOrderSnapshot | None,
        claim: Mapping[str, Any] | None,
        *,
        exchange_was_open: bool,
        linked_fill_contracts: int,
        fill_identity_conflicts: tuple[str, ...],
    ) -> OrderResolution:
        if exchange is None:
            raise AssertionError("exchange-only resolution requires exchange evidence")
        mismatches = () if claim is None else Reconciler._semantic_mismatches(
            claim,
            exchange,
        )
        if not exchange_was_open and not exchange.is_terminal:
            kind = ReconciliationKind.AMBIGUOUS
            ownership = (
                OrderOwnership.BOT
                if claim is not None
                else OrderOwnership.EXTERNAL
            )
            reason = (
                "exact lookup returned an active order absent from the "
                "complete open-order snapshot"
            )
        elif claim is not None and (mismatches or fill_identity_conflicts):
            kind = ReconciliationKind.UNCLAIMED
            ownership = OrderOwnership.UNCLAIMED
            conflicts = list(mismatches)
            conflicts.extend(fill_identity_conflicts)
            reason = "claim conflicts with exchange evidence: " + ", ".join(
                conflicts
            )
            claim = None
        elif claim is not None and linked_fill_contracts != exchange.filled_contracts:
            kind = ReconciliationKind.AMBIGUOUS
            ownership = OrderOwnership.BOT
            reason = (
                "complete trade history does not match the exchange cumulative fill: "
                f"trades={linked_fill_contracts} exchange={exchange.filled_contracts}"
            )
        elif claim is not None:
            kind = ReconciliationKind.EXCHANGE_ONLY
            ownership = OrderOwnership.BOT
            reason = "deterministic clientOrderId matched a known logical slot"
        elif exchange.client_order_id.startswith("dg1_"):
            kind = ReconciliationKind.UNCLAIMED
            ownership = OrderOwnership.UNCLAIMED
            reason = "bot-shaped clientOrderId could not be mapped safely"
        else:
            kind = ReconciliationKind.EXTERNAL
            ownership = OrderOwnership.EXTERNAL
            reason = "order is not in the bot clientOrderId namespace"
        return OrderResolution(
            client_order_id=exchange.client_order_id,
            kind=kind,
            ownership=ownership,
            local=None,
            exchange=exchange,
            effective_status=exchange.status,
            cumulative_filled_contracts=exchange.filled_contracts,
            reason=reason,
            claim=claim,
        )

    @staticmethod
    def _local_resolution(
        local: Mapping[str, Any],
        exchange: ExchangeOrderSnapshot | None,
        *,
        exchange_was_open: bool,
        exact_was_queried: bool,
        linked_fill_contracts: int,
        fill_identity_conflicts: tuple[str, ...],
    ) -> OrderResolution:
        client_id = str(local["client_order_id"])
        quantity = int(local["quantity_contracts"])
        previous_filled = int(_local_value(local, "cumulative_filled_contracts", 0))
        observed_filled = max(previous_filled, linked_fill_contracts)
        ownership = OrderOwnership(str(_local_value(local, "ownership", "BOT")))

        if fill_identity_conflicts:
            return OrderResolution(
                client_order_id=client_id,
                kind=ReconciliationKind.AMBIGUOUS,
                ownership=ownership,
                local=local,
                exchange=exchange,
                effective_status=ExchangeOrderStatus.UNKNOWN,
                cumulative_filled_contracts=previous_filled,
                reason="fill identity conflicts with local order: "
                + ", ".join(fill_identity_conflicts),
            )

        if linked_fill_contracts > quantity:
            return OrderResolution(
                client_order_id=client_id,
                kind=ReconciliationKind.AMBIGUOUS,
                ownership=ownership,
                local=local,
                exchange=exchange,
                effective_status=ExchangeOrderStatus.UNKNOWN,
                cumulative_filled_contracts=previous_filled,
                reason="cumulative trade evidence exceeds the local order quantity",
            )

        if linked_fill_contracts < previous_filled:
            return OrderResolution(
                client_order_id=client_id,
                kind=ReconciliationKind.AMBIGUOUS,
                ownership=ownership,
                local=local,
                exchange=exchange,
                effective_status=ExchangeOrderStatus.UNKNOWN,
                cumulative_filled_contracts=previous_filled,
                reason=(
                    "complete trade history is behind the locally persisted "
                    "cumulative fill"
                ),
            )

        if exchange is not None:
            mismatches = Reconciler._semantic_mismatches(local, exchange)
            if mismatches:
                return OrderResolution(
                    client_order_id=client_id,
                    kind=ReconciliationKind.AMBIGUOUS,
                    ownership=ownership,
                    local=local,
                    exchange=exchange,
                    effective_status=ExchangeOrderStatus.UNKNOWN,
                    cumulative_filled_contracts=previous_filled,
                    reason=(
                        "clientOrderId identity conflicts with exchange fields: "
                        + ", ".join(mismatches)
                    ),
                )
            if exchange.filled_contracts < observed_filled:
                return OrderResolution(
                    client_order_id=client_id,
                    kind=ReconciliationKind.AMBIGUOUS,
                    ownership=ownership,
                    local=local,
                    exchange=exchange,
                    effective_status=ExchangeOrderStatus.UNKNOWN,
                    cumulative_filled_contracts=previous_filled,
                    reason=(
                        "exchange cumulative fill regressed behind durable trade evidence"
                    ),
                )
            if linked_fill_contracts < exchange.filled_contracts:
                return OrderResolution(
                    client_order_id=client_id,
                    kind=ReconciliationKind.AMBIGUOUS,
                    ownership=ownership,
                    local=local,
                    exchange=exchange,
                    effective_status=ExchangeOrderStatus.UNKNOWN,
                    cumulative_filled_contracts=previous_filled,
                    reason=(
                        "complete trade history is missing part of the exchange "
                        "cumulative fill"
                    ),
                )
            if not exchange_was_open and not exchange.is_terminal:
                return OrderResolution(
                    client_order_id=client_id,
                    kind=ReconciliationKind.AMBIGUOUS,
                    ownership=ownership,
                    local=local,
                    exchange=exchange,
                    effective_status=ExchangeOrderStatus.UNKNOWN,
                    cumulative_filled_contracts=previous_filled,
                    reason=(
                        "exact lookup returned an active order absent from the "
                        "complete open-order snapshot"
                    ),
                )
            observed_filled = max(observed_filled, exchange.filled_contracts)
            if exchange.status is ExchangeOrderStatus.PARTIALLY_FILLED:
                kind = ReconciliationKind.PARTIAL
            elif exchange.is_terminal:
                kind = ReconciliationKind.TERMINAL
            else:
                kind = ReconciliationKind.MATCHED
            return OrderResolution(
                client_order_id=client_id,
                kind=kind,
                ownership=ownership,
                local=local,
                exchange=exchange,
                effective_status=exchange.status,
                cumulative_filled_contracts=observed_filled,
                reason="exchange order is authoritative",
            )

        local_state = str(_local_value(local, "local_state", ""))
        persisted_status: ExchangeOrderStatus | None = None
        if local_state == OrderLocalState.TERMINAL.value:
            try:
                persisted_status = ExchangeOrderStatus(
                    str(_local_value(local, "exchange_status"))
                )
            except ValueError as exc:
                raise ValueError(
                    "terminal local order has an invalid exchange status"
                ) from exc
            if (
                persisted_status is ExchangeOrderStatus.REJECTED
                and observed_filled != 0
            ):
                return OrderResolution(
                    client_order_id=client_id,
                    kind=ReconciliationKind.AMBIGUOUS,
                    ownership=ownership,
                    local=local,
                    exchange=None,
                    effective_status=ExchangeOrderStatus.UNKNOWN,
                    cumulative_filled_contracts=previous_filled,
                    reason=(
                        "REJECTED terminal state conflicts with real fill evidence"
                    ),
                )

        if linked_fill_contracts == quantity and quantity > 0:
            return OrderResolution(
                client_order_id=client_id,
                kind=ReconciliationKind.TERMINAL,
                ownership=ownership,
                local=local,
                exchange=None,
                effective_status=ExchangeOrderStatus.FILLED,
                cumulative_filled_contracts=quantity,
                reason="complete REST trade evidence proves FILLED",
            )

        if local_state == OrderLocalState.TERMINAL.value:
            assert persisted_status is not None
            return OrderResolution(
                client_order_id=client_id,
                kind=ReconciliationKind.TERMINAL,
                ownership=ownership,
                local=local,
                exchange=None,
                effective_status=persisted_status,
                cumulative_filled_contracts=observed_filled,
                reason="persisted terminal fact remains terminal",
            )
        if local_state == OrderLocalState.ACK_UNKNOWN.value:
            kind = ReconciliationKind.AMBIGUOUS
            reason = "ACK_UNKNOWN was not resolved by exact order or complete fills"
        elif exact_was_queried:
            kind = ReconciliationKind.LOCAL_ONLY
            reason = "not open and exact lookup found no order; terminal history is insufficient"
        else:
            kind = ReconciliationKind.AMBIGUOUS
            reason = "missing open order was not resolved by exact clientOrderId lookup"
        effective_status = ExchangeOrderStatus.UNKNOWN
        return OrderResolution(
            client_order_id=client_id,
            kind=kind,
            ownership=ownership,
            local=local,
            exchange=None,
            effective_status=effective_status,
            cumulative_filled_contracts=observed_filled,
            reason=reason,
        )

    @staticmethod
    def _local_fill_evidence(
        local: Mapping[str, Any] | None,
        *,
        client_id: str,
        exchange_order_ids: set[Any],
        fill_evidence: Sequence[_FillEvidence],
    ) -> tuple[int, tuple[str, ...]]:
        if local is None:
            return 0, ()
        contracts = 0
        conflicts: list[str] = []
        expected_account = str(local.get("account_id", ""))
        expected_symbol = str(local.get("symbol", ""))
        expected_side = str(local.get("side", "")).lower()
        expected_position_side = str(local.get("position_side", "")).lower()
        for evidence in fill_evidence:
            by_client = evidence.client_order_id == client_id
            by_exchange = evidence.exchange_order_id in exchange_order_ids
            if not by_client and not by_exchange:
                continue
            mismatch_fields: list[str] = []
            if (
                evidence.client_order_id is not None
                and evidence.client_order_id != client_id
            ):
                mismatch_fields.append("client_order_id")
            if (
                exchange_order_ids
                and evidence.exchange_order_id not in exchange_order_ids
            ):
                mismatch_fields.append("exchange_order_id")
            if expected_account and evidence.account_id != expected_account:
                mismatch_fields.append("account_id")
            if expected_symbol and evidence.symbol != expected_symbol:
                mismatch_fields.append("symbol")
            if evidence.side is not None and evidence.side != expected_side:
                mismatch_fields.append("side")
            if (
                evidence.position_side is not None
                and evidence.position_side != expected_position_side
            ):
                mismatch_fields.append("position_side")
            if mismatch_fields:
                conflicts.append(
                    "{}({})".format(
                        evidence.key[2],
                        "/".join(mismatch_fields),
                    )
                )
            else:
                contracts += evidence.contracts
        return contracts, tuple(conflicts)

    @staticmethod
    def _observed_fill_matches_resolution(
        fill: ExchangeFill,
        resolution: OrderResolution,
        exchange_order_ids: set[Any],
    ) -> bool:
        if not (
            fill.client_order_id == resolution.client_order_id
            or fill.exchange_order_id in exchange_order_ids
        ):
            return False
        if (
            fill.client_order_id is not None
            and fill.client_order_id != resolution.client_order_id
        ):
            return False
        if exchange_order_ids and fill.exchange_order_id not in exchange_order_ids:
            return False
        source: Mapping[str, Any] | None = resolution.local or resolution.claim
        if source is not None:
            if fill.account_id != str(source.get("account_id", fill.account_id)):
                return False
            if fill.symbol != str(source.get("symbol", fill.symbol)):
                return False
            if fill.side.value != str(source.get("side", fill.side.value)).lower():
                return False
            if fill.position_side.value != str(
                source.get("position_side", fill.position_side.value)
            ).lower():
                return False
        if resolution.exchange is not None:
            exchange = resolution.exchange
            if (
                fill.symbol != exchange.symbol
                or fill.side is not exchange.side
                or fill.position_side is not exchange.position_side
            ):
                return False
        return True

    @staticmethod
    def _fill_evidence_matches_resolution(
        fill: _FillEvidence,
        resolution: OrderResolution,
    ) -> bool:
        """Prove that merged durable/REST fill evidence belongs to one order."""

        exchange_order_ids = {
            str(value)
            for value in (
                None
                if resolution.local is None
                else resolution.local.get("exchange_order_id"),
                None
                if resolution.claim is None
                else resolution.claim.get("exchange_order_id"),
                None
                if resolution.exchange is None
                else resolution.exchange.exchange_order_id,
            )
            if value is not None
        }
        if not (
            fill.client_order_id == resolution.client_order_id
            or (
                fill.exchange_order_id is not None
                and fill.exchange_order_id in exchange_order_ids
            )
        ):
            return False
        if (
            fill.client_order_id is not None
            and fill.client_order_id != resolution.client_order_id
        ):
            return False
        if (
            exchange_order_ids
            and fill.exchange_order_id is not None
            and fill.exchange_order_id not in exchange_order_ids
        ):
            return False
        source: Mapping[str, Any] | None = resolution.local or resolution.claim
        if source is not None:
            if fill.account_id != str(source.get("account_id", fill.account_id)):
                return False
            if fill.symbol != str(source.get("symbol", fill.symbol)):
                return False
            if (
                fill.side is not None
                and fill.side != str(source.get("side", fill.side)).lower()
            ):
                return False
            if (
                fill.position_side is not None
                and fill.position_side
                != str(source.get("position_side", fill.position_side)).lower()
            ):
                return False
        if resolution.exchange is not None:
            exchange = resolution.exchange
            if fill.symbol != exchange.symbol:
                return False
            if fill.side is not None and fill.side != exchange.side.value:
                return False
            if (
                fill.position_side is not None
                and fill.position_side != exchange.position_side.value
            ):
                return False
        return True

    @classmethod
    def _project_bot_positions(
        cls,
        initial_positions: Sequence[Mapping[str, Any]],
        resolutions: Sequence[OrderResolution],
        fill_evidence: Sequence[_FillEvidence],
    ) -> tuple[tuple[Mapping[str, Any], ...], tuple[str, ...]]:
        """Rebuild strategy exposure from its baseline and unique real fills.

        ``positions`` is an exchange observation cache and is deliberately not
        involved.  Only fills that can be tied to exactly one durable BOT
        ownership record affect the strategy projection.
        """

        quantities: dict[tuple[str, PositionSide], int] = {}
        for item in initial_positions:
            key = (str(item["symbol"]), PositionSide(str(item["position_side"])))
            if key in quantities:
                raise ValueError("duplicate initial strategy position key")
            contracts = item["quantity_contracts"]
            if isinstance(contracts, bool) or not isinstance(contracts, int):
                raise ValueError("initial strategy position must use integer contracts")
            quantities[key] = contracts

        blockers: list[str] = []
        bot_resolutions = [
            item for item in resolutions if item.ownership is OrderOwnership.BOT
        ]
        for fill in fill_evidence:
            owners = [
                resolution
                for resolution in bot_resolutions
                if cls._fill_evidence_matches_resolution(fill, resolution)
            ]
            if not owners:
                # Unowned/manual fills are account truth, but they are not
                # strategy execution and therefore cannot mutate its intent.
                continue
            if len(owners) != 1:
                blockers.append(
                    "AMBIGUOUS_POSITION_FILL:{}:multiple BOT owners".format(
                        fill.key[2]
                    )
                )
                continue
            owner = owners[0]
            source: Mapping[str, Any] | None = owner.local or owner.claim
            side = fill.side
            position_side = fill.position_side
            if source is not None:
                side = side or str(source.get("side", "")).lower()
                position_side = position_side or str(
                    source.get("position_side", "")
                ).lower()
            if side not in {"buy", "sell"}:
                blockers.append(
                    f"AMBIGUOUS_POSITION_FILL:{fill.key[2]}:side is missing"
                )
                continue
            if position_side != PositionSide.BOTH.value:
                blockers.append(
                    "UNSUPPORTED_POSITION_SIDE:{}:{}".format(
                        fill.key[2], position_side or "missing"
                    )
                )
                continue
            key = (fill.symbol, PositionSide.BOTH)
            delta = fill.contracts if side == "buy" else -fill.contracts
            quantities[key] = quantities.get(key, 0) + delta

        projected = tuple(
            {
                "symbol": symbol,
                "position_side": position_side.value,
                "quantity_contracts": contracts,
            }
            for (symbol, position_side), contracts in sorted(
                quantities.items(),
                key=lambda item: (item[0][0], item[0][1].value),
            )
        )
        return projected, tuple(blockers)

    @staticmethod
    def _semantic_mismatches(
        record: Mapping[str, Any],
        exchange: ExchangeOrderSnapshot,
    ) -> tuple[str, ...]:
        mismatches: list[str] = []

        def enum_text(value: Any) -> str:
            return str(getattr(value, "value", value)).lower()

        expected_values = (
            ("symbol", exchange.symbol, str),
            ("side", exchange.side.value, enum_text),
            ("position_side", exchange.position_side.value, enum_text),
            ("order_type", exchange.order_type, lambda value: str(value).upper()),
            (
                "time_in_force",
                exchange.time_in_force,
                lambda value: str(value).upper(),
            ),
        )
        for field_name, actual, normalize in expected_values:
            if field_name in record and record[field_name] is not None:
                if normalize(record[field_name]) != actual:
                    mismatches.append(field_name)

        if "quantity_contracts" in record and record["quantity_contracts"] is not None:
            expected_contracts = record["quantity_contracts"]
            if (
                isinstance(expected_contracts, bool)
                or not isinstance(expected_contracts, int)
                or expected_contracts != exchange.original_contracts
            ):
                mismatches.append("quantity_contracts")

        if "price" in record and record["price"] is not None:
            try:
                expected_price = Decimal(str(record["price"]))
            except (InvalidOperation, TypeError, ValueError):
                mismatches.append("price")
            else:
                if exchange.price is None or expected_price != exchange.price:
                    mismatches.append("price")

        if "reduce_only" in record and record["reduce_only"] is not None:
            expected_reduce_only = record["reduce_only"]
            if expected_reduce_only in (0, 1, False, True):
                if bool(expected_reduce_only) is not exchange.reduce_only:
                    mismatches.append("reduce_only")
            else:
                mismatches.append("reduce_only")

        exchange_order_id = record.get("exchange_order_id")
        if (
            exchange_order_id is not None
            and str(exchange_order_id) != exchange.exchange_order_id
        ):
            mismatches.append("exchange_order_id")
        return tuple(mismatches)

    @staticmethod
    def reconcile_positions(
        local_positions: Sequence[Mapping[str, Any]],
        exchange_positions: Sequence[ExchangePosition],
    ) -> tuple[PositionResolution, ...]:
        local_by_key = {
            (str(item["symbol"]), PositionSide(str(item["position_side"]))): int(
                item["quantity_contracts"]
            )
            for item in local_positions
        }
        exchange_by_key: dict[tuple[str, PositionSide], ExchangePosition] = {}
        for item in exchange_positions:
            key = (item.symbol, item.position_side)
            previous = exchange_by_key.get(key)
            if previous is not None and previous != item:
                raise ValueError(
                    "duplicate position key carries conflicting exchange data"
                )
            exchange_by_key[key] = item
        keys = set(local_by_key).union(exchange_by_key)
        result = []
        for symbol, side in sorted(keys, key=lambda value: (value[0], value[1].value)):
            local_contracts = local_by_key.get((symbol, side), 0)
            exchange = exchange_by_key.get((symbol, side))
            exchange_contracts = 0 if exchange is None else exchange.contracts
            result.append(
                PositionResolution(
                    symbol=symbol,
                    position_side=side,
                    local_contracts=local_contracts,
                    exchange_contracts=exchange_contracts,
                    exchange=exchange,
                    kind=(
                        ReconciliationKind.MATCHED
                        if local_contracts == exchange_contracts
                        else ReconciliationKind.POSITION_MISMATCH
                    ),
                )
            )
        return tuple(result)


__all__ = ["Reconciler"]
