"""Causal state machine for the frozen M4 first-stair live rule."""

from __future__ import annotations

from dataclasses import dataclass, field

LAMPORTS_PER_SOL = 1_000_000_000


@dataclass(frozen=True, slots=True)
class M4Decision:
    pool: str
    mint: str
    decision_time_ns: int
    quote_liquidity_sol: float | None
    action: str
    reason: str
    source_slot: int | None
    source_event_time_ns: int | None = None
    first_trade_event_time_ns: int | None = None
    first_trade_observed_time_ns: int | None = None


@dataclass(slots=True)
class _Pool:
    pool: str
    mint: str
    create_slot: int
    first_trade_time_ns: int | None = None
    first_trade_observed_time_ns: int | None = None
    latest_event_time_ns: int | None = None
    latest_observed_time_ns: int | None = None
    latest_quote_lamports: int | None = None
    latest_slot: int | None = None
    cashback_seen: bool = False
    nonzero_virtual_quote_seen: bool = False
    decided: bool = False


@dataclass(slots=True)
class M4LiveEngine:
    checkpoint_seconds: int = 8
    maximum_quote_liquidity_sol: float = 0.908108808
    maximum_decision_lateness_ms: float = 1_500.0
    pools: dict[str, _Pool] = field(default_factory=dict)

    def observe_pool(self, *, pool: str, mint: str, slot: int) -> None:
        prior = self.pools.get(pool)
        if prior is not None and prior.mint != mint:
            raise RuntimeError("pool-to-mint mapping changed")
        if prior is None:
            self.pools[pool] = _Pool(pool=pool, mint=mint, create_slot=slot)

    def observe_trade(
        self,
        *,
        pool: str,
        event_time_ns: int,
        post_quote_lamports: int,
        cashback_fee_bps: int,
        slot: int,
        virtual_quote_reserves: int = 0,
        observed_time_ns: int | None = None,
    ) -> None:
        state = self.pools.get(pool)
        if state is None or state.decided:
            return
        observed_ns = event_time_ns if observed_time_ns is None else observed_time_ns
        if event_time_ns < 0 or observed_ns < 0 or post_quote_lamports < 0 or cashback_fee_bps < 0:
            raise ValueError("trade fields must be non-negative")
        if state.first_trade_time_ns is None:
            state.first_trade_time_ns = event_time_ns
            state.first_trade_observed_time_ns = observed_ns
        assert state.first_trade_observed_time_ns is not None
        deadline = state.first_trade_observed_time_ns + self.checkpoint_seconds * 1_000_000_000
        if observed_ns <= deadline and (
            state.latest_observed_time_ns is None
            or observed_ns > state.latest_observed_time_ns
            or (
                observed_ns == state.latest_observed_time_ns
                and (
                    state.latest_event_time_ns is None
                    or event_time_ns >= state.latest_event_time_ns
                )
            )
        ):
            state.latest_event_time_ns = event_time_ns
            state.latest_observed_time_ns = observed_ns
            state.latest_quote_lamports = post_quote_lamports
            state.latest_slot = slot
            state.cashback_seen = state.cashback_seen or cashback_fee_bps > 0
            state.nonzero_virtual_quote_seen = (
                state.nonzero_virtual_quote_seen or virtual_quote_reserves != 0
            )

    def due_decisions(self, now_ns: int) -> tuple[M4Decision, ...]:
        decisions: list[M4Decision] = []
        for state in self.pools.values():
            if state.decided or state.first_trade_observed_time_ns is None:
                continue
            deadline = state.first_trade_observed_time_ns + self.checkpoint_seconds * 1_000_000_000
            if now_ns < deadline:
                continue
            state.decided = True
            lateness_ms = (now_ns - deadline) / 1_000_000
            liquidity = (
                None
                if state.latest_quote_lamports is None
                else state.latest_quote_lamports / LAMPORTS_PER_SOL
            )
            if lateness_ms > self.maximum_decision_lateness_ms:
                action, reason = "skip", "decision_stale"
            elif state.cashback_seen:
                action, reason = "skip", "cashback"
            elif state.nonzero_virtual_quote_seen:
                action, reason = "skip", "unmodeled_virtual_quote_reserves"
            elif liquidity is None:
                action, reason = "skip", "missing_reserve_state"
            elif liquidity <= self.maximum_quote_liquidity_sol:
                action, reason = "enter", "m4_liquidity_edge"
            else:
                action, reason = "skip", "liquidity_above_threshold"
            decisions.append(
                M4Decision(
                    pool=state.pool,
                    mint=state.mint,
                    decision_time_ns=deadline,
                    quote_liquidity_sol=liquidity,
                    action=action,
                    reason=reason,
                    source_slot=state.latest_slot,
                    source_event_time_ns=state.latest_event_time_ns,
                    first_trade_event_time_ns=state.first_trade_time_ns,
                    first_trade_observed_time_ns=state.first_trade_observed_time_ns,
                )
            )
        return tuple(decisions)
