"""Chronological shared-wallet replay for exact, size-dependent trade quotes.

The quoting callback is deliberately injected.  This keeps portfolio accounting
separate from protocol math while still requiring every accepted opportunity to
be re-quoted at the wallet's actual dynamic order size.
"""

from __future__ import annotations

import math
from collections import Counter
from collections.abc import Callable, Sequence
from dataclasses import asdict, dataclass
from typing import Protocol

LAMPORTS_PER_SOL = 1_000_000_000


class SharedWalletReplayError(RuntimeError):
    """A quote or wallet transition violates the replay contract."""


class Opportunity(Protocol):
    mint: str
    signal_time_ns: int
    signal_fx_usd: float


@dataclass(frozen=True, slots=True)
class DynamicTradeQuote:
    entered: bool
    closed: bool
    reason: str
    entry_time_ns: int | None
    exit_time_ns: int | None
    entry_spend_lamports: int
    entry_cost_lamports: int
    exit_credit_lamports: int
    exit_cost_lamports: int
    token_account_rent_lamports: int = 0
    token_account_rent_reclaimed: bool = False

    def __post_init__(self) -> None:
        for name in (
            "entry_spend_lamports",
            "entry_cost_lamports",
            "exit_credit_lamports",
            "exit_cost_lamports",
            "token_account_rent_lamports",
        ):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(f"{name} must be a non-negative integer")
        if self.entered:
            if self.entry_time_ns is None or self.exit_time_ns is None:
                raise ValueError("entered trades require entry and settlement times")
            if self.exit_time_ns < self.entry_time_ns:
                raise ValueError("trade settlement precedes entry")
            if self.entry_spend_lamports <= 0:
                raise ValueError("entered trade has no principal")
        elif self.entry_spend_lamports or self.exit_credit_lamports or self.exit_cost_lamports:
            raise ValueError("non-entered quote contains position cash flows")

    @property
    def entry_debit_lamports(self) -> int:
        return (
            self.entry_spend_lamports + self.entry_cost_lamports + self.token_account_rent_lamports
        )

    @property
    def exit_net_credit_lamports(self) -> int:
        gross_credit = self.exit_credit_lamports + (
            self.token_account_rent_lamports if self.token_account_rent_reclaimed else 0
        )
        if self.exit_cost_lamports > gross_credit:
            return 0
        return gross_credit - self.exit_cost_lamports


@dataclass(frozen=True, slots=True)
class WalletDecision:
    mint: str
    signal_time_ns: int
    accepted: bool
    reason: str
    equity_before_lamports: int
    cash_after_lamports: int
    order_usd: float
    order_budget_lamports: int
    open_position_count: int


@dataclass(frozen=True, slots=True)
class _OpenPosition:
    mint: str
    exit_time_ns: int
    principal_lamports: int
    net_exit_credit_lamports: int


def replay_dynamic_shared_wallet(
    opportunities: Sequence[Opportunity],
    *,
    starting_usd: float,
    starting_quote_lamports: int | None = None,
    fixed_recoverable_rent_lamports: int = 0,
    fixed_rent_close_cost_lamports: int = 0,
    wallet_fraction: float,
    absolute_notional_cap_usd: float,
    max_concurrent_positions: int,
    quote_trade: Callable[[Opportunity, int, float], DynamicTradeQuote],
) -> dict[str, object]:
    """Replay one wallet in chronological order with cost-basis open equity.

    ``quote_trade`` receives the opportunity, exact order budget in lamports,
    and its USD value.  It must re-run the AMM quote at that size; scaling a
    fixed-notional return is not allowed by this interface.
    """

    if not opportunities:
        raise ValueError("shared wallet requires at least one opportunity")
    for name, value in (
        ("starting_usd", starting_usd),
        ("wallet_fraction", wallet_fraction),
        ("absolute_notional_cap_usd", absolute_notional_cap_usd),
    ):
        if not math.isfinite(value) or value <= 0:
            raise ValueError(f"{name} must be positive and finite")
    if wallet_fraction > 1:
        raise ValueError("wallet_fraction cannot exceed one")
    if max_concurrent_positions < 1:
        raise ValueError("max_concurrent_positions must be positive")

    ordered = sorted(
        opportunities,
        key=lambda item: (int(item.signal_time_ns), str(item.mint)),
    )
    first_fx = float(ordered[0].signal_fx_usd)
    if not math.isfinite(first_fx) or first_fx <= 0:
        raise ValueError("first opportunity has invalid causal FX")
    if starting_quote_lamports is not None and (
        isinstance(starting_quote_lamports, bool)
        or not isinstance(starting_quote_lamports, int)
        or starting_quote_lamports <= 0
    ):
        raise ValueError("starting_quote_lamports must be a positive integer")
    for name, value in (
        ("fixed_recoverable_rent_lamports", fixed_recoverable_rent_lamports),
        ("fixed_rent_close_cost_lamports", fixed_rent_close_cost_lamports),
    ):
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ValueError(f"{name} must be a non-negative integer")
    starting_wallet = (
        starting_quote_lamports
        if starting_quote_lamports is not None
        else max(1, math.floor(starting_usd / first_fx * LAMPORTS_PER_SOL))
    )
    if fixed_recoverable_rent_lamports + fixed_rent_close_cost_lamports >= starting_wallet:
        raise ValueError("fixed infrastructure rent and cleanup cost exhaust the wallet")
    # This reserve models a persistent WSOL ATA created in the first successful
    # entry transaction and kept until stair shutdown. Reserving it from time
    # zero is conservative and prevents the replay from spending lamports that
    # must remain available to create/retain the account.
    cash = starting_wallet - fixed_recoverable_rent_lamports
    starting_cash = cash
    minimum_cost_basis_equity = starting_wallet
    maximum_concurrent = 0
    open_positions: list[_OpenPosition] = []
    decisions: list[WalletDecision] = []
    reason_counts: Counter[str] = Counter()

    def cost_basis_equity() -> int:
        return (
            cash
            + fixed_recoverable_rent_lamports
            + sum(item.principal_lamports for item in open_positions)
        )

    def settle(through_ns: int) -> None:
        nonlocal cash, minimum_cost_basis_equity
        ready = sorted(
            (item for item in open_positions if item.exit_time_ns <= through_ns),
            key=lambda item: (item.exit_time_ns, item.mint),
        )
        for item in ready:
            open_positions.remove(item)
            cash += item.net_exit_credit_lamports
            minimum_cost_basis_equity = min(minimum_cost_basis_equity, cost_basis_equity())

    for opportunity in ordered:
        signal_time_ns = int(opportunity.signal_time_ns)
        fx_usd = float(opportunity.signal_fx_usd)
        if not math.isfinite(fx_usd) or fx_usd <= 0:
            raise SharedWalletReplayError("opportunity has invalid causal FX")
        settle(signal_time_ns)
        equity_before = cost_basis_equity()
        equity_usd = equity_before / LAMPORTS_PER_SOL * fx_usd
        order_usd = min(wallet_fraction * equity_usd, absolute_notional_cap_usd)
        order_budget = max(1, math.floor(order_usd / fx_usd * LAMPORTS_PER_SOL))
        rejection: str | None = None
        if any(item.mint == opportunity.mint for item in open_positions):
            rejection = "mint_already_open"
        elif len(open_positions) >= max_concurrent_positions:
            rejection = "max_concurrent_positions"

        if rejection is not None:
            reason_counts[rejection] += 1
            decisions.append(
                WalletDecision(
                    str(opportunity.mint),
                    signal_time_ns,
                    False,
                    rejection,
                    equity_before,
                    cash,
                    order_usd,
                    order_budget,
                    len(open_positions),
                )
            )
            continue

        quote = quote_trade(opportunity, order_budget, order_usd)
        if quote.entry_debit_lamports > cash:
            reason_counts["insufficient_cash"] += 1
            decisions.append(
                WalletDecision(
                    str(opportunity.mint),
                    signal_time_ns,
                    False,
                    "insufficient_cash",
                    equity_before,
                    cash,
                    order_usd,
                    order_budget,
                    len(open_positions),
                )
            )
            continue

        cash -= quote.entry_debit_lamports
        if cash < 0:
            raise SharedWalletReplayError("wallet cash became negative")
        accepted = quote.entered
        if quote.entered:
            assert quote.exit_time_ns is not None  # guaranteed for entered quotes
            open_positions.append(
                _OpenPosition(
                    str(opportunity.mint),
                    quote.exit_time_ns,
                    quote.entry_spend_lamports + quote.token_account_rent_lamports,
                    quote.exit_net_credit_lamports,
                )
            )
            maximum_concurrent = max(maximum_concurrent, len(open_positions))
        reason_counts[quote.reason] += 1
        minimum_cost_basis_equity = min(minimum_cost_basis_equity, cost_basis_equity())
        decisions.append(
            WalletDecision(
                str(opportunity.mint),
                signal_time_ns,
                accepted,
                quote.reason,
                equity_before,
                cash,
                order_usd,
                order_budget,
                len(open_positions),
            )
        )

    for position in sorted(open_positions, key=lambda item: (item.exit_time_ns, item.mint)):
        cash += position.net_exit_credit_lamports
        open_positions.remove(position)
        minimum_cost_basis_equity = min(minimum_cost_basis_equity, cost_basis_equity())

    fixed_rent_cleanup_funded = cash >= fixed_rent_close_cost_lamports
    if fixed_rent_cleanup_funded:
        cash += fixed_recoverable_rent_lamports
        cash -= fixed_rent_close_cost_lamports
    final_fx = float(ordered[-1].signal_fx_usd)
    final_usd = cash / LAMPORTS_PER_SOL * final_fx
    return {
        "kind": "pumpquant.dynamic_shared_wallet_replay",
        "version": 1,
        "starting_usd": starting_usd,
        "starting_usd_at_first_causal_fx": starting_wallet / LAMPORTS_PER_SOL * first_fx,
        "starting_wallet_lamports": starting_wallet,
        "starting_cash_lamports": starting_cash,
        "fixed_recoverable_rent_lamports": fixed_recoverable_rent_lamports,
        "fixed_rent_close_cost_lamports": fixed_rent_close_cost_lamports,
        "fixed_rent_cleanup_funded": fixed_rent_cleanup_funded,
        "final_cash_lamports": cash,
        "final_usd_at_last_causal_fx": final_usd,
        "quote_return": cash / starting_wallet - 1.0,
        "minimum_cost_basis_equity_lamports": minimum_cost_basis_equity,
        "maximum_concurrent_positions": maximum_concurrent,
        "accepted_trades": sum(item.accepted for item in decisions),
        "rejected_signals": sum(not item.accepted for item in decisions),
        "reason_counts": dict(sorted(reason_counts.items())),
        "decisions": [asdict(item) for item in decisions],
    }


__all__ = [
    "DynamicTradeQuote",
    "SharedWalletReplayError",
    "WalletDecision",
    "replay_dynamic_shared_wallet",
]
