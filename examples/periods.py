"""Print each test period's result from the synthetic demo (the README's Python sample)."""

from pumpquant.demo import run_demo

run = run_demo(tokens=36, seed=7)
for fold in run.report.folds:
    result = fold.test_backtest
    pnl = result.total_quote_pnl
    print(f"period {fold.number + 1}: {result.position_outcomes} outcomes, {pnl:+.3f} SOL")
