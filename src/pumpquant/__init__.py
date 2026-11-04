"""PumpQuant: backtesting engine for Pump.fun, Solana's token-launch market.

The package follows the path a trade idea takes, from raw chain data to a verdict:

    acquisition   fetch and archive raw transactions, blocks and streams
    decoding      decode Solana wire bytes and Pump/PumpSwap program events
    state         rebuild ordered market state, with when it was observable
    features      compute causal features at a decision boundary
    pricing       quote trades exactly, in integer protocol units
    execution     replay entries and exits with latency, retries, fees and capital
    research      search strategies chronologically, with every trial recorded
    validation    statistical and economic gates a candidate must pass
    audits        the audit that retired the live rule, kept as evidence
    dashboard     the read-only research dashboard and its local preview

``pumpquant demo`` runs the whole path on generated data in about a second.
"""

__version__ = "0.1.0"
