"""PumpQuant: execution-aware strategy research for Pump.fun token markets.

The package follows the path a trade idea takes, from raw chain data to a verdict:

    acquisition   fetch and archive raw transactions, blocks and streams
    decoding      decode Solana wire bytes and Pump/PumpSwap program events
    state         rebuild ordered market state, with when it was observable
    features      compute causal features at an explicit decision boundary
    pricing       quote trades exactly, in integer protocol units
    execution     replay entries and exits with latency, retries, fees and capital
    research      search strategies chronologically, with every trial recorded
    validation    statistical and economic gates a candidate must pass
    audits        historical investigations, including the one that rejected M4
    dashboard     the read-only research dashboard and its local preview

``pumpquant demo`` runs the whole path on generated data in about a second.
"""

__version__ = "0.1.0"
