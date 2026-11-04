"""The ``pumpquant`` command line."""

from __future__ import annotations

import argparse

from pumpquant.cli import bigquery, research


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="pumpquant",
        description="Backtesting engine for Pump.fun, Solana's token-launch market",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    research.register(subparsers)
    bigquery.register(subparsers)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return int(args.handler(args))
