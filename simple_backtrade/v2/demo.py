"""Minimal CLI for running the v2 backtest framework."""

from __future__ import annotations

import argparse

from .config import BacktestConfig
from .engine.runner import run_backtest
from .strategy.empty import EmptyStrategy


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the v2 local backtest demo")
    parser.add_argument("--start", required=True, help="Backtest start date, for example 2026-01-05")
    parser.add_argument("--end", required=True, help="Backtest end date, for example 2026-01-31")
    parser.add_argument("--data-path", default="./data", help="Root data directory")
    parser.add_argument("--initial-cash", type=float, default=1_000_000, help="Initial account cash")
    parser.add_argument("--warmup-bars", type=int, default=60, help="Warmup bars loaded before start")
    args = parser.parse_args()

    config = BacktestConfig(
        start=args.start,
        end=args.end,
        data_path=args.data_path,
        initial_cash=args.initial_cash,
        warmup_bars=args.warmup_bars,
    )
    result = run_backtest(config, EmptyStrategy())

    print("Summary")
    for key, value in result.summary.items():
        print(f"  {key}: {value}")

    print("")
    print("Equity Curve")
    print(result.equity_curve.tail())


if __name__ == "__main__":
    main()
