import argparse

from simple_backtrade.config import BacktestConfig
from simple_backtrade.engine.runner import run_backtest
from simple_backtrade.strategy.empty import EmptyStrategy
from simple_backtrade.benchmark import (
    CustomIndexBenchmarkProvider,
    CustomIndexConfig,
    CustomIndexEngine,
    FinanceGrowthIndexStrategy,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the v2 local backtest demo")
    parser.add_argument("--start", required=True, help="Backtest start date, for example 2026-01-05")
    parser.add_argument("--end", required=True, help="Backtest end date, for example 2026-01-31")
    parser.add_argument("--data-path", default="./data", help="Root data directory")
    parser.add_argument("--initial-cash", type=float, default=1_000_000, help="Initial account cash")
    parser.add_argument("--warmup-bars", type=int, default=60, help="Warmup bars loaded before start")
    args = parser.parse_args()

    result = CustomIndexEngine(
            CustomIndexConfig(
                start="2025-01-01",
                end="2026-01-01",
                data_path=args.data_path,
                warmup_bars=0,
                benchmark='tushare:000300.SH'
            ),
            FinanceGrowthIndexStrategy(),
        ).run()

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
