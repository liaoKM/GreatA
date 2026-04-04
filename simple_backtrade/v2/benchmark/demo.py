"""Minimal CLI for running a custom benchmark demo."""

from __future__ import annotations

import argparse

from ..models import TargetPortfolio
from ..strategy.base import BaseStrategy
from .config import CustomIndexConfig
from .engine import run_custom_index


class FinanceEventEqualWeightStrategy(BaseStrategy):
    """A minimal demo strategy that equal-weights stocks touched by finance events."""

    def __init__(self):
        self.members: set[str] = set()

    def on_events(self, context, events) -> None:
        for event in events:
            if event.source == "finance":
                self.members.add(event.stock_code)

    def on_bar_close(self, context):
        return TargetPortfolio.equal_weight(sorted(self.members)) if self.members else None


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the v2 custom benchmark demo")
    parser.add_argument("--start", required=True, help="Index start date, for example 2026-01-05")
    parser.add_argument("--end", required=True, help="Index end date, for example 2026-01-31")
    parser.add_argument("--data-path", default="./data", help="Root data directory")
    parser.add_argument("--initial-level", type=float, default=1000.0, help="Initial index level")
    parser.add_argument("--warmup-bars", type=int, default=60, help="Warmup bars loaded before start")
    args = parser.parse_args()

    config = CustomIndexConfig(
        start=args.start,
        end=args.end,
        data_path=args.data_path,
        initial_level=args.initial_level,
        warmup_bars=args.warmup_bars,
    )
    result = run_custom_index(config, FinanceEventEqualWeightStrategy())

    print("Summary")
    for key, value in result.summary.items():
        print(f"  {key}: {value}")

    print("")
    print("Index Curve")
    print(result.equity_curve.tail())


if __name__ == "__main__":
    main()
