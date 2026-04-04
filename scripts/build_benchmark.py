"""Build and persist local benchmark data."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))

from simple_backtrade.benchmark import build_registered_custom_benchmark


def main() -> None:
    parser = argparse.ArgumentParser(description="Build and persist one custom benchmark")
    parser.add_argument("--benchmark-id", required=True, help="Benchmark id, for example custom:finance_growth")
    parser.add_argument("--start", required=True, help="Start date in YYYY-MM-DD format")
    parser.add_argument("--end", required=True, help="End date in YYYY-MM-DD format")
    parser.add_argument("--data-path", default="./data", help="Root data directory")
    parser.add_argument("--initial-level", type=float, default=1000.0, help="Initial index level")
    parser.add_argument("--warmup-bars", type=int, default=60, help="Warmup bars loaded before start")
    args = parser.parse_args()

    result, written_paths = build_registered_custom_benchmark(
        benchmark_id=args.benchmark_id,
        start=args.start,
        end=args.end,
        data_path=args.data_path,
        initial_level=args.initial_level,
        warmup_bars=args.warmup_bars,
    )

    print("Summary")
    for key, value in result.summary.items():
        print(f"  {key}: {value}")

    print("")
    print("Written")
    for path in written_paths:
        print(f"  {path}")


if __name__ == "__main__":
    main()
