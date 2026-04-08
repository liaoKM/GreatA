"""Unified incremental fetch entrypoint."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))

from data_service.fetch_pipeline import run_fetch


def main() -> None:
    parser = argparse.ArgumentParser(description="Unified data fetch")
    parser.add_argument("--until", required=True, help="Inclusive cutoff date in YYYY-MM-DD format")
    parser.add_argument(
        "--type",
        default="all",
        choices=["market", "benchmark", "finance", "announcement", "all"],
        help="Target data type",
    )
    parser.add_argument("--data-path", default="./data", help="Root data directory")
    args = parser.parse_args()

    results = run_fetch(
        until=args.until,
        data_type=args.type,
        data_path=args.data_path,
    )

    for data_type, result in results.items():
        print(f"[{data_type}] {result.status}")
        print(f"  range: {result.start:%Y-%m-%d} -> {result.end:%Y-%m-%d}")
        print(f"  rows: {result.row_count}")
        print(f"  files: {len(result.written_paths)}")
        print(f"  consolidate: {len(result.consolidation_actions)} actions")


if __name__ == "__main__":
    main()
