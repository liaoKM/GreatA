"""Daily data fetch script."""

from __future__ import annotations

import argparse
from datetime import date, datetime
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))

from data_service.daily_fetch_runner import fetch_daily_data


def _print_single_result(data_type: str, result: dict) -> None:
    status = result["status"]
    row_count = result["row_count"]

    if data_type == "announcement":
        print(f"  Fetched announcement data: {status} ({row_count} rows)")
        for ann_type, ann_result in result.get("types", {}).items():
            ann_status = ann_result["status"]
            ann_rows = ann_result["row_count"]
            path = ann_result.get("path")
            if path:
                print(f"    {ann_type}: {ann_status} {path} ({ann_rows} rows)")
            else:
                print(f"    {ann_type}: {ann_status} ({ann_rows} rows)")
        return

    if data_type == "benchmark":
        print(f"  Fetched benchmark data: {status} ({row_count} rows)")
        for benchmark_id, benchmark_result in result.get("benchmarks", {}).items():
            benchmark_status = benchmark_result["status"]
            benchmark_rows = benchmark_result["row_count"]
            path = benchmark_result.get("path")
            if path:
                print(f"    {benchmark_id}: {benchmark_status} {path} ({benchmark_rows} rows)")
            else:
                print(f"    {benchmark_id}: {benchmark_status} ({benchmark_rows} rows)")
        return

    label = {
        "market": "market",
        "benchmark": "benchmark",
        "finance": "finance",
    }.get(data_type, data_type)
    path = result.get("path")
    if path:
        print(f"  Fetched {label} data: {status} {path} ({row_count} rows)")
    else:
        print(f"  Fetched {label} data: {status} ({row_count} rows)")


def fetch_daily(target_date: date, data_type: str = "all", data_path: str = "./data") -> dict:
    """Fetch one day of data and write fetch status files."""
    print(f"Fetching data for {target_date}...")
    results = fetch_daily_data(target_date, data_type=data_type, data_path=data_path)
    for current_type, result in results.items():
        _print_single_result(current_type, result)
    print("Fetch completed.")
    return results


def main() -> None:
    parser = argparse.ArgumentParser(description="Daily data fetch")
    parser.add_argument("--date", type=str, help="Target date in YYYY-MM-DD format, default is today")
    parser.add_argument(
        "--type",
        type=str,
        default="all",
        choices=["market", "benchmark", "finance", "announcement", "all"],
        help="Data type, default is all",
    )
    args = parser.parse_args()

    if args.date:
        target_date = datetime.strptime(args.date, "%Y-%m-%d").date()
    else:
        target_date = date.today()

    fetch_daily(target_date, args.type)


if __name__ == "__main__":
    main()
