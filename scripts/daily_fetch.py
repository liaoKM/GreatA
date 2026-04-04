"""Daily data fetch script."""

from __future__ import annotations

import argparse
from datetime import date, datetime
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from data_service.daily_fetch_runner import fetch_daily_data


def _print_single_result(data_type: str, result: dict) -> None:
    status = result["status"]
    row_count = result["row_count"]

    if data_type == "announcement":
        print(f"  拉取公告数据: {status} ({row_count} 条)")
        for ann_type, ann_result in result.get("types", {}).items():
            ann_status = ann_result["status"]
            ann_rows = ann_result["row_count"]
            path = ann_result.get("path")
            if path:
                print(f"    {ann_type}: {ann_status} {path} ({ann_rows} 条)")
            else:
                print(f"    {ann_type}: {ann_status} ({ann_rows} 条)")
        return

    label = {
        "market": "市场",
        "finance": "财报",
    }.get(data_type, data_type)
    path = result.get("path")
    if path:
        print(f"  拉取{label}数据: {status} {path} ({row_count} 条)")
    else:
        print(f"  拉取{label}数据: {status} ({row_count} 条)")


def fetch_daily(target_date: date, data_type: str = "all", data_path: str = "./data") -> dict:
    """Fetch one day of data and write fetch status files."""
    print(f"开始拉取 {target_date} 的数据...")
    results = fetch_daily_data(target_date, data_type=data_type, data_path=data_path)
    for current_type, result in results.items():
        _print_single_result(current_type, result)
    print("拉取完成!")
    return results


def main() -> None:
    parser = argparse.ArgumentParser(description="每日数据拉取")
    parser.add_argument("--date", type=str, help="指定日期 (YYYY-MM-DD)，默认为今天")
    parser.add_argument(
        "--type",
        type=str,
        default="all",
        choices=["market", "finance", "announcement", "all"],
        help="数据类型，默认为 all",
    )
    args = parser.parse_args()

    if args.date:
        target_date = datetime.strptime(args.date, "%Y-%m-%d").date()
    else:
        target_date = date.today()

    fetch_daily(target_date, args.type)


if __name__ == "__main__":
    main()
