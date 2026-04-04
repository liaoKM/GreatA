"""Aggregate daily data into monthly and yearly parquet files."""

from __future__ import annotations

import argparse
from calendar import monthrange
from datetime import date, datetime
from functools import lru_cache
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).parent.parent))

from data_service import (
    AnnouncementAggregator,
    FinanceAggregator,
    MarketAggregator,
    TushareMarketFetcher,
)
from data_service.fetch_status import get_fetch_status_root, summarize_month_status
from data_service.fetcher.announcement_fetcher import ANNOUNCEMENT_TYPES


DATA_TYPES = ["market", "finance", "announcement"]
AGGREGATORS = {
    "market": MarketAggregator,
    "finance": FinanceAggregator,
    "announcement": AnnouncementAggregator,
}


def _normalize_reference_date(reference_date: date | datetime | None) -> date:
    if reference_date is None:
        return date.today()
    if isinstance(reference_date, datetime):
        return reference_date.date()
    return reference_date


def _month_end(year: int, month: int) -> date:
    return date(year, month, monthrange(year, month)[1])


def _is_month_closed(year: int, month: int, reference_date: date) -> bool:
    return reference_date > _month_end(year, month)


def _is_year_closed(year: int, reference_date: date) -> bool:
    return reference_date > date(year, 12, 31)


@lru_cache(maxsize=1)
def _market_fetcher() -> TushareMarketFetcher:
    return TushareMarketFetcher()


@lru_cache(maxsize=256)
def get_expected_trade_days(year: int, month: int) -> tuple[date, ...]:
    """Return actual open-market dates for one month."""
    start = datetime(year, month, 1)
    end = datetime(year, month, monthrange(year, month)[1])
    trade_calendar = _market_fetcher().fetch_trade_calendar(start, end)
    if trade_calendar is None or trade_calendar.empty:
        return tuple()

    trade_calendar = trade_calendar.copy()
    trade_calendar["is_open"] = trade_calendar["is_open"].astype(str)
    open_dates = trade_calendar.loc[trade_calendar["is_open"] == "1", "cal_date"].tolist()
    normalized = {
        datetime.strptime(str(cal_date), "%Y%m%d").date()
        for cal_date in open_dates
    }
    return tuple(sorted(normalized))


def _parse_daily_file_dates(file_paths: list[Path]) -> set[date]:
    dates: set[date] = set()
    for file_path in file_paths:
        try:
            dates.add(datetime.strptime(file_path.stem, "%Y-%m-%d").date())
        except ValueError:
            continue
    return dates


def _parse_monthly_file_months(file_paths: list[Path]) -> set[int]:
    months: set[int] = set()
    for file_path in file_paths:
        try:
            months.add(datetime.strptime(file_path.stem, "%Y-%m").month)
        except ValueError:
            continue
    return months


def _extract_date_from_status_path(file_path: Path) -> date | None:
    try:
        return datetime.strptime(file_path.stem, "%Y-%m-%d").date()
    except ValueError:
        return None


def _format_dates(dates: list[date], limit: int = 10) -> str:
    if not dates:
        return "-"
    formatted = [current.isoformat() for current in dates[:limit]]
    suffix = " ..." if len(dates) > limit else ""
    return ", ".join(formatted) + suffix


def _describe_result(result: Any) -> str:
    if isinstance(result, dict):
        parts = [f"{key}={value}" for key, value in sorted(result.items())]
        return ", ".join(parts)
    return str(result)


def get_available_months(aggregator, year: int) -> list[int]:
    return [month for month in range(1, 13) if aggregator.monthly_file_exists(year, month)]


def _get_market_missing_trade_days(aggregator: MarketAggregator, year: int, month: int) -> list[date]:
    expected_days = set(get_expected_trade_days(year, month))
    available_days = _parse_daily_file_dates(aggregator._load_daily_files(year, month))
    return sorted(expected_days - available_days)


def _get_status_summary(aggregator, year: int, month: int) -> dict[str, Any]:
    announcement_types = list(ANNOUNCEMENT_TYPES) if aggregator.DATA_TYPE == "announcement" else None
    return summarize_month_status(
        aggregator.data_path,
        aggregator.DATA_TYPE,
        year,
        month,
        announcement_types=announcement_types,
    )


def _print_status_issues(summary: dict[str, Any]) -> None:
    missing_dates = summary["missing_dates"]
    error_dates = summary["error_dates"]
    if missing_dates:
        print(f"      跳过：缺少抓取状态 {len(missing_dates)} 天，示例: {_format_dates(missing_dates)}")
    if error_dates:
        print(f"      跳过：存在错误状态 {len(error_dates)} 天，示例: {_format_dates(error_dates)}")

    missing_types = summary["missing_types"]
    error_types = summary["error_types"]
    if missing_types:
        example_date, example_types = next(iter(missing_types.items()))
        print(f"      跳过：公告子类型状态不完整，示例 {example_date} -> {', '.join(example_types)}")
    if error_types:
        example_date, example_types = next(iter(error_types.items()))
        print(f"      跳过：公告子类型抓取失败，示例 {example_date} -> {', '.join(example_types)}")


def _iter_status_paths(data_path: Path, data_type: str) -> list[Path]:
    status_root = get_fetch_status_root(data_path) / data_type
    if not status_root.exists():
        return []
    return sorted(status_root.glob("*.json"))


def _delete_paths(paths: list[Path]) -> int:
    deleted = 0
    for file_path in paths:
        if file_path.exists():
            file_path.unlink()
            deleted += 1
    return deleted


def _delete_status_files(
    data_path: Path,
    data_type: str,
    year: int,
    month: int | None = None,
) -> int:
    deleted = 0
    for file_path in _iter_status_paths(data_path, data_type):
        current_date = _extract_date_from_status_path(file_path)
        if current_date is None or current_date.year != year:
            continue
        if month is not None and current_date.month != month:
            continue
        file_path.unlink()
        deleted += 1
    return deleted


def _collect_candidate_years(aggregator) -> list[int]:
    years: set[int] = set()

    if aggregator.DATA_TYPE == "announcement":
        for ann_type in ANNOUNCEMENT_TYPES:
            paths = aggregator.type_paths[ann_type]
            for file_path in paths["daily"].glob("*.parquet"):
                current_date = _extract_date_from_status_path(file_path)
                if current_date is not None:
                    years.add(current_date.year)
            for file_path in paths["monthly"].glob("*.parquet"):
                try:
                    years.add(datetime.strptime(file_path.stem, "%Y-%m").year)
                except ValueError:
                    continue
            for file_path in paths["yearly"].glob("*.parquet"):
                try:
                    years.add(int(file_path.stem))
                except ValueError:
                    continue
    else:
        for file_path in aggregator.daily_path.glob("*.parquet"):
            current_date = _extract_date_from_status_path(file_path)
            if current_date is not None:
                years.add(current_date.year)
        for file_path in aggregator.monthly_path.glob("*.parquet"):
            try:
                years.add(datetime.strptime(file_path.stem, "%Y-%m").year)
            except ValueError:
                continue
        for file_path in aggregator.yearly_path.glob("*.parquet"):
            try:
                years.add(int(file_path.stem))
            except ValueError:
                continue

    for file_path in _iter_status_paths(aggregator.data_path, aggregator.DATA_TYPE):
        current_date = _extract_date_from_status_path(file_path)
        if current_date is not None:
            years.add(current_date.year)

    return sorted(years)


def _collect_daily_months(aggregator, year: int) -> set[int]:
    if aggregator.DATA_TYPE == "announcement":
        months: set[int] = set()
        for ann_type in ANNOUNCEMENT_TYPES:
            months.update(
                current_date.month
                for current_date in _parse_daily_file_dates(
                    sorted(aggregator.type_paths[ann_type]["daily"].glob(f"{year}-*.parquet"))
                )
            )
        return months

    return {
        current_date.month
        for current_date in _parse_daily_file_dates(sorted(aggregator.daily_path.glob(f"{year}-*.parquet")))
    }


def _collect_status_months(aggregator, year: int) -> set[int]:
    months: set[int] = set()
    for file_path in _iter_status_paths(aggregator.data_path, aggregator.DATA_TYPE):
        current_date = _extract_date_from_status_path(file_path)
        if current_date is not None and current_date.year == year:
            months.add(current_date.month)
    return months


def _collect_month_candidates(aggregator, year: int) -> list[int]:
    months = set(get_available_months(aggregator, year))
    months.update(_collect_daily_months(aggregator, year))
    months.update(_collect_status_months(aggregator, year))
    return sorted(months)


def _delete_month_daily_fragments(aggregator, year: int, month: int) -> dict[str, int]:
    if aggregator.DATA_TYPE == "announcement":
        daily_deleted = 0
        for ann_type in ANNOUNCEMENT_TYPES:
            pattern = f"{year}-{month:02d}-*.parquet"
            files = list(aggregator.type_paths[ann_type]["daily"].glob(pattern))
            daily_deleted += _delete_paths(files)
        status_deleted = _delete_status_files(aggregator.data_path, aggregator.DATA_TYPE, year, month)
        return {"daily": daily_deleted, "status": status_deleted}

    daily_deleted = aggregator._delete_daily_files(year, month)
    status_deleted = _delete_status_files(aggregator.data_path, aggregator.DATA_TYPE, year, month)
    return {"daily": daily_deleted, "status": status_deleted}


def _delete_year_fragments(aggregator, year: int) -> dict[str, int]:
    if aggregator.DATA_TYPE == "announcement":
        monthly_deleted = 0
        daily_deleted = 0
        for ann_type in ANNOUNCEMENT_TYPES:
            monthly_deleted += _delete_paths(list(aggregator.type_paths[ann_type]["monthly"].glob(f"{year}-*.parquet")))
            daily_deleted += _delete_paths(list(aggregator.type_paths[ann_type]["daily"].glob(f"{year}-*.parquet")))
        status_deleted = _delete_status_files(aggregator.data_path, aggregator.DATA_TYPE, year)
        return {"monthly": monthly_deleted, "daily": daily_deleted, "status": status_deleted}

    monthly_deleted = aggregator._delete_monthly_files(year)
    daily_deleted = _delete_paths(list(aggregator.daily_path.glob(f"{year}-*.parquet")))
    status_deleted = _delete_status_files(aggregator.data_path, aggregator.DATA_TYPE, year)
    return {"monthly": monthly_deleted, "daily": daily_deleted, "status": status_deleted}


def _cleanup_month_if_covered(aggregator, year: int, month: int) -> bool:
    if not aggregator.monthly_file_exists(year, month):
        return False

    deleted = _delete_month_daily_fragments(aggregator, year, month)
    if deleted["daily"] or deleted["status"]:
        print(
            f"      清理 {year}-{month:02d} 已被月文件覆盖的碎片:"
            f" daily={deleted['daily']}, status={deleted['status']}"
        )
    return True


def _cleanup_year_if_covered(aggregator, year: int) -> bool:
    if not aggregator.yearly_file_exists(year):
        return False

    deleted = _delete_year_fragments(aggregator, year)
    if deleted["monthly"] or deleted["daily"] or deleted["status"]:
        print(
            f"      清理 {year} 已被年文件覆盖的碎片:"
            f" monthly={deleted['monthly']}, daily={deleted['daily']}, status={deleted['status']}"
        )
    return True


def aggregate_yearly(
    aggregator,
    year: int,
    force: bool = False,
    reference_date: date | datetime | None = None,
) -> bool:
    """Aggregate one year of monthly data when all 12 months are ready."""
    del force
    normalized_reference = _normalize_reference_date(reference_date)

    if not _is_year_closed(year, normalized_reference):
        print(f"      跳过 {year} 年（年份尚未结束）")
        return False

    if aggregator.yearly_file_exists(year):
        print(f"      跳过 {year} 年（年度文件已存在）")
        return False

    available_months = get_available_months(aggregator, year)
    if len(available_months) < 12:
        print(f"      跳过 {year} 年（月度文件不完整 {len(available_months)}/12）")
        return False

    result = aggregator.aggregate_monthly_to_yearly(year)
    if result:
        print(f"      合并 {year} 年: {_describe_result(result)}")
        return True
    return False


def aggregate_monthly(
    aggregator,
    year: int,
    month: int,
    force: bool = False,
    reference_date: date | datetime | None = None,
) -> bool:
    """Aggregate one sealed month of daily data."""
    normalized_reference = _normalize_reference_date(reference_date)
    month_label = f"{year}-{month:02d}"

    if aggregator.monthly_file_exists(year, month):
        print(f"      跳过 {month_label}（月度文件已存在）")
        return False

    if not force and not _is_month_closed(year, month, normalized_reference):
        print(f"      跳过 {month_label}（月份尚未封口）")
        return False

    if aggregator.DATA_TYPE == "market":
        expected_trade_days = get_expected_trade_days(year, month)
        if not expected_trade_days:
            print(f"      跳过 {month_label}（未获取到交易日历）")
            return False
        missing_trade_days = _get_market_missing_trade_days(aggregator, year, month)
        if missing_trade_days:
            print(
                f"      跳过 {month_label}（缺少 {len(missing_trade_days)} 个交易日，示例: "
                f"{_format_dates(missing_trade_days)}）"
            )
            return False
    elif not force:
        status_summary = _get_status_summary(aggregator, year, month)
        if not status_summary["complete"]:
            print(f"      跳过 {month_label}（抓取状态未封口）")
            _print_status_issues(status_summary)
            return False

    result = aggregator.aggregate_daily_to_monthly(year, month)
    if result:
        print(f"      合并 {month_label}: {_describe_result(result)}")
        return True
    return False


def _scan_and_aggregate_year(
    aggregator,
    year: int,
    reference_date: date,
) -> None:
    if _cleanup_year_if_covered(aggregator, year):
        return

    months = _collect_month_candidates(aggregator, year)
    for current_month in months:
        if _cleanup_month_if_covered(aggregator, year, current_month):
            continue
        aggregate_monthly(
            aggregator,
            year,
            current_month,
            force=False,
            reference_date=reference_date,
        )

    if aggregate_yearly(
        aggregator,
        year,
        force=False,
        reference_date=reference_date,
    ):
        _cleanup_year_if_covered(aggregator, year)


def aggregate_type(
    data_type: str,
    year: int | None = None,
    month: int | None = None,
    force: bool = False,
    reference_date: date | datetime | None = None,
    data_path: str = "./data",
) -> None:
    """Aggregate one data type."""
    print(f"  合并 {data_type} 数据...")
    normalized_reference = _normalize_reference_date(reference_date)
    aggregator = AGGREGATORS[data_type](data_path=data_path)

    if year and month:
        _cleanup_year_if_covered(aggregator, year)
        if not aggregator.yearly_file_exists(year):
            _cleanup_month_if_covered(aggregator, year, month)
            aggregate_monthly(
                aggregator,
                year,
                month,
                force=force,
                reference_date=normalized_reference,
            )
        return

    if year:
        _scan_and_aggregate_year(
            aggregator,
            year,
            reference_date=normalized_reference,
        )
        return

    for target_year in _collect_candidate_years(aggregator):
        _scan_and_aggregate_year(
            aggregator,
            target_year,
            reference_date=normalized_reference,
        )


def aggregate(
    data_type: str = "all",
    year: int | None = None,
    month: int | None = None,
    reference_date: date | datetime | None = None,
    data_path: str = "./data",
) -> None:
    """Aggregate one or more data types."""
    normalized_reference = _normalize_reference_date(reference_date)
    print(f"开始合并数据，参考日期 {normalized_reference.isoformat()} ...")

    types_to_process = DATA_TYPES if data_type == "all" else [data_type]
    for current_type in types_to_process:
        aggregate_type(
            current_type,
            year=year,
            month=month,
            force=bool(year and month),
            reference_date=normalized_reference,
            data_path=data_path,
        )

    print("合并完成!")


def main() -> None:
    parser = argparse.ArgumentParser(description="数据聚合")
    parser.add_argument(
        "--type",
        type=str,
        default="all",
        choices=["market", "finance", "announcement", "all"],
        help="数据类型，默认为 all",
    )
    parser.add_argument("--year", type=int, help="指定年份")
    parser.add_argument("--month", type=int, help="指定月份，需要和 --year 搭配使用")
    parser.add_argument(
        "--reference-date",
        type=str,
        help="聚合判定的参考日期 (YYYY-MM-DD)，默认为今天",
    )
    args = parser.parse_args()

    if args.month and not args.year:
        parser.error("--month 需要和 --year 一起使用")

    if args.reference_date:
        reference_date = datetime.strptime(args.reference_date, "%Y-%m-%d").date()
    else:
        reference_date = date.today()

    aggregate(
        args.type,
        args.year,
        args.month,
        reference_date=reference_date,
    )


if __name__ == "__main__":
    main()
