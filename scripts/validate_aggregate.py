"""Validation script for aggregation logic."""

from __future__ import annotations

import argparse
from calendar import monthrange
from datetime import date, datetime
import sys
import tempfile
import time
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))

from data_service import AnnouncementAggregator, FinanceAggregator, MarketAggregator, TushareMarketFetcher
from data_service.fetch_status import (
    delete_fetch_status,
    get_fetch_status_path,
    iter_month_dates,
    write_fetch_status,
)
from data_service.fetcher.announcement_fetcher import ANNOUNCEMENT_TYPES
from scripts.aggregate import aggregate_monthly, aggregate_yearly
from scripts.daily_fetch import fetch_daily


DATA_TYPES = ["market", "finance", "announcement"]


def _normalize_reference_date(reference_date: date | datetime | None) -> date:
    if reference_date is None:
        return date.today()
    if isinstance(reference_date, datetime):
        return reference_date.date()
    return reference_date


def _resolve_closed_month(
    reference_date: date,
    year: int | None,
    month: int | None,
) -> tuple[int, int]:
    if year and month:
        return year, month
    if reference_date.month == 1:
        return reference_date.year - 1, 12
    return reference_date.year, reference_date.month - 1


def _selected_types(data_type: str) -> list[str]:
    return DATA_TYPES if data_type == "all" else [data_type]


def _assert(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def _sleep_for_api() -> None:
    time.sleep(0.12)


def _month_days(year: int, month: int) -> list[date]:
    return iter_month_dates(year, month)


def _month_file(path_root: Path, year: int, month: int) -> Path:
    return path_root / f"{year}-{month:02d}.parquet"


def _year_file(path_root: Path, year: int) -> Path:
    return path_root / f"{year}.parquet"


def _get_trade_dates(year: int, month: int) -> list[date]:
    fetcher = TushareMarketFetcher()
    trade_calendar = fetcher.fetch_trade_calendar(
        datetime(year, month, 1),
        datetime(year, month, monthrange(year, month)[1]),
    )
    if trade_calendar is None or trade_calendar.empty:
        return []
    trade_calendar = trade_calendar.copy()
    trade_calendar["is_open"] = trade_calendar["is_open"].astype(str)
    open_dates = trade_calendar.loc[trade_calendar["is_open"] == "1", "cal_date"].tolist()
    return sorted(
        {
            datetime.strptime(str(cal_date), "%Y%m%d").date()
            for cal_date in open_dates
        }
    )


def _validate_target_month_not_aggregated(data_path: Path, data_type: str, year: int, month: int) -> None:
    if data_type == "market":
        aggregator = MarketAggregator(data_path=str(data_path))
        _assert(
            not aggregator.monthly_file_exists(year, month),
            f"market {year}-{month:02d} 已经合并，拒绝动主目录",
        )
    elif data_type == "finance":
        aggregator = FinanceAggregator(data_path=str(data_path))
        _assert(
            not aggregator.monthly_file_exists(year, month),
            f"finance {year}-{month:02d} 已经合并，拒绝动主目录",
        )
    else:
        aggregator = AnnouncementAggregator(data_path=str(data_path))
        _assert(
            not aggregator.monthly_file_exists(year, month),
            f"announcement {year}-{month:02d} 已经合并，拒绝动主目录",
        )


def _delete_market_partition(data_path: Path, year: int, month: int) -> None:
    for current_date in _get_trade_dates(year, month):
        daily_path = data_path / "market" / "daily" / f"{current_date:%Y-%m-%d}.parquet"
        if daily_path.exists():
            daily_path.unlink()
        delete_fetch_status(data_path, "market", current_date)


def _delete_finance_partition(data_path: Path, year: int, month: int) -> None:
    for current_date in _month_days(year, month):
        daily_path = data_path / "finance" / "daily" / f"{current_date:%Y-%m-%d}.parquet"
        if daily_path.exists():
            daily_path.unlink()
        delete_fetch_status(data_path, "finance", current_date)


def _delete_announcement_partition(data_path: Path, year: int, month: int) -> None:
    for current_date in _month_days(year, month):
        delete_fetch_status(data_path, "announcement", current_date)
        for ann_type in ANNOUNCEMENT_TYPES:
            daily_path = data_path / "announcement" / ann_type / "daily" / f"{current_date:%Y-%m-%d}.parquet"
            if daily_path.exists():
                daily_path.unlink()


def _fetch_month(data_path: Path, data_type: str, year: int, month: int) -> None:
    if data_type == "market":
        for current_date in _get_trade_dates(year, month):
            fetch_daily(current_date, "market", data_path=str(data_path))
            _sleep_for_api()
        return

    for current_date in _month_days(year, month):
        fetch_daily(current_date, data_type, data_path=str(data_path))
        _sleep_for_api()


def _fetch_open_month_sample(data_path: Path, data_type: str, reference_date: date) -> None:
    sample_days = min(reference_date.day - 1, 3)
    if sample_days <= 0:
        return

    for day in range(1, sample_days + 1):
        current_date = date(reference_date.year, reference_date.month, day)
        fetch_daily(current_date, data_type, data_path=str(data_path))
        _sleep_for_api()


def _assert_month_deleted(data_path: Path, data_type: str, year: int, month: int) -> None:
    if data_type == "market":
        remaining = list((data_path / "market" / "daily").glob(f"{year}-{month:02d}-*.parquet"))
    elif data_type == "finance":
        remaining = list((data_path / "finance" / "daily").glob(f"{year}-{month:02d}-*.parquet"))
    else:
        remaining = []
        for ann_type in ANNOUNCEMENT_TYPES:
            remaining.extend(
                (data_path / "announcement" / ann_type / "daily").glob(f"{year}-{month:02d}-*.parquet")
            )
    _assert(not remaining, f"{data_type} {year}-{month:02d} 仍有未清理的日文件")


def _assert_open_month_not_aggregated(data_path: Path, data_type: str, reference_date: date) -> None:
    year = reference_date.year
    month = reference_date.month
    if data_type == "announcement":
        aggregator = AnnouncementAggregator(data_path=str(data_path))
    elif data_type == "finance":
        aggregator = FinanceAggregator(data_path=str(data_path))
    else:
        aggregator = MarketAggregator(data_path=str(data_path))
    _assert(
        not aggregator.monthly_file_exists(year, month),
        f"{data_type} 当前开放月份 {year}-{month:02d} 不应被自动合并",
    )


def _assert_finance_uniqueness(path: Path) -> None:
    df = pd.read_parquet(path)
    if df.empty:
        return
    duplicates = len(df) - len(df.drop_duplicates(subset=["stock_code", "end_date"]))
    _assert(duplicates == 0, f"finance 月文件去重失败: {path}")


def _assert_announcement_uniqueness(monthly_root: Path, year: int, month: int) -> None:
    forecast_path = _month_file(monthly_root / "forecast" / "monthly", year, month)
    express_path = _month_file(monthly_root / "express" / "monthly", year, month)
    repurchase_path = _month_file(monthly_root / "repurchase" / "monthly", year, month)
    holdertrade_path = _month_file(monthly_root / "holdertrade" / "monthly", year, month)

    for path in [forecast_path, express_path, repurchase_path, holdertrade_path]:
        _assert(path.exists(), f"公告月文件缺失: {path}")

    forecast_df = pd.read_parquet(forecast_path)
    express_df = pd.read_parquet(express_path)
    repurchase_df = pd.read_parquet(repurchase_path)
    holdertrade_df = pd.read_parquet(holdertrade_path)

    forecast_dup = 0
    if not forecast_df.empty:
        forecast_dup = len(forecast_df) - len(forecast_df.drop_duplicates(subset=["stock_code", "end_date"]))
    express_dup = 0
    if not express_df.empty:
        express_dup = len(express_df) - len(express_df.drop_duplicates(subset=["stock_code", "end_date"]))
    repurchase_exact_dup = len(repurchase_df) - len(repurchase_df.drop_duplicates())
    holdertrade_exact_dup = len(holdertrade_df) - len(holdertrade_df.drop_duplicates())

    _assert(forecast_dup == 0, "announcement forecast 去重失败")
    _assert(express_dup == 0, "announcement express 去重失败")
    _assert(repurchase_exact_dup == 0, "announcement repurchase 仍存在完全重复行")
    _assert(holdertrade_exact_dup == 0, "announcement holdertrade 仍存在完全重复行")


def _run_real_market_validation(data_path: Path, year: int, month: int, reference_date: date) -> None:
    print("实盘联调: market")
    _validate_target_month_not_aggregated(data_path, "market", year, month)
    _delete_market_partition(data_path, year, month)
    _fetch_month(data_path, "market", year, month)
    _fetch_open_month_sample(data_path, "market", reference_date)

    trade_dates = _get_trade_dates(year, month)
    _assert(trade_dates, "market 联调未取到交易日历")
    missing_date = trade_dates[0]
    missing_daily_path = data_path / "market" / "daily" / f"{missing_date:%Y-%m-%d}.parquet"
    _assert(missing_daily_path.exists(), f"缺少联调用市场日文件: {missing_daily_path}")
    missing_daily_path.unlink()

    aggregator = MarketAggregator(data_path=str(data_path))
    merged = aggregate_monthly(aggregator, year, month, reference_date=reference_date)
    _assert(not merged, "market 缺交易日时不应合并成功")
    _assert(not aggregator.monthly_file_exists(year, month), "market 缺交易日时不应产出月文件")

    fetch_daily(missing_date, "market", data_path=str(data_path))
    merged = aggregate_monthly(aggregator, year, month, reference_date=reference_date)
    _assert(merged, "market 完整月应能合并")
    _assert(aggregator.monthly_file_exists(year, month), "market 月文件缺失")
    _assert_month_deleted(data_path, "market", year, month)
    _assert_open_month_not_aggregated(data_path, "market", reference_date)


def _run_real_finance_validation(data_path: Path, year: int, month: int, reference_date: date) -> None:
    print("实盘联调: finance")
    _validate_target_month_not_aggregated(data_path, "finance", year, month)
    _delete_finance_partition(data_path, year, month)
    _fetch_month(data_path, "finance", year, month)
    _fetch_open_month_sample(data_path, "finance", reference_date)

    missing_date = _month_days(year, month)[0]
    missing_status = get_fetch_status_path(data_path, "finance", missing_date)
    _assert(missing_status.exists(), f"finance 联调缺少状态文件: {missing_status}")
    missing_status.unlink()

    aggregator = FinanceAggregator(data_path=str(data_path))
    merged = aggregate_monthly(aggregator, year, month, reference_date=reference_date)
    _assert(not merged, "finance 缺状态文件时不应合并成功")
    _assert(not aggregator.monthly_file_exists(year, month), "finance 缺状态文件时不应产出月文件")

    fetch_daily(missing_date, "finance", data_path=str(data_path))
    merged = aggregate_monthly(aggregator, year, month, reference_date=reference_date)
    _assert(merged, "finance 完整封口月份应能合并")
    monthly_path = data_path / "finance" / "monthly" / f"{year}-{month:02d}.parquet"
    _assert(monthly_path.exists(), "finance 月文件缺失")
    _assert_finance_uniqueness(monthly_path)
    _assert_month_deleted(data_path, "finance", year, month)
    _assert_open_month_not_aggregated(data_path, "finance", reference_date)


def _run_real_announcement_validation(data_path: Path, year: int, month: int, reference_date: date) -> None:
    print("实盘联调: announcement")
    _validate_target_month_not_aggregated(data_path, "announcement", year, month)
    _delete_announcement_partition(data_path, year, month)
    _fetch_month(data_path, "announcement", year, month)
    _fetch_open_month_sample(data_path, "announcement", reference_date)

    missing_date = _month_days(year, month)[0]
    missing_status = get_fetch_status_path(data_path, "announcement", missing_date)
    _assert(missing_status.exists(), f"announcement 联调缺少状态文件: {missing_status}")
    missing_status.unlink()

    aggregator = AnnouncementAggregator(data_path=str(data_path))
    merged = aggregate_monthly(aggregator, year, month, reference_date=reference_date)
    _assert(not merged, "announcement 缺状态文件时不应合并成功")
    _assert(not aggregator.monthly_file_exists(year, month), "announcement 缺状态文件时不应产出月文件")

    fetch_daily(missing_date, "announcement", data_path=str(data_path))
    merged = aggregate_monthly(aggregator, year, month, reference_date=reference_date)
    _assert(merged, "announcement 完整封口月份应能合并")
    _assert(aggregator.monthly_file_exists(year, month), "announcement 月文件缺失")
    _assert_announcement_uniqueness(data_path / "announcement", year, month)
    _assert_month_deleted(data_path, "announcement", year, month)
    _assert_open_month_not_aggregated(data_path, "announcement", reference_date)


def _write_finance_status_month(data_path: Path, year: int, month: int, status: str = "empty") -> None:
    for current_date in _month_days(year, month):
        payload = {
            "data_type": "finance",
            "target_date": current_date.isoformat(),
            "fetched_at": datetime.now().isoformat(timespec="seconds"),
            "status": status,
            "row_count": 0,
        }
        write_fetch_status(data_path, "finance", current_date, payload)


def _write_announcement_status_month(data_path: Path, year: int, month: int, status: str = "empty") -> None:
    for current_date in _month_days(year, month):
        payload = {
            "data_type": "announcement",
            "target_date": current_date.isoformat(),
            "fetched_at": datetime.now().isoformat(timespec="seconds"),
            "status": status,
            "row_count": 0,
            "types": {
                ann_type: {
                    "status": status,
                    "row_count": 0,
                }
                for ann_type in ANNOUNCEMENT_TYPES
            },
        }
        write_fetch_status(data_path, "announcement", current_date, payload)


def _build_finance_df(ann_date: str, end_date: str, update_flag: int, value: float) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "stock_code": "000001.SZ",
                "ann_date": pd.Timestamp(ann_date),
                "end_date": pd.Timestamp(end_date),
                "eps": value,
                "update_flag": update_flag,
            }
        ]
    )


def _build_forecast_df(ann_date: str, end_date: str, update_flag: int, summary: str) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "stock_code": "000001.SZ",
                "ann_date": pd.Timestamp(ann_date),
                "end_date": pd.Timestamp(end_date),
                "summary": summary,
                "update_flag": update_flag,
            }
        ]
    )


def _build_express_df(ann_date: str, end_date: str, update_flag: int, revenue: float) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "stock_code": "000001.SZ",
                "ann_date": pd.Timestamp(ann_date),
                "end_date": pd.Timestamp(end_date),
                "revenue": revenue,
                "update_flag": update_flag,
            }
        ]
    )


def _build_repurchase_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "stock_code": "000001.SZ",
                "ann_date": pd.Timestamp("2024-02-05"),
                "end_date": pd.NaT,
                "proc": "预案",
                "amount": 10.0,
            },
            {
                "stock_code": "000001.SZ",
                "ann_date": pd.Timestamp("2024-02-05"),
                "end_date": pd.NaT,
                "proc": "预案",
                "amount": 10.0,
            },
        ]
    )


def _build_holdertrade_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "stock_code": "000001.SZ",
                "ann_date": pd.Timestamp("2024-02-05"),
                "holder_name": "Holder A",
                "holder_type": "G",
                "in_de": "IN",
                "change_vol": 100.0,
            },
            {
                "stock_code": "000001.SZ",
                "ann_date": pd.Timestamp("2024-02-05"),
                "holder_name": "Holder A",
                "holder_type": "G",
                "in_de": "IN",
                "change_vol": 100.0,
            },
        ]
    )


def _run_synthetic_finance_checks(temp_root: Path) -> None:
    print("临时目录断言: finance")
    _write_finance_status_month(temp_root, 2024, 2, status="empty")
    aggregator = FinanceAggregator(data_path=str(temp_root))
    merged = aggregate_monthly(aggregator, 2024, 2, reference_date=date(2024, 3, 5))
    _assert(merged, "finance 零数据月份应生成空月文件")
    monthly_path = temp_root / "finance" / "monthly" / "2024-02.parquet"
    _assert(monthly_path.exists(), "finance 空月文件缺失")
    _assert(pd.read_parquet(monthly_path).empty, "finance 空月文件应为空")

    monthly_root = temp_root / "finance" / "monthly"
    for month in range(1, 12):
        monthly_root.mkdir(parents=True, exist_ok=True)
        if month == 1:
            df = _build_finance_df("2024-01-20", "2023-12-31", 0, 1.0)
        elif month == 2:
            df = pd.concat(
                [
                    _build_finance_df("2024-02-05", "2023-12-31", 0, 2.0),
                    _build_finance_df("2024-02-05", "2023-12-31", 1, 3.0),
                ],
                ignore_index=True,
            )
        else:
            df = pd.DataFrame()
        df.to_parquet(monthly_root / f"2024-{month:02d}.parquet", index=False)

    yearly_path = temp_root / "finance" / "yearly" / "2024.parquet"
    merged = aggregate_yearly(aggregator, 2024, reference_date=date(2025, 1, 15))
    _assert(not merged, "finance 缺少月份时不应年合并")
    _assert(not yearly_path.exists(), "finance 年文件不应在缺少月份时生成")

    pd.DataFrame().to_parquet(monthly_root / "2024-12.parquet", index=False)
    merged = aggregate_yearly(aggregator, 2024, reference_date=date(2025, 1, 15))
    _assert(merged, "finance 满 12 个月后应能年合并")
    _assert(yearly_path.exists(), "finance 年文件缺失")
    yearly_df = pd.read_parquet(yearly_path)
    yearly_dup = len(yearly_df) - len(yearly_df.drop_duplicates(subset=["stock_code", "end_date"]))
    _assert(yearly_dup == 0, "finance 年文件去重失败")
    _assert(float(yearly_df.loc[0, "eps"]) == 3.0, "finance 年文件未保留最新 update_flag=1 记录")


def _run_synthetic_announcement_checks(temp_root: Path) -> None:
    print("临时目录断言: announcement")
    _write_announcement_status_month(temp_root, 2024, 2, status="empty")
    aggregator = AnnouncementAggregator(data_path=str(temp_root))
    merged = aggregate_monthly(aggregator, 2024, 2, reference_date=date(2024, 3, 5))
    _assert(merged, "announcement 零数据月份应生成空月文件")
    for ann_type in ANNOUNCEMENT_TYPES:
        monthly_path = temp_root / "announcement" / ann_type / "monthly" / "2024-02.parquet"
        _assert(monthly_path.exists(), f"announcement 空月文件缺失: {monthly_path}")
        _assert(pd.read_parquet(monthly_path).empty, f"announcement 空月文件应为空: {monthly_path}")

    _write_announcement_status_month(temp_root, 2024, 3, status="empty")
    payload = {
        "data_type": "announcement",
        "target_date": date(2024, 3, 1).isoformat(),
        "fetched_at": datetime.now().isoformat(timespec="seconds"),
        "status": "empty",
        "row_count": 0,
        "types": {
            ann_type: {"status": "empty", "row_count": 0}
            for ann_type in ANNOUNCEMENT_TYPES
            if ann_type != "forecast"
        },
    }
    write_fetch_status(temp_root, "announcement", date(2024, 3, 1), payload)
    merged = aggregate_monthly(aggregator, 2024, 3, reference_date=date(2024, 4, 5))
    _assert(not merged, "announcement 缺少子类型状态时不应合并")

    write_fetch_status(
        temp_root,
        "announcement",
        date(2024, 3, 1),
        {
            "data_type": "announcement",
            "target_date": date(2024, 3, 1).isoformat(),
            "fetched_at": datetime.now().isoformat(timespec="seconds"),
            "status": "empty",
            "row_count": 0,
            "types": {
                ann_type: {"status": "empty", "row_count": 0}
                for ann_type in ANNOUNCEMENT_TYPES
            },
        },
    )

    forecast_daily = temp_root / "announcement" / "forecast" / "daily" / "2024-03-01.parquet"
    express_daily = temp_root / "announcement" / "express" / "daily" / "2024-03-01.parquet"
    repurchase_daily = temp_root / "announcement" / "repurchase" / "daily" / "2024-03-01.parquet"
    holdertrade_daily = temp_root / "announcement" / "holdertrade" / "daily" / "2024-03-01.parquet"

    forecast_daily.parent.mkdir(parents=True, exist_ok=True)
    express_daily.parent.mkdir(parents=True, exist_ok=True)
    repurchase_daily.parent.mkdir(parents=True, exist_ok=True)
    holdertrade_daily.parent.mkdir(parents=True, exist_ok=True)

    pd.concat(
        [
            _build_forecast_df("2024-03-01", "2024-12-31", 0, "old"),
            _build_forecast_df("2024-03-01", "2024-12-31", 1, "new"),
        ],
        ignore_index=True,
    ).to_parquet(forecast_daily, index=False)
    pd.concat(
        [
            _build_express_df("2024-03-01", "2024-12-31", 0, 10.0),
            _build_express_df("2024-03-02", "2024-12-31", 0, 20.0),
        ],
        ignore_index=True,
    ).to_parquet(express_daily, index=False)
    _build_repurchase_df().to_parquet(repurchase_daily, index=False)
    _build_holdertrade_df().to_parquet(holdertrade_daily, index=False)

    merged = aggregate_monthly(aggregator, 2024, 3, reference_date=date(2024, 4, 5))
    _assert(merged, "announcement 合成样例月份应能合并")
    _assert_announcement_uniqueness(temp_root / "announcement", 2024, 3)


def _run_synthetic_checks(selected_types: list[str]) -> None:
    with tempfile.TemporaryDirectory(prefix="validate_aggregate_") as temp_dir:
        temp_root = Path(temp_dir)
        if "finance" in selected_types:
            _run_synthetic_finance_checks(temp_root)
        if "announcement" in selected_types:
            _run_synthetic_announcement_checks(temp_root)


def run_validation(
    data_type: str = "all",
    year: int | None = None,
    month: int | None = None,
    reference_date: date | datetime | None = None,
    data_path: str = "./data",
    confirm_delete: bool = False,
) -> None:
    normalized_reference = _normalize_reference_date(reference_date)
    closed_year, closed_month = _resolve_closed_month(normalized_reference, year, month)
    selected_types = _selected_types(data_type)
    data_root = Path(data_path)

    if not confirm_delete:
        print("该脚本会删除主 data 目录下未合并分区的日文件和抓取状态后再重拉。")
        print("如确认执行，请追加 --yes。")
        return

    print(f"参考日期: {normalized_reference.isoformat()}")
    print(f"联调目标月份: {closed_year}-{closed_month:02d}")

    if "market" in selected_types:
        _run_real_market_validation(data_root, closed_year, closed_month, normalized_reference)
    if "finance" in selected_types:
        _run_real_finance_validation(data_root, closed_year, closed_month, normalized_reference)
    if "announcement" in selected_types:
        _run_real_announcement_validation(data_root, closed_year, closed_month, normalized_reference)

    _run_synthetic_checks(selected_types)
    print("验证完成!")


def main() -> None:
    parser = argparse.ArgumentParser(description="数据聚合联调验证")
    parser.add_argument(
        "--type",
        type=str,
        default="all",
        choices=["market", "finance", "announcement", "all"],
        help="数据类型，默认为 all",
    )
    parser.add_argument("--year", type=int, help="指定闭口月份的年份")
    parser.add_argument("--month", type=int, help="指定闭口月份的月份，需要和 --year 配合")
    parser.add_argument(
        "--reference-date",
        type=str,
        help="验证时使用的参考日期 (YYYY-MM-DD)，默认为今天",
    )
    parser.add_argument("--yes", action="store_true", help="确认删除未合并分区并执行真实联调")
    args = parser.parse_args()

    if args.month and not args.year:
        parser.error("--month 需要和 --year 一起使用")

    if args.reference_date:
        reference_date = datetime.strptime(args.reference_date, "%Y-%m-%d").date()
    else:
        reference_date = date.today()

    run_validation(
        data_type=args.type,
        year=args.year,
        month=args.month,
        reference_date=reference_date,
        confirm_delete=args.yes,
    )


if __name__ == "__main__":
    main()
