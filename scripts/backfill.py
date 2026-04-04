"""历史数据回填脚本

使用方法:
    python scripts/backfill.py --start 2024-01-01 --end 2024-12-31  # 回填指定时间范围
    python scripts/backfill.py --start 2020-01-01                    # 回填到今天
    python scripts/backfill.py --type market --start 2024-01-01     # 只回填市场数据

存储逻辑：
    - 整年数据（1月1日-12月31日）→ yearly/2024.parquet
    - 整月数据 → monthly/2024-03.parquet
    - 零散日期 → daily/2024-03-15.parquet

增量回填：
    - 自动跳过已存在的年/月/日文件
    - 每完成一个年/月立即保存，避免崩溃丢失
"""
import argparse
from datetime import datetime, timedelta
import sys
from pathlib import Path

# 添加项目根目录到路径
sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
from data_service import (
    TushareMarketFetcher, TushareFinanceFetcher, TushareAnnouncementFetcher
)
from data_service.fetch_status import read_fetch_status, write_fetch_status
from data_service.fetcher.announcement_fetcher import ANNOUNCEMENT_TYPES


def _write_status(data_path: Path, data_type: str, target_date: datetime, row_count: int,
                  extra: dict = None):
    payload = {
        'data_type': data_type,
        'target_date': target_date.date().isoformat(),
        'fetched_at': datetime.now().isoformat(timespec='seconds'),
        'status': 'success' if row_count > 0 else 'empty',
        'row_count': int(row_count),
    }
    if extra:
        payload.update(extra)
    write_fetch_status(data_path, data_type, target_date, payload)


def _write_market_daily_statuses(fetcher, start: datetime, end: datetime, df: pd.DataFrame):
    """为市场散日回补写入状态文件。"""
    trade_dates = get_trade_dates_in_range(fetcher, start, end)
    if not trade_dates:
        return

    grouped = {}
    if df is not None and not df.empty and 'trade_date' in df.columns:
        normalized = df.copy()
        normalized['trade_date'] = pd.to_datetime(normalized['trade_date'])
        grouped = {
            trade_date.date(): len(group_df)
            for trade_date, group_df in normalized.groupby('trade_date')
        }

    for trade_date in trade_dates:
        dt = datetime.strptime(trade_date, '%Y%m%d')
        _write_status(fetcher.data_path, 'market', dt, grouped.get(dt.date(), 0))


def _write_finance_daily_statuses(fetcher, start: datetime, end: datetime, df: pd.DataFrame):
    """为财报散日回补写入状态文件，包括空结果日期。"""
    grouped = {}
    if df is not None and not df.empty and 'ann_date' in df.columns:
        normalized = df.copy()
        normalized['ann_date'] = pd.to_datetime(normalized['ann_date'])
        grouped = {
            ann_date.date(): len(group_df)
            for ann_date, group_df in normalized.groupby('ann_date')
        }

    current = start
    while current <= end:
        _write_status(fetcher.data_path, 'finance', current, grouped.get(current.date(), 0))
        current += timedelta(days=1)


def _write_announcement_daily_statuses(fetcher, ann_type: str, start: datetime, end: datetime, df: pd.DataFrame):
    """为公告散日回补写入状态文件，按子类型合并到同一个日状态里。"""
    grouped = {}
    if df is not None and not df.empty and 'ann_date' in df.columns:
        normalized = df.copy()
        normalized['ann_date'] = pd.to_datetime(normalized['ann_date'])
        grouped = {
            ann_date.date(): len(group_df)
            for ann_date, group_df in normalized.groupby('ann_date')
        }

    current = start
    while current <= end:
        payload = read_fetch_status(fetcher.data_path, 'announcement', current) or {
            'data_type': 'announcement',
            'target_date': current.date().isoformat(),
            'fetched_at': datetime.now().isoformat(timespec='seconds'),
            'status': 'empty',
            'row_count': 0,
            'types': {},
        }

        row_count = grouped.get(current.date(), 0)
        payload.setdefault('types', {})
        payload['types'][ann_type] = {
            'status': 'success' if row_count > 0 else 'empty',
            'row_count': int(row_count),
        }
        payload['row_count'] = sum(item.get('row_count', 0) for item in payload['types'].values())

        type_statuses = [item.get('status') for item in payload['types'].values()]
        if any(status == 'error' for status in type_statuses):
            payload['status'] = 'error'
        elif any(status == 'success' for status in type_statuses):
            payload['status'] = 'success'
        else:
            payload['status'] = 'empty'

        payload['fetched_at'] = datetime.now().isoformat(timespec='seconds')
        write_fetch_status(fetcher.data_path, 'announcement', current, payload)
        current += timedelta(days=1)


def _has_valid_daily_status(data_path: Path, data_type: str, target_date: datetime) -> bool:
    payload = read_fetch_status(data_path, data_type, target_date)
    if not payload:
        return False
    return payload.get('status') in {'success', 'empty'}


def _has_valid_announcement_type_status(data_path: Path, ann_type: str, target_date: datetime) -> bool:
    payload = read_fetch_status(data_path, 'announcement', target_date)
    if not payload:
        return False

    type_payload = payload.get('types', {}).get(ann_type)
    if not isinstance(type_payload, dict):
        return False
    return type_payload.get('status') in {'success', 'empty'}


def get_existing_periods(fetcher) -> dict:
    """
    获取已有的数据文件

    Returns:
        dict: {
            'yearly': set([2024, 2025, ...]),
            'monthly': set(['2024-01', '2024-02', ...]),
            'daily': set(['2024-03-15', ...])
        }
    """
    existing = {
        'yearly': set(),
        'monthly': set(),
        'daily': set()
    }

    # 检查年度文件
    for f in fetcher.yearly_path.glob('*.parquet'):
        if len(f.stem) == 4 and f.stem.isdigit():
            existing['yearly'].add(int(f.stem))

    # 检查月度文件
    for f in fetcher.monthly_path.glob('*.parquet'):
        if len(f.stem) == 7:
            try:
                datetime.strptime(f.stem, '%Y-%m')
                existing['monthly'].add(f.stem)
            except ValueError:
                pass

    # 检查日度文件
    for f in fetcher.daily_path.glob('*.parquet'):
        if len(f.stem) == 10:
            try:
                dt = datetime.strptime(f.stem, '%Y-%m-%d')
                if _has_valid_daily_status(fetcher.data_path, fetcher.DATA_TYPE, dt):
                    existing['daily'].add(f.stem)
            except ValueError:
                pass

    return existing


def get_trade_dates_in_range(fetcher, start: datetime, end: datetime) -> list:
    """获取时间范围内的交易日列表"""
    trade_cal = fetcher.pro.trade_cal(
        start_date=start.strftime('%Y%m%d'),
        end_date=end.strftime('%Y%m%d'),
        is_open='1'
    )
    if trade_cal is None or trade_cal.empty:
        return []
    return trade_cal['cal_date'].tolist()


def backfill_market(start_date: datetime, end_date: datetime):
    """回填市场数据"""
    print(f"  回填市场数据 ({start_date.date()} ~ {end_date.date()})...")

    fetcher = TushareMarketFetcher()
    existing = get_existing_periods(fetcher)

    start_year = start_date.year
    end_year = end_date.year

    # 按年处理
    for year in range(start_year, end_year + 1):
        year_start = datetime(year, 1, 1)
        year_end = datetime(year, 12, 31)
        actual_start = max(start_date, year_start)
        actual_end = min(end_date, year_end)

        # 检查是否是完整年份且已存在
        is_full_year = (actual_start == year_start and actual_end == year_end)
        if is_full_year and year in existing['yearly']:
            print(f"    跳过 {year} 年（已存在）")
            continue

        # 获取该年的交易日
        trade_dates = get_trade_dates_in_range(fetcher, actual_start, actual_end)
        if not trade_dates:
            continue

        # 确定缺失的交易日
        missing_dates = []
        for td in trade_dates:
            date_str = td  # YYYYMMDD 格式
            daily_key = datetime.strptime(date_str, '%Y%m%d').strftime('%Y-%m-%d')

            # 检查是否已被年度/月度文件覆盖
            month_key = daily_key[:7]
            if year in existing['yearly']:
                continue  # 已有年度文件
            if month_key in existing['monthly']:
                continue  # 已有月度文件
            if daily_key in existing['daily']:
                continue  # 已有日度文件

            missing_dates.append(date_str)

        if not missing_dates:
            print(f"    跳过 {year} 年（数据完整）")
            continue

        # 按月分组拉取，每月保存一次
        print(f"    拉取 {year} 年数据...")
        for month in range(1, 13):
            month_start = datetime(year, month, 1)
            if month == 12:
                month_end = datetime(year, 12, 31)
            else:
                month_end = datetime(year, month + 1, 1) - timedelta(days=1)

            # 跳过不在请求范围内的月份
            if month_end < actual_start or month_start > actual_end:
                continue

            actual_month_start = max(actual_start, month_start)
            actual_month_end = min(actual_end, month_end)

            # 检查该月是否已存在
            month_key = f"{year}-{month:02d}"
            is_full_month = (actual_month_start == month_start and actual_month_end == month_end)
            if is_full_month and month_key in existing['monthly']:
                print(f"      跳过 {month_key}（已存在）")
                continue

            # 检查该月是否已被年度文件覆盖
            if year in existing['yearly']:
                print(f"      跳过 {month_key}（年度文件已存在）")
                continue

            # 拉取该月数据
            month_trade_dates = [d for d in missing_dates if d.startswith(f"{year}{month:02d}")]
            if not month_trade_dates:
                continue

            print(f"      拉取 {month_key}...")
            df = fetcher.fetch_history(actual_month_start, actual_month_end)

            if df is not None:
                # 判断保存方式
                if is_full_month:
                    filepath = fetcher.monthly_path / f"{month_key}.parquet"
                    df.to_parquet(filepath, index=False)
                    print(f"      保存: {filepath} ({len(df)} 条)")
                else:
                    # 零散日期，按日保存
                    for trade_date in df['trade_date'].unique():
                        daily_df = df[df['trade_date'] == trade_date]
                        fetcher.save_daily(daily_df, trade_date)
                    _write_market_daily_statuses(fetcher, actual_month_start, actual_month_end, df)
                    print(f"      保存 {len(df)} 条日数据")

    print("  市场数据回填完成!")


def backfill_finance(start_date: datetime, end_date: datetime):
    """回填财报数据"""
    print(f"  回填财报数据 ({start_date.date()} ~ {end_date.date()})...")
    fetcher = TushareFinanceFetcher()
    existing = get_existing_periods(fetcher)

    start_year = start_date.year
    end_year = end_date.year

    total_count = 0
    for year in range(start_year, end_year + 1):
        # 跳过已有年度文件
        if year in existing['yearly']:
            print(f"    跳过 {year} 年（已存在）")
            continue

        year_start = datetime(year, 1, 1)
        year_end = datetime(year, 12, 31)
        actual_start = max(start_date, year_start)
        actual_end = min(end_date, year_end)

        # 按月拉取并保存
        for month in range(1, 13):
            month_start = datetime(year, month, 1)
            if month == 12:
                month_end = datetime(year, 12, 31)
            else:
                month_end = datetime(year, month + 1, 1) - timedelta(days=1)

            if month_end < actual_start or month_start > actual_end:
                continue

            month_key = f"{year}-{month:02d}"
            if month_key in existing['monthly']:
                print(f"    跳过 {month_key}（已存在）")
                continue

            actual_month_start = max(actual_start, month_start)
            actual_month_end = min(actual_end, month_end)

            # 判断是否是完整月份
            is_full_month = (actual_month_start == month_start and actual_month_end == month_end)

            print(f"    拉取 {month_key}...")
            df = fetcher.fetch_history(actual_month_start, actual_month_end)

            if df is not None:
                if is_full_month:
                    # 完整月份，保存到 monthly/ 目录
                    filepath = fetcher.monthly_path / f"{month_key}.parquet"
                    df.to_parquet(filepath, index=False)
                    print(f"    保存: {filepath} ({len(df)} 条)")
                else:
                    # 零散日期，按日保存
                    for ann_date in df['ann_date'].unique():
                        daily_df = df[df['ann_date'] == ann_date]
                        dt = pd.to_datetime(ann_date)
                        fetcher.save_daily(daily_df, dt)
                    _write_finance_daily_statuses(fetcher, actual_month_start, actual_month_end, df)
                    print(f"    保存 {len(df)} 条日数据")
                total_count += len(df)

    print(f"  财报数据回填完成，共 {total_count} 条")


def get_existing_periods_for_type(fetcher, ann_type: str) -> dict:
    """
    获取指定公告类型已有的数据文件

    Args:
        fetcher: 数据拉取器
        ann_type: 公告类型

    Returns:
        dict: {'yearly': set(), 'monthly': set(), 'daily': set()}
    """
    existing = {
        'yearly': set(),
        'monthly': set(),
        'daily': set()
    }

    if ann_type not in fetcher.type_paths:
        return existing

    paths = fetcher.type_paths[ann_type]

    # 检查年度文件
    for f in paths['yearly'].glob('*.parquet'):
        if len(f.stem) == 4 and f.stem.isdigit():
            existing['yearly'].add(int(f.stem))

    # 检查月度文件
    for f in paths['monthly'].glob('*.parquet'):
        if len(f.stem) == 7:
            try:
                datetime.strptime(f.stem, '%Y-%m')
                existing['monthly'].add(f.stem)
            except ValueError:
                pass

    # 检查日度文件
    for f in paths['daily'].glob('*.parquet'):
        if len(f.stem) == 10:
            try:
                dt = datetime.strptime(f.stem, '%Y-%m-%d')
                if _has_valid_announcement_type_status(fetcher.data_path, ann_type, dt):
                    existing['daily'].add(f.stem)
            except ValueError:
                pass

    return existing


def backfill_announcement(start_date: datetime, end_date: datetime):
    """回填公告数据"""
    print(f"  回填公告数据 ({start_date.date()} ~ {end_date.date()})...")
    fetcher = TushareAnnouncementFetcher()

    total_count = 0

    # 按公告类型分别处理
    for ann_type, type_name in ANNOUNCEMENT_TYPES.items():
        print(f"    处理 {type_name} ({ann_type})...")

        existing = get_existing_periods_for_type(fetcher, ann_type)

        start_year = start_date.year
        end_year = end_date.year

        type_count = 0
        for year in range(start_year, end_year + 1):
            if year in existing['yearly']:
                print(f"      跳过 {year} 年（已存在）")
                continue

            year_start = datetime(year, 1, 1)
            year_end = datetime(year, 12, 31)
            actual_start = max(start_date, year_start)
            actual_end = min(end_date, year_end)

            for month in range(1, 13):
                month_start = datetime(year, month, 1)
                if month == 12:
                    month_end = datetime(year, 12, 31)
                else:
                    month_end = datetime(year, month + 1, 1) - timedelta(days=1)

                if month_end < actual_start or month_start > actual_end:
                    continue

                month_key = f"{year}-{month:02d}"
                if month_key in existing['monthly']:
                    continue

                actual_month_start = max(actual_start, month_start)
                actual_month_end = min(actual_end, month_end)

                # 判断是否是完整月份
                is_full_month = (actual_month_start == month_start and actual_month_end == month_end)

                print(f"      拉取 {month_key}...")
                df = fetcher.fetch_history_by_type(ann_type, actual_month_start, actual_month_end)

                if df is not None:
                    if is_full_month:
                        # 完整月份，保存到 monthly/ 目录
                        filepath = fetcher.save_monthly_by_type(df, ann_type, year, month)
                        print(f"      保存: {filepath} ({len(df)} 条)")
                    else:
                        # 零散日期，按日保存
                        for ann_date in df['ann_date'].unique():
                            daily_df = df[df['ann_date'] == ann_date]
                            dt = pd.to_datetime(ann_date)
                            fetcher.save_daily_by_type(daily_df, ann_type, dt)
                        _write_announcement_daily_statuses(
                            fetcher, ann_type, actual_month_start, actual_month_end, df
                        )
                        print(f"      保存 {len(df)} 条日数据")
                    type_count += len(df)

        print(f"    {type_name}完成，共 {type_count} 条")
        total_count += type_count

    print(f"  公告数据回填完成，共 {total_count} 条")


def backfill(data_type: str, start_date: datetime, end_date: datetime):
    """
    回填历史数据

    Args:
        data_type: 数据类型 ('market', 'finance', 'announcement', 'all')
        start_date: 开始日期
        end_date: 结束日期
    """
    print(f"开始回填 {start_date.date()} 到 {end_date.date()} 的{data_type}数据...")

    if data_type in ('market', 'all'):
        backfill_market(start_date, end_date)

    if data_type in ('finance', 'all'):
        backfill_finance(start_date, end_date)

    if data_type in ('announcement', 'all'):
        backfill_announcement(start_date, end_date)

    print("回填完成!")


def main():
    parser = argparse.ArgumentParser(description='历史数据回填')
    parser.add_argument('--start', type=str, required=True, help='开始日期 (YYYY-MM-DD)')
    parser.add_argument('--end', type=str, help='结束日期 (YYYY-MM-DD)，默认为今天')
    parser.add_argument('--type', type=str, default='all',
                        choices=['market', 'finance', 'announcement', 'all'],
                        help='数据类型，默认为all')
    args = parser.parse_args()

    start_date = datetime.strptime(args.start, '%Y-%m-%d')
    if args.end:
        end_date = datetime.strptime(args.end, '%Y-%m-%d')
    else:
        end_date = datetime.now()

    backfill(args.type, start_date, end_date)


if __name__ == '__main__':
    main()
