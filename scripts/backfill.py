"""历史数据回填脚本

使用方法:
    python scripts/backfill.py --start 2024-01-01 --end 2024-12-31  # 回填指定时间范围
    python scripts/backfill.py --start 2020-01-01                    # 回填到今天
    python scripts/backfill.py --type market --start 2024-01-01     # 只回填市场数据

存储逻辑：
    - 整年数据（1月1日-12月31日）→ yearly/2024.parquet
    - 整月数据 → monthly/2024-03.parquet
    - 零散日期 → daily/2024-03-15.parquet
"""
import argparse
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import sys
from pathlib import Path

# 添加项目根目录到路径
sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
from data_service import (
    TushareMarketFetcher, TushareFinanceFetcher, TushareAnnouncementFetcher
)


def get_time_periods(start_date: datetime, end_date: datetime):
    """
    将时间范围划分为年度、月度、日度三个部分

    Returns:
        dict: {
            'yearly': [(year, start, end), ...],      # 完整年份
            'monthly': [(year, month, start, end), ...], # 完整月份
            'daily': [(date, date), ...]               # 零散日期
        }
    """
    periods = {
        'yearly': [],
        'monthly': [],
        'daily': []
    }

    start_year = start_date.year
    end_year = end_date.year

    # 遍历每一年
    for year in range(start_year, end_year + 1):
        year_start = datetime(year, 1, 1)
        year_end = datetime(year, 12, 31)

        # 实际的开始和结束日期
        actual_start = max(start_date, year_start)
        actual_end = min(end_date, year_end)

        # 检查是否是完整年份
        is_full_year = (actual_start == year_start and actual_end == year_end)

        if is_full_year:
            periods['yearly'].append((year, year_start, year_end))
        else:
            # 不完整年份，按月划分
            for month in range(actual_start.month, actual_end.month + 1):
                month_start = datetime(year, month, 1)

                # 月末日期
                if month == 12:
                    month_end = datetime(year, 12, 31)
                else:
                    month_end = datetime(year, month + 1, 1) - timedelta(days=1)

                # 实际的月开始和结束
                actual_month_start = max(actual_start, month_start)
                actual_month_end = min(actual_end, month_end)

                # 检查是否是完整月份
                is_full_month = (actual_month_start == month_start and actual_month_end == month_end)

                if is_full_month:
                    periods['monthly'].append((year, month, month_start, month_end))
                else:
                    # 不完整月份，按天拆分
                    current = actual_month_start
                    while current <= actual_month_end:
                        periods['daily'].append((current, current))
                        current += timedelta(days=1)

    return periods


def backfill_market(start_date: datetime, end_date: datetime):
    """回填市场数据"""
    print(f"  回填市场数据 ({start_date.date()} ~ {end_date.date()})...")

    fetcher = TushareMarketFetcher()
    periods = get_time_periods(start_date, end_date)

    # 1. 处理完整年份
    for year, year_start, year_end in periods['yearly']:
        print(f"    拉取 {year} 年数据...")
        df = fetcher.fetch_history(year_start, year_end)
        if df is not None and not df.empty:
            # 保存到 yearly 目录
            filepath = fetcher.yearly_path / f"{year}.parquet"
            df.to_parquet(filepath, index=False)
            print(f"    保存: {filepath} ({len(df)} 条)")

    # 2. 处理完整月份
    for year, month, month_start, month_end in periods['monthly']:
        print(f"    拉取 {year}-{month:02d} 月数据...")
        df = fetcher.fetch_history(month_start, month_end)
        if df is not None and not df.empty:
            # 保存到 monthly 目录
            filepath = fetcher.monthly_path / f"{year}-{month:02d}.parquet"
            df.to_parquet(filepath, index=False)
            print(f"    保存: {filepath} ({len(df)} 条)")

    # 3. 处理零散日期
    if periods['daily']:
        # 合并所有零散日期的请求
        daily_start = periods['daily'][0][0]
        daily_end = periods['daily'][-1][1]
        print(f"    拉取零散日期数据 ({daily_start.date()} ~ {daily_end.date()})...")
        df = fetcher.fetch_history(daily_start, daily_end)

        if df is not None and not df.empty:
            # 按日期拆分保存
            for trade_date in df['trade_date'].unique():
                daily_df = df[df['trade_date'] == trade_date]
                fetcher.save_daily(daily_df, trade_date)
            print(f"    保存了 {len(df)} 条日数据")

    print("  市场数据回填完成!")


def backfill_finance(start_date: datetime, end_date: datetime):
    """回填财报数据"""
    print(f"  回填财报数据 ({start_date.date()} ~ {end_date.date()})...")
    fetcher = TushareFinanceFetcher()

    # 财报数据按公告日期，一次性拉取
    df = fetcher.fetch_history(start_date, end_date)
    if df is not None and not df.empty:
        # 按公告日期拆分保存
        for ann_date in df['ann_date'].unique():
            daily_df = df[df['ann_date'] == ann_date]
            dt = pd.to_datetime(ann_date)
            fetcher.save_daily(daily_df, dt)
        print(f"  财报数据回填完成，共 {len(df)} 条")
    else:
        print("  无财报数据")


def backfill_announcement(start_date: datetime, end_date: datetime):
    """回填公告数据"""
    print(f"  回填公告数据 ({start_date.date()} ~ {end_date.date()})...")
    fetcher = TushareAnnouncementFetcher()

    df = fetcher.fetch_history(start_date, end_date)
    if df is not None and not df.empty:
        for ann_date in df['ann_date'].unique():
            daily_df = df[df['ann_date'] == ann_date]
            dt = pd.to_datetime(ann_date)
            fetcher.save_daily(daily_df, dt)
        print(f"  公告数据回填完成，共 {len(df)} 条")
    else:
        print("  无公告数据")


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
