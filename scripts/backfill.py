"""历史数据回填脚本

使用方法:
    python scripts/backfill.py --start 2024-01-01 --end 2024-12-31  # 回填指定时间范围
    python scripts/backfill.py --start 2020-01-01                    # 回填到今天
    python scripts/backfill.py --type market --start 2024-01-01     # 只回填市场数据
"""
import argparse
from datetime import datetime
import sys
from pathlib import Path

# 添加项目根目录到路径
sys.path.insert(0, str(Path(__file__).parent.parent))

from data_service import (
    TushareMarketFetcher, TushareFinanceFetcher, TushareAnnouncementFetcher
)


def backfill_market(start_date: datetime, end_date: datetime):
    """回填市场数据"""
    print(f"  回填市场数据 ({start_date.date()} ~ {end_date.date()})...")
    fetcher = TushareMarketFetcher()
    df = fetcher.fetch_history(start_date, end_date)

    if df is None or df.empty:
        print("  无市场数据")
        return

    # 按日期拆分保存
    for trade_date in df['trade_date'].unique():
        daily_df = df[df['trade_date'] == trade_date]
        dt = pd.to_datetime(trade_date)
        fetcher.save_daily(daily_df, dt)

    print(f"  市场数据回填完成，共 {len(df)} 条")


def backfill_finance(start_date: datetime, end_date: datetime):
    """回填财报数据"""
    print(f"  回填财报数据 ({start_date.date()} ~ {end_date.date()})...")
    fetcher = TushareFinanceFetcher()
    df = fetcher.fetch_history(start_date, end_date)

    if df is None or df.empty:
        print("  无财报数据")
        return

    # 按公告日期拆分保存
    for ann_date in df['ann_date'].unique():
        daily_df = df[df['ann_date'] == ann_date]
        dt = pd.to_datetime(ann_date)
        fetcher.save_daily(daily_df, dt)

    print(f"  财报数据回填完成，共 {len(df)} 条")


def backfill_announcement(start_date: datetime, end_date: datetime):
    """回填公告数据"""
    print(f"  回填公告数据 ({start_date.date()} ~ {end_date.date()})...")
    fetcher = TushareAnnouncementFetcher()
    df = fetcher.fetch_history(start_date, end_date)

    if df is None or df.empty:
        print("  无公告数据")
        return

    # 按公告日期拆分保存
    for ann_date in df['ann_date'].unique():
        daily_df = df[df['ann_date'] == ann_date]
        dt = pd.to_datetime(ann_date)
        fetcher.save_daily(daily_df, dt)

    print(f"  公告数据回填完成，共 {len(df)} 条")


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
    import pandas as pd  # 移到顶部会在 backfill 中用到
    main()
