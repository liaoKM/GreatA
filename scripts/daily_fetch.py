"""每日数据拉取脚本

使用方法:
    python scripts/daily_fetch.py                    # 拉取今日数据
    python scripts/daily_fetch.py --date 2025-01-15  # 拉取指定日期数据
    python scripts/daily_fetch.py --type market      # 只拉取市场数据
"""
import argparse
from datetime import datetime, date
import sys
from pathlib import Path

# 添加项目根目录到路径
sys.path.insert(0, str(Path(__file__).parent.parent))

from data_service import (
    TushareMarketFetcher, TushareFinanceFetcher, TushareAnnouncementFetcher
)


def fetch_daily(target_date: date, data_type: str = 'all'):
    """
    拉取指定日期的数据

    Args:
        target_date: 目标日期
        data_type: 数据类型 ('market', 'finance', 'announcement', 'all')
    """
    print(f"开始拉取 {target_date} 的数据...")

    if data_type in ('market', 'all'):
        print("  拉取市场数据...")
        fetcher = TushareMarketFetcher()
        df = fetcher.fetch_daily(target_date)
        if df is not None and not df.empty:
            path = fetcher.save_daily(df, target_date)
            print(f"  市场数据已保存: {path} ({len(df)} 条)")
        else:
            print("  市场数据为空（可能是非交易日）")

    if data_type in ('finance', 'all'):
        print("  拉取财报数据...")
        fetcher = TushareFinanceFetcher()
        df = fetcher.fetch_daily(target_date)
        if df is not None and not df.empty:
            path = fetcher.save_daily(df, target_date)
            print(f"  财报数据已保存: {path} ({len(df)} 条)")
        else:
            print("  当日无新财报公告")

    if data_type in ('announcement', 'all'):
        print("  拉取公告数据...")
        fetcher = TushareAnnouncementFetcher()
        df = fetcher.fetch_daily(target_date)
        if df is not None and not df.empty:
            path = fetcher.save_daily(df, target_date)
            print(f"  公告数据已保存: {path} ({len(df)} 条)")
        else:
            print("  当日无新公告")

    print("拉取完成!")


def main():
    parser = argparse.ArgumentParser(description='每日数据拉取')
    parser.add_argument('--date', type=str, help='指定日期 (YYYY-MM-DD)，默认为今天')
    parser.add_argument('--type', type=str, default='all',
                        choices=['market', 'finance', 'announcement', 'all'],
                        help='数据类型，默认为all')
    args = parser.parse_args()

    if args.date:
        target_date = datetime.strptime(args.date, '%Y-%m-%d').date()
    else:
        target_date = date.today()

    fetch_daily(target_date, args.type)


if __name__ == '__main__':
    main()
