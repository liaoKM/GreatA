"""月度数据整合脚本

使用方法:
    python scripts/aggregate_monthly.py                    # 整合上月数据
    python scripts/aggregate_monthly.py --year 2025 --month 1  # 整合指定月份
"""
import argparse
from datetime import datetime, date
import sys
from pathlib import Path

# 添加项目根目录到路径
sys.path.insert(0, str(Path(__file__).parent.parent))

from data_service import MarketAggregator, FinanceAggregator, AnnouncementAggregator


def aggregate_monthly(year: int, month: int):
    """
    整合指定月份的数据

    Args:
        year: 年份
        month: 月份
    """
    print(f"开始整合 {year}年{month}月 的数据...")

    # 市场数据
    print("  整合市场数据...")
    market_agg = MarketAggregator()
    path = market_agg.aggregate_daily_to_monthly(year, month)
    if path:
        print(f"  市场数据已整合: {path}")
    else:
        print("  无市场数据需要整合")

    # 财报数据
    print("  整合财报数据...")
    finance_agg = FinanceAggregator()
    path = finance_agg.aggregate_daily_to_monthly(year, month)
    if path:
        print(f"  财报数据已整合: {path}")
    else:
        print("  无财报数据需要整合")

    # 公告数据
    print("  整合公告数据...")
    announcement_agg = AnnouncementAggregator()
    path = announcement_agg.aggregate_daily_to_monthly(year, month)
    if path:
        print(f"  公告数据已整合: {path}")
    else:
        print("  无公告数据需要整合")

    print("整合完成!")


def main():
    parser = argparse.ArgumentParser(description='月度数据整合')
    parser.add_argument('--year', type=int, help='年份')
    parser.add_argument('--month', type=int, help='月份')
    args = parser.parse_args()

    if args.year and args.month:
        year, month = args.year, args.month
    else:
        # 默认整合上个月
        today = date.today()
        if today.month == 1:
            year, month = today.year - 1, 12
        else:
            year, month = today.year, today.month - 1

    aggregate_monthly(year, month)


if __name__ == '__main__':
    main()
