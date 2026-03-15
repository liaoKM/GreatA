"""年度数据整合脚本

使用方法:
    python scripts/aggregate_yearly                # 整合去年数据
    python scripts/aggregate_yearly --year 2024    # 整合指定年份
"""
import argparse
from datetime import date
import sys
from pathlib import Path

# 添加项目根目录到路径
sys.path.insert(0, str(Path(__file__).parent.parent))

from data_service import MarketAggregator, FinanceAggregator, AnnouncementAggregator


def aggregate_yearly(year: int):
    """
    整合指定年份的数据

    Args:
        year: 年份
    """
    print(f"开始整合 {year}年 的数据...")

    # 市场数据
    print("  整合市场数据...")
    market_agg = MarketAggregator()
    path = market_agg.aggregate_monthly_to_yearly(year)
    if path:
        print(f"  市场数据已整合: {path}")
    else:
        print("  无市场数据需要整合")

    # 财报数据
    print("  整合财报数据...")
    finance_agg = FinanceAggregator()
    path = finance_agg.aggregate_monthly_to_yearly(year)
    if path:
        print(f"  财报数据已整合: {path}")
    else:
        print("  无财报数据需要整合")

    # 公告数据
    print("  整合公告数据...")
    announcement_agg = AnnouncementAggregator()
    path = announcement_agg.aggregate_monthly_to_yearly(year)
    if path:
        print(f"  公告数据已整合: {path}")
    else:
        print("  无公告数据需要整合")

    print("整合完成!")


def main():
    parser = argparse.ArgumentParser(description='年度数据整合')
    parser.add_argument('--year', type=int, help='年份')
    args = parser.parse_args()

    if args.year:
        year = args.year
    else:
        # 默认整合去年
        year = date.today().year - 1

    aggregate_yearly(year)


if __name__ == '__main__':
    main()
