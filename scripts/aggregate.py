"""数据聚合脚本

使用方法:
    python scripts/aggregate.py                     # 自动聚合所有类型
    python scripts/aggregate.py --type market       # 只聚合市场数据
    python scripts/aggregate.py --year 2024         # 强制聚合指定年份
    python scripts/aggregate.py --year 2024 --month 3  # 强制聚合指定月份

聚合逻辑：
    - 自动检测满一年的月度数据 → 聚合为年度文件
    - 自动检测满一月的日度数据 → 聚合为月度文件
    - 已存在的年度/月度文件会被跳过
"""
import argparse
from datetime import datetime, date
import sys
from pathlib import Path

# 添加项目根目录到路径
sys.path.insert(0, str(Path(__file__).parent.parent))

from data_service import MarketAggregator, FinanceAggregator, AnnouncementAggregator

DATA_TYPES = ['market', 'finance', 'announcement']
AGGREGATORS = {
    'market': MarketAggregator,
    'finance': FinanceAggregator,
    'announcement': AnnouncementAggregator
}


def get_expected_trade_days(year: int, month: int = None) -> list:
    """
    获取预期的交易日列表（简化版：根据当前年份估算）

    实际应用中应该从交易日历API获取
    这里用经验值：每月约20-23个交易日
    """
    # 简化：返回该月所有工作日（实际应查交易日历）
    from calendar import monthrange
    import datetime

    days = []
    if month:
        start = datetime.date(year, month, 1)
        _, last_day = monthrange(year, month)
        end = datetime.date(year, month, last_day)
    else:
        start = datetime.date(year, 1, 1)
        end = datetime.date(year, 12, 31)

    current = start
    while current <= end:
        # 周一到周五
        if current.weekday() < 5:
            days.append(current)
        current += datetime.timedelta(days=1)

    return days


def get_available_months(aggregator, year: int) -> list:
    """获取该年已有的月度文件列表"""
    monthly_files = aggregator._load_monthly_files(year)
    months = []
    for f in monthly_files:
        # 2024-01.parquet -> 1
        try:
            month = int(f.stem.split('-')[1])
            months.append(month)
        except:
            pass
    return sorted(months)


def get_available_daily_days(aggregator, year: int, month: int) -> list:
    """获取该月已有的日度文件列表"""
    daily_files = aggregator._load_daily_files(year, month)
    days = []
    for f in daily_files:
        # 2024-01-15.parquet -> 15
        try:
            day = int(f.stem.split('-')[2])
            days.append(day)
        except:
            pass
    return sorted(days)


def aggregate_yearly(aggregator, year: int, force: bool = False) -> bool:
    """
    聚合年度数据

    Args:
        aggregator: 聚合器实例
        year: 年份
        force: 是否强制聚合（不检查完整性）

    Returns:
        是否成功聚合
    """
    # 检查年度文件是否已存在
    yearly_file = aggregator.yearly_path / f"{year}.parquet"
    if yearly_file.exists():
        print(f"      跳过 {year} 年（年度文件已存在）")
        return False

    # 检查月度文件
    available_months = get_available_months(aggregator, year)
    if not available_months:
        return False

    # 检查是否满12个月
    if not force and len(available_months) < 12:
        print(f"      跳过 {year} 年（月度数据不完整: {len(available_months)}/12）")
        return False

    # 执行聚合
    path = aggregator.aggregate_monthly_to_yearly(year)
    if path:
        print(f"      聚合 {year} 年: {path} ({len(available_months)} 个月)")
        return True
    return False


def aggregate_monthly(aggregator, year: int, month: int, force: bool = False) -> bool:
    """
    聚合月度数据

    Args:
        aggregator: 聚合器实例
        year: 年份
        month: 月份
        force: 是否强制聚合

    Returns:
        是否成功聚合
    """
    # 检查月度文件是否已存在
    monthly_file = aggregator.monthly_path / f"{year}-{month:02d}.parquet"
    if monthly_file.exists():
        print(f"      跳过 {year}-{month:02d}（月度文件已存在）")
        return False

    # 检查日度文件
    available_days = get_available_daily_days(aggregator, year, month)
    if not available_days:
        return False

    # 检查是否足够（至少有日度文件才聚合）
    expected_days = get_expected_trade_days(year, month)
    min_days = max(1, len(expected_days) // 2)  # 至少一半交易日

    if not force and len(available_days) < min_days:
        print(f"      跳过 {year}-{month:02d}（日度数据不足: {len(available_days)}天）")
        return False

    # 执行聚合
    path = aggregator.aggregate_daily_to_monthly(year, month)
    if path:
        print(f"      聚合 {year}-{month:02d}: {path} ({len(available_days)} 天)")
        return True
    return False


def aggregate_type(data_type: str, year: int = None, month: int = None, force: bool = False):
    """
    聚合指定类型的数据

    Args:
        data_type: 数据类型
        year: 指定年份（可选）
        month: 指定月份（可选）
        force: 是否强制聚合
    """
    print(f"  聚合 {data_type} 数据...")
    aggregator = AGGREGATORS[data_type]()

    today = date.today()

    if year and month:
        # 指定具体月份
        aggregate_monthly(aggregator, year, month, force=True)
    elif year:
        # 指定具体年份
        aggregate_yearly(aggregator, year, force=True)
        # 也聚合该年所有月
        for m in range(1, 13):
            aggregate_monthly(aggregator, year, m, force=True)
    else:
        # 自动聚合
        # 1. 先聚合去年及之前的完整年份
        last_year = today.year - 1
        for y in range(last_year, last_year - 3, -1):  # 检查最近3年
            aggregate_yearly(aggregator, y, force=False)

        # 2. 聚合上月及之前的完整月份
        if today.month == 1:
            last_month_year, last_month = today.year - 1, 12
        else:
            last_month_year, last_month = today.year, today.month - 1

        # 检查今年和去年的月度
        for y in [last_month_year, last_month_year - 1]:
            for m in range(12, 0, -1):
                if y == last_month_year and m > last_month:
                    continue
                aggregate_monthly(aggregator, y, m, force=False)


def aggregate(data_type: str = 'all', year: int = None, month: int = None):
    """
    聚合数据

    Args:
        data_type: 数据类型 ('market', 'finance', 'announcement', 'all')
        year: 指定年份
        month: 指定月份
    """
    print(f"开始聚合数据...")

    types_to_process = DATA_TYPES if data_type == 'all' else [data_type]

    for t in types_to_process:
        aggregate_type(t, year, month)

    print("聚合完成!")


def main():
    parser = argparse.ArgumentParser(description='数据聚合')
    parser.add_argument('--type', type=str, default='all',
                        choices=['market', 'finance', 'announcement', 'all'],
                        help='数据类型，默认为all')
    parser.add_argument('--year', type=int, help='指定年份')
    parser.add_argument('--month', type=int, help='指定月份（需配合--year使用）')
    args = parser.parse_args()

    aggregate(args.type, args.year, args.month)


if __name__ == '__main__':
    main()
