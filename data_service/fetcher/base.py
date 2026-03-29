"""数据拉取基类"""
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
import pandas as pd


class BaseFetcher(ABC):
    """数据拉取抽象基类"""

    # 子类需要定义
    DATA_TYPE: str = None  # 'market', 'finance', 'announcement'

    def __init__(self, data_path: str = './data'):
        self.data_path = Path(data_path)
        self.daily_path = self.data_path / self.DATA_TYPE / 'daily'
        self.monthly_path = self.data_path / self.DATA_TYPE / 'monthly'
        self.yearly_path = self.data_path / self.DATA_TYPE / 'yearly'

    @abstractmethod
    def fetch_daily(self, date: datetime) -> pd.DataFrame:
        """
        拉取某日增量数据

        Args:
            date: 日期

        Returns:
            当日数据的DataFrame
        """
        pass

    @abstractmethod
    def fetch_history(self, start: datetime, end: datetime) -> pd.DataFrame:
        """
        拉取历史数据（用于回填）

        Args:
            start: 开始日期
            end: 结束日期

        Returns:
            历史数据的DataFrame
        """
        pass

    def save_daily(self, df: pd.DataFrame, date: datetime) -> Path:
        """
        保存日增量数据到 daily/ 目录

        Args:
            df: 数据
            date: 日期

        Returns:
            保存的文件路径
        """

        filename = f"{date.strftime('%Y-%m-%d')}.parquet"
        filepath = self.daily_path / filename
        df.to_parquet(filepath, index=False)
        return filepath

    def get_last_update(self) -> datetime:
        """
        获取该数据类型最后更新日期

        Returns:
            最后更新日期，如果没有数据返回None
        """
        daily_files = list(self.daily_path.glob('*.parquet'))
        monthly_files = list(self.monthly_path.glob('*.parquet'))
        yearly_files = list(self.yearly_path.glob('*.parquet'))

        all_dates = []
        for f in daily_files + monthly_files + yearly_files:
            # 从文件名解析日期
            # daily: 2025-01-02.parquet -> 取最后一天
            # monthly: 2025-01.parquet -> 取月份最后一天
            # yearly: 2025.parquet -> 取年份最后一天
            stem = f.stem
            try:
                if len(stem) == 10:  # daily
                    all_dates.append(datetime.strptime(stem, '%Y-%m-%d'))
                elif len(stem) == 7:  # monthly
                    all_dates.append(datetime.strptime(stem, '%Y-%m'))
                elif len(stem) == 4:  # yearly
                    all_dates.append(datetime.strptime(stem, '%Y'))
            except ValueError:
                continue

        return max(all_dates) if all_dates else None
