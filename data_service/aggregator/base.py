"""数据整合基类"""
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
import pandas as pd


class BaseAggregator(ABC):
    """数据整合抽象基类"""

    # 子类需要定义
    DATA_TYPE: str = None  # 'market', 'finance', 'announcement'

    def __init__(self, data_path: str = './data'):
        self.data_path = Path(data_path)
        self.daily_path = self.data_path / self.DATA_TYPE / 'daily'
        self.monthly_path = self.data_path / self.DATA_TYPE / 'monthly'
        self.yearly_path = self.data_path / self.DATA_TYPE / 'yearly'

    @abstractmethod
    def aggregate_daily_to_monthly(self, year: int, month: int) -> Path:
        """
        将当月所有 daily 合并为 monthly

        Args:
            year: 年份
            month: 月份

        Returns:
            生成的monthly文件路径
        """
        pass

    @abstractmethod
    def aggregate_monthly_to_yearly(self, year: int) -> Path:
        """
        将当年所有 monthly 合并为 yearly

        Args:
            year: 年份

        Returns:
            生成的yearly文件路径
        """
        pass

    def _load_daily_files(self, year: int, month: int) -> list[Path]:
        """获取当月所有daily文件"""
        pattern = f"{year}-{month:02d}-*.parquet"
        return sorted(self.daily_path.glob(pattern))

    def _load_monthly_files(self, year: int) -> list[Path]:
        """获取当年所有monthly文件"""
        pattern = f"{year}-*.parquet"
        return sorted(self.monthly_path.glob(pattern))

    def _delete_daily_files(self, year: int, month: int) -> int:
        """
        删除当月所有daily文件

        Args:
            year: 年份
            month: 月份

        Returns:
            删除的文件数量
        """
        files = self._load_daily_files(year, month)
        count = 0
        for f in files:
            f.unlink()
            count += 1
        return count

    def _delete_monthly_files(self, year: int) -> int:
        """
        删除当年所有monthly文件

        Args:
            year: 年份

        Returns:
            删除的文件数量
        """
        files = self._load_monthly_files(year)
        count = 0
        for f in files:
            f.unlink()
            count += 1
        return count

    def check_daily_complete(self, year: int, month: int) -> bool:
        """
        检查当月daily数据是否完整（用于决定是否整合）

        Args:
            year: 年份
            month: 月份

        Returns:
            是否完整
        """
        # TODO: 实现交易日历检查
        raise NotImplementedError("待实现交易日历检查")
