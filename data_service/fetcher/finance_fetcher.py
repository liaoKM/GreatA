"""财报数据拉取器基类"""
from abc import abstractmethod
from datetime import datetime
import pandas as pd
from .base import BaseFetcher


class FinanceFetcher(BaseFetcher):
    """财报数据拉取器基类"""

    DATA_TYPE = 'finance'

    @abstractmethod
    def fetch_daily(self, date: datetime) -> pd.DataFrame:
        """
        拉取某日公告的财报数据

        Args:
            date: 公告日期

        Returns:
            DataFrame包含当日公告的所有财报
        """
        pass

    @abstractmethod
    def fetch_history(self, start: datetime, end: datetime) -> pd.DataFrame:
        """
        拉取历史财报数据

        Args:
            start: 开始日期
            end: 结束日期

        Returns:
            DataFrame包含时间范围内所有财报
        """
        pass
