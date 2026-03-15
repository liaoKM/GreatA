"""公告数据拉取器基类"""
from abc import abstractmethod
from datetime import datetime
import pandas as pd
from .base import BaseFetcher


class AnnouncementFetcher(BaseFetcher):
    """公告数据拉取器基类"""

    DATA_TYPE = 'announcement'

    @abstractmethod
    def fetch_daily(self, date: datetime) -> pd.DataFrame:
        """
        拉取某日所有公告

        Args:
            date: 公告日期

        Returns:
            DataFrame包含当日所有公告
        """
        pass

    @abstractmethod
    def fetch_history(self, start: datetime, end: datetime) -> pd.DataFrame:
        """
        拉取历史公告数据

        Args:
            start: 开始日期
            end: 结束日期

        Returns:
            DataFrame包含时间范围内所有公告
        """
        pass

    @abstractmethod
    def fetch_by_type(self, date: datetime, ann_type: str) -> pd.DataFrame:
        """
        按公告类型拉取

        Args:
            date: 公告日期
            ann_type: 公告类型，如 '业绩预告', '增减持' 等

        Returns:
            DataFrame包含该类型公告
        """
        pass
