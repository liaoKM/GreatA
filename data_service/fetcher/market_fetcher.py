"""市场数据拉取器基类"""
from abc import abstractmethod
from datetime import datetime
import pandas as pd
from .base import BaseFetcher


class MarketFetcher(BaseFetcher):
    """市场数据拉取器基类"""

    DATA_TYPE = 'market'

    @abstractmethod
    def fetch_daily(self, date: datetime) -> pd.DataFrame:
        """
        拉取某日全市场日线数据

        Args:
            date: 交易日

        Returns:
            DataFrame包含: stock_code, date, open, high, low, close, volume, amount 等
        """
        pass

    @abstractmethod
    def fetch_history(self, start: datetime, end: datetime) -> pd.DataFrame:
        """
        拉取历史日线数据

        Args:
            start: 开始日期
            end: 结束日期

        Returns:
            DataFrame包含时间范围内所有日线数据
        """
        pass
