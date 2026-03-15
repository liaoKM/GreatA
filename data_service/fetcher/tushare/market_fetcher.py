"""Tushare 市场数据拉取器"""
from datetime import datetime
import pandas as pd
import tushare as ts
from ..market_fetcher import MarketFetcher


class TushareMarketFetcher(MarketFetcher):
    """Tushare 市场数据拉取器"""

    def __init__(self, token: str = None, data_path: str = './data'):
        super().__init__(data_path)
        self.pro = ts.pro_api(token)

    def fetch_daily(self, date: datetime) -> pd.DataFrame:
        """
        拉取某日全市场日线数据

        Args:
            date: 交易日

        Returns:
            DataFrame包含: ts_code, trade_date, open, high, low, close, vol, amount 等
        """
        trade_date = date.strftime('%Y%m%d')
        df = self.pro.daily(trade_date=trade_date)

        if df is None or df.empty:
            return pd.DataFrame()

        # 字段重命名以统一格式
        df = df.rename(columns={
            'ts_code': 'stock_code',
            'vol': 'volume',
        })

        # 转换日期格式
        df['trade_date'] = pd.to_datetime(df['trade_date'])

        return df

    def fetch_history(self, start: datetime, end: datetime) -> pd.DataFrame:
        """
        拉取历史日线数据

        Args:
            start: 开始日期
            end: 结束日期

        Returns:
            DataFrame包含时间范围内所有日线数据
        """
        start_date = start.strftime('%Y%m%d')
        end_date = end.strftime('%Y%m%d')

        df = self.pro.daily(start_date=start_date, end_date=end_date)

        if df is None or df.empty:
            return pd.DataFrame()

        # 字段重命名
        df = df.rename(columns={
            'ts_code': 'stock_code',
            'vol': 'volume',
        })

        df['trade_date'] = pd.to_datetime(df['trade_date'])

        return df

    def fetch_index_daily(self, date: datetime, index_code: str = '000300.SH') -> pd.DataFrame:
        """
        拉取指数日线数据

        Args:
            date: 交易日
            index_code: 指数代码

        Returns:
            DataFrame包含指数日线数据
        """
        trade_date = date.strftime('%Y%m%d')
        df = self.pro.index_daily(ts_code=index_code, start_date=trade_date, end_date=trade_date)
        return df

    def fetch_trade_calendar(self, start: datetime, end: datetime) -> pd.DataFrame:
        """
        获取交易日历

        Args:
            start: 开始日期
            end: 结束日期

        Returns:
            DataFrame包含交易日历
        """
        start_date = start.strftime('%Y%m%d')
        end_date = end.strftime('%Y%m%d')
        df = self.pro.trade_cal(start_date=start_date, end_date=end_date)
        return df
