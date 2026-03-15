"""Tushare 财报数据拉取器"""
from datetime import datetime
import pandas as pd
import tushare as ts
from ..finance_fetcher import FinanceFetcher


class TushareFinanceFetcher(FinanceFetcher):
    """Tushare 财报数据拉取器"""

    def __init__(self, token: str = None, data_path: str = './data'):
        super().__init__(data_path)
        self.pro = ts.pro_api(token)

    def fetch_daily(self, date: datetime) -> pd.DataFrame:
        """
        拉取某日公告的财报数据

        Args:
            date: 公告日期

        Returns:
            DataFrame包含当日公告的所有财报
        """
        ann_date = date.strftime('%Y%m%d')

        # 获取财务指标数据
        df = self.pro.fina_indicator(ann_date=ann_date)

        if df is None or df.empty:
            return pd.DataFrame()

        # 字段重命名
        df = df.rename(columns={
            'ts_code': 'stock_code',
        })

        df['ann_date'] = pd.to_datetime(df['ann_date'])
        df['end_date'] = pd.to_datetime(df['end_date'])

        return df

    def fetch_history(self, start: datetime, end: datetime) -> pd.DataFrame:
        """
        拉取历史财报数据

        Args:
            start: 开始日期
            end: 结束日期

        Returns:
            DataFrame包含时间范围内所有财报
        """
        start_date = start.strftime('%Y%m%d')
        end_date = end.strftime('%Y%m%d')

        df = self.pro.fina_indicator(start_date=start_date, end_date=end_date)

        if df is None or df.empty:
            return pd.DataFrame()

        df = df.rename(columns={
            'ts_code': 'stock_code',
        })

        df['ann_date'] = pd.to_datetime(df['ann_date'])
        df['end_date'] = pd.to_datetime(df['end_date'])

        return df

    def fetch_by_report_period(self, period: str) -> pd.DataFrame:
        """
        按报告期拉取财报

        Args:
            period: 报告期，如 '20251231'（年报）, '20250930'（三季报）

        Returns:
            DataFrame包含该报告期所有财报
        """
        df = self.pro.fina_indicator(end_date=period)

        if df is None or df.empty:
            return pd.DataFrame()

        df = df.rename(columns={
            'ts_code': 'stock_code',
        })

        df['ann_date'] = pd.to_datetime(df['ann_date'])
        df['end_date'] = pd.to_datetime(df['end_date'])

        return df

    def fetch_income(self, period: str = None, ts_code: str = None) -> pd.DataFrame:
        """
        获取利润表

        Args:
            period: 报告期
            ts_code: 股票代码

        Returns:
            利润表DataFrame
        """
        params = {}
        if period:
            params['period'] = period
        if ts_code:
            params['ts_code'] = ts_code

        df = self.pro.income(**params)
        return df

    def fetch_balancesheet(self, period: str = None, ts_code: str = None) -> pd.DataFrame:
        """
        获取资产负债表

        Args:
            period: 报告期
            ts_code: 股票代码

        Returns:
            资产负债表DataFrame
        """
        params = {}
        if period:
            params['period'] = period
        if ts_code:
            params['ts_code'] = ts_code

        df = self.pro.balancesheet(**params)
        return df
