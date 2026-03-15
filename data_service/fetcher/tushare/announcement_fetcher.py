"""Tushare 公告数据拉取器"""
from datetime import datetime
import pandas as pd
import tushare as ts
from ..announcement_fetcher import AnnouncementFetcher


class TushareAnnouncementFetcher(AnnouncementFetcher):
    """Tushare 公告数据拉取器"""

    def __init__(self, token: str = None, data_path: str = './data'):
        super().__init__(data_path)
        self.pro = ts.pro_api(token)

    def fetch_daily(self, date: datetime) -> pd.DataFrame:
        """
        拉取某日所有公告

        Args:
            date: 公告日期

        Returns:
            DataFrame包含当日所有公告
        """
        ann_date = date.strftime('%Y%m%d')

        # 获取公告数据
        df = self.pro.ann_d(ann_date=ann_date)

        if df is None or df.empty:
            return pd.DataFrame()

        # 字段重命名
        df = df.rename(columns={
            'ts_code': 'stock_code',
        })

        df['ann_date'] = pd.to_datetime(df['ann_date'])

        return df

    def fetch_history(self, start: datetime, end: datetime) -> pd.DataFrame:
        """
        拉取历史公告数据

        Args:
            start: 开始日期
            end: 结束日期

        Returns:
            DataFrame包含时间范围内所有公告
        """
        start_date = start.strftime('%Y%m%d')
        end_date = end.strftime('%Y%m%d')

        df = self.pro.ann_d(start_date=start_date, end_date=end_date)

        if df is None or df.empty:
            return pd.DataFrame()

        df = df.rename(columns={
            'ts_code': 'stock_code',
        })

        df['ann_date'] = pd.to_datetime(df['ann_date'])

        return df

    def fetch_by_type(self, date: datetime, ann_type: str) -> pd.DataFrame:
        """
        按公告类型拉取

        Args:
            date: 公告日期
            ann_type: 公告类型

        Returns:
            DataFrame包含该类型公告
        """
        ann_date = date.strftime('%Y%m%d')

        df = self.pro.ann_d(ann_date=ann_date, ann_type=ann_type)

        if df is None or df.empty:
            return pd.DataFrame()

        df = df.rename(columns={
            'ts_code': 'stock_code',
        })

        df['ann_date'] = pd.to_datetime(df['ann_date'])

        return df

    def fetch_forecast(self, start: datetime, end: datetime) -> pd.DataFrame:
        """
        获取业绩预告

        Args:
            start: 开始日期
            end: 结束日期

        Returns:
            业绩预告DataFrame
        """
        start_date = start.strftime('%Y%m%d')
        end_date = end.strftime('%Y%m%d')

        df = self.pro.forecast(start_date=start_date, end_date=end_date)
        return df

    def fetch_express(self, start: datetime, end: datetime) -> pd.DataFrame:
        """
        获取业绩快报

        Args:
            start: 开始日期
            end: 结束日期

        Returns:
            业绩快报DataFrame
        """
        start_date = start.strftime('%Y%m%d')
        end_date = end.strftime('%Y%m%d')

        df = self.pro.express(start_date=start_date, end_date=end_date)
        return df
