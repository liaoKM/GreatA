"""Tushare 公告数据拉取器"""
from datetime import datetime, timedelta
import time
import pandas as pd
import tushare as ts
from ..announcement_fetcher import AnnouncementFetcher, ANNOUNCEMENT_TYPES
from config.settings import get_token


class TushareAnnouncementFetcher(AnnouncementFetcher):
    """Tushare 公告数据拉取器

    支持的公告类型:
    - forecast: 业绩预告
    - express: 业绩快报
    - repurchase: 股票回购
    - holdertrade: 股东增减持
    - broker_forecast: 券商预测
    """

    def __init__(self, token: str = None, data_path: str = './data'):
        super().__init__(data_path)
        if token is None:
            token = get_token('tushare')
        self.pro = ts.pro_api(token)

    def fetch_by_type(self, ann_type: str, date: datetime) -> pd.DataFrame:
        """
        拉取指定类型的单日公告数据

        Args:
            ann_type: 公告类型
            date: 公告日期

        Returns:
            DataFrame包含当日该类型的所有公告
        """
        if ann_type not in ANNOUNCEMENT_TYPES:
            raise ValueError(f"未知的公告类型: {ann_type}，支持的类型: {list(ANNOUNCEMENT_TYPES.keys())}")

        ann_date = date.strftime('%Y%m%d')

        if ann_type == 'forecast':
            df = self._fetch_forecast(ann_date)
        elif ann_type == 'express':
            df = self._fetch_express(ann_date)
        elif ann_type == 'repurchase':
            df = self._fetch_repurchase(ann_date)
        elif ann_type == 'holdertrade':
            df = self._fetch_holdertrade(ann_date)
        elif ann_type == 'broker_forecast':
            df = self._fetch_broker_forecast(ann_date)

        if df is None or df.empty:
            return pd.DataFrame()

        # 标准化字段名
        df = df.rename(columns={'ts_code': 'stock_code'})
        if 'ann_date' in df.columns:
            df['ann_date'] = pd.to_datetime(df['ann_date'])

        return df

    def fetch_history_by_type(self, ann_type: str, start: datetime, end: datetime) -> pd.DataFrame:
        """
        拉取指定类型的历史公告数据

        Args:
            ann_type: 公告类型
            start: 开始日期
            end: 结束日期

        Returns:
            DataFrame包含时间范围内该类型的所有公告
        """
        if ann_type not in ANNOUNCEMENT_TYPES:
            raise ValueError(f"未知的公告类型: {ann_type}")

        # 对于支持 ann_date 参数的接口，可以逐日拉取或用日期范围
        # report_rc 接口支持 start_date/end_date
        if ann_type == 'broker_forecast':
            return self._fetch_broker_forecast_history(start, end)

        # 其他接口支持 ann_date，逐日拉取
        all_dfs = []
        current = start

        while current <= end:
            df = self.fetch_by_type(ann_type, current)
            time.sleep(0.12)
            if df is not None and not df.empty:
                all_dfs.append(df)
            current = current + timedelta(days=1)

        if not all_dfs:
            return pd.DataFrame()

        return pd.concat(all_dfs, ignore_index=True)

    def _fetch_forecast(self, ann_date: str) -> pd.DataFrame:
        """业绩预告"""
        return self.pro.forecast(ann_date=ann_date)

    def _fetch_express(self, ann_date: str) -> pd.DataFrame:
        """业绩快报"""
        return self.pro.express(ann_date=ann_date)

    def _fetch_repurchase(self, ann_date: str) -> pd.DataFrame:
        """股票回购"""
        return self.pro.repurchase(ann_date=ann_date)

    def _fetch_holdertrade(self, ann_date: str) -> pd.DataFrame:
        """股东增减持"""
        return self.pro.stk_holdertrade(ann_date=ann_date)

    def _fetch_broker_forecast(self, ann_date: str) -> pd.DataFrame:
        """券商预测（单日）

        注意: report_rc 接口的 report_date 是研报发布日期，需要筛选
        """
        df = self.pro.report_rc(report_date=ann_date)
        return df

    def _fetch_broker_forecast_history(self, start: datetime, end: datetime) -> pd.DataFrame:
        """券商预测（历史范围）"""
        start_date = start.strftime('%Y%m%d')
        end_date = end.strftime('%Y%m%d')

        df = self.pro.report_rc(start_date=start_date, end_date=end_date)

        if df is None or df.empty:
            return pd.DataFrame()

        df = df.rename(columns={'ts_code': 'stock_code'})
        df['ann_date'] = pd.to_datetime(df['report_date'])

        return df

    # 保留旧方法以兼容
    def fetch_forecast(self, start: datetime, end: datetime) -> pd.DataFrame:
        """获取业绩预告（兼容旧接口）"""
        return self.fetch_history_by_type('forecast', start, end)

    def fetch_express(self, start: datetime, end: datetime) -> pd.DataFrame:
        """获取业绩快报（兼容旧接口）"""
        return self.fetch_history_by_type('express', start, end)