"""Tushare 市场数据拉取器"""
from datetime import datetime, timedelta
import pandas as pd
import tushare as ts
from ..market_fetcher import MarketFetcher
from config.settings import get_token


class TushareMarketFetcher(MarketFetcher):
    """Tushare 市场数据拉取器

    数据来源：
    - daily: 日线行情（open, high, low, close, volume, amount 等）
    - adj_factor: 复权因子
    - daily_basic: 每日指标（pe, pb, turnover_rate 等）

    注意：tushare 单次请求有 6000 条限制，按交易日循环获取
    """

    def __init__(self, token: str = None, data_path: str = './data'):
        super().__init__(data_path)
        if token is None:
            token = get_token('tushare')
        self.pro = ts.pro_api(token)

    def fetch_daily(self, date: datetime) -> pd.DataFrame:
        """
        拉取某日全市场日线数据，合并 daily + adj_factor + daily_basic

        Args:
            date: 交易日

        Returns:
            DataFrame包含合并后的全部字段
        """
        trade_date = date.strftime('%Y%m%d')

        # 1. 获取日线行情
        df_daily = self.pro.daily(trade_date=trade_date)
        if df_daily is None or df_daily.empty:
            return pd.DataFrame()

        # 2. 获取复权因子
        df_adj = self.pro.adj_factor(trade_date=trade_date)

        # 3. 获取每日指标
        df_basic = self.pro.daily_basic(trade_date=trade_date)

        # 合并数据
        df = self._merge_data(df_daily, df_adj, df_basic)
        return df

    def fetch_history(self, start: datetime, end: datetime) -> pd.DataFrame:
        """
        拉取历史日线数据，合并 daily + adj_factor + daily_basic

        由于 tushare 单次请求有 6000 条限制，按交易日循环获取

        Args:
            start: 开始日期
            end: 结束日期

        Returns:
            DataFrame包含时间范围内所有日线数据
        """
        # 获取交易日历
        trade_cal = self.pro.trade_cal(
            start_date=start.strftime('%Y%m%d'),
            end_date=end.strftime('%Y%m%d'),
            is_open='1'
        )

        if trade_cal is None or trade_cal.empty:
            return pd.DataFrame()

        trade_dates = trade_cal['cal_date'].tolist()

        # 按交易日循环获取数据
        all_data = []
        for trade_date in trade_dates:
            df = self._fetch_single_day(trade_date)
            if df is not None and not df.empty:
                all_data.append(df)

        if not all_data:
            return pd.DataFrame()

        return pd.concat(all_data, ignore_index=True)

    def _fetch_single_day(self, trade_date: str) -> pd.DataFrame:
        """
        获取单个交易日的合并数据

        Args:
            trade_date: 交易日期 (YYYYMMDD 格式)

        Returns:
            合并后的 DataFrame
        """
        # 1. 获取日线行情
        df_daily = self.pro.daily(trade_date=trade_date)
        if df_daily is None or df_daily.empty:
            return pd.DataFrame()

        # 2. 获取复权因子
        df_adj = self.pro.adj_factor(trade_date=trade_date)

        # 3. 获取每日指标
        df_basic = self.pro.daily_basic(trade_date=trade_date)

        # 合并数据
        df = self._merge_data(df_daily, df_adj, df_basic)
        return df

    def _merge_data(self, df_daily: pd.DataFrame, df_adj: pd.DataFrame, df_basic: pd.DataFrame) -> pd.DataFrame:
        """
        合并 daily + adj_factor + daily_basic 数据

        Args:
            df_daily: 日线行情数据
            df_adj: 复权因子数据
            df_basic: 每日指标数据

        Returns:
            合并后的DataFrame
        """
        df = df_daily.copy()

        # 合并复权因子
        if df_adj is not None and not df_adj.empty:
            df = df.merge(
                df_adj[['ts_code', 'trade_date', 'adj_factor']],
                on=['ts_code', 'trade_date'],
                how='left'
            )

        # 合并每日指标
        if df_basic is not None and not df_basic.empty:
            # 选择需要的指标列
            basic_cols = ['ts_code', 'trade_date', 'close', 'turnover_rate', 'turnover_rate_f',
                          'volume_ratio', 'pe', 'pe_ttm', 'pb', 'ps', 'ps_ttm',
                          'dv_ratio', 'dv_ttm', 'total_share', 'float_share',
                          'free_share', 'total_mv', 'circ_mv']
            # 只保留存在的列
            available_cols = [col for col in basic_cols if col in df_basic.columns]
            df_basic_subset = df_basic[available_cols].copy()

            # 重命名 close 列避免冲突
            if 'close' in df_basic_subset.columns:
                df_basic_subset = df_basic_subset.rename(columns={'close': 'close_basic'})

            df = df.merge(df_basic_subset, on=['ts_code', 'trade_date'], how='left')

        # 字段重命名
        df = df.rename(columns={
            'ts_code': 'stock_code',
            'vol': 'volume',
        })

        # 转换日期格式
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
