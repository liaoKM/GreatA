"""Tushare 财报数据拉取器"""
from datetime import datetime,timedelta
import time
import pandas as pd
import tushare as ts
from ..finance_fetcher import FinanceFetcher
from config.settings import get_token


class TushareFinanceFetcher(FinanceFetcher):
    """Tushare 财报数据拉取器"""

    MAX_FINANCE_ANNOUNCEMENT_LAG_DAYS = 124
    fina_indicator_fields=[
        "ts_code",
        "ann_date",
        "end_date",
        "eps",
        "dt_eps",
        "total_revenue_ps",
        "revenue_ps",
        "capital_rese_ps",
        "surplus_rese_ps",
        "undist_profit_ps",
        "extra_item",
        "profit_dedt",
        "gross_margin",
        "ar_turn",
        "op_income",
        "fcff",
        "fcfe",
        "bps",
        "ocfps",
        "retainedps",
        "cfps",
        "ebit_ps",
        "fcff_ps",
        "fcfe_ps",
        "netprofit_margin",
        "grossprofit_margin",
        "cogs_of_sales",
        "expense_of_sales",
        "profit_to_gr",
        "saleexp_to_gr",
        "adminexp_of_gr",
        "finaexp_of_gr",
        "impai_ttm",
        "gc_of_gr",
        "op_of_gr",
        "ebit_of_gr",
        "roe",
        "roe_waa",
        "roe_dt",
        "roa",
        "npta",
        "roic",
        "roe_yearly",
        "roa2_yearly",
        "debt_to_assets",
        "roa_yearly",
        "roa_dp",
        "basic_eps_yoy",
        "dt_eps_yoy",
        "cfps_yoy",
        "netprofit_yoy",
        "dt_netprofit_yoy",
        "tr_yoy",
        "or_yoy",
        "inv_turn",
        "fixed_assets",
        "q_netprofit_yoy",
        "q_netprofit_qoq",
        "op_yoy",
        "ebt_yoy",
        "update_flag"

    ]

    def __init__(self, token: str = None, data_path: str = './data'):
        super().__init__(data_path)
        if token is None:
            token = get_token('tushare')
        self.pro = ts.pro_api(token)

    def _deduplicate_same_day_reissues(self, df: pd.DataFrame) -> pd.DataFrame:
        """Deduplicate same-day reissues, preferring rows with update_flag == 1."""
        duplicate_keys = ['stock_code', 'ann_date', 'end_date']
        if df.empty:
            return df

        duplicated_mask = df.duplicated(subset=duplicate_keys, keep=False)
        if not duplicated_mask.any():
            return df

        duplicate_rows = df.loc[duplicated_mask].copy()
        duplicate_rows['_update_priority'] = pd.to_numeric(
            duplicate_rows.get('update_flag'),
            errors='coerce',
        ).fillna(0)

        for _, group in duplicate_rows.groupby(duplicate_keys, sort=False):
            if len(group) <= 1:
                continue
            if (group['_update_priority'] == 1).any():
                continue
            breakpoint()

        working = df.copy()
        working['_update_priority'] = pd.to_numeric(
            working.get('update_flag'),
            errors='coerce',
        ).fillna(0)
        working['_source_order'] = range(len(working))
        working = working.sort_values(
            ['stock_code', 'ann_date', 'end_date', '_update_priority', '_source_order'],
            ascending=[True, True, True, False, False],
            kind='mergesort',
        )
        working = working.drop_duplicates(subset=duplicate_keys, keep='first')
        return working.drop(columns=['_update_priority', '_source_order']).reset_index(drop=True)

    def fetch_daily(self, date: datetime) -> pd.DataFrame:
        """
        拉取某日公告的财报数据

        Args:
            date: 公告日期

        Returns:
            DataFrame包含当日公告的所有财报
        """
        ann_date = date.strftime('%Y%m%d')

        # 使用 fina_indicator_vip 接口，支持 ann_date 参数查询所有股票
        df = self.pro.fina_indicator_vip(ann_date=ann_date,fields=self.fina_indicator_fields)

        if df is None or df.empty:
            return pd.DataFrame()

        # 字段重命名
        df = df.rename(columns={
            'ts_code': 'stock_code',
        })

        df['ann_date'] = pd.to_datetime(df['ann_date'])
        df['end_date'] = pd.to_datetime(df['end_date'])
        df['update_flag'] = pd.to_numeric(df['update_flag'])

        # 丢弃上市后集中补发的历史财报，避免把过晚披露的数据带入后续分析和回测。
        announcement_lag = (df['ann_date'] - df['end_date']).dt.days
        df = df[announcement_lag <= self.MAX_FINANCE_ANNOUNCEMENT_LAG_DAYS].copy()
        df = df[df['update_flag']==1]

        return df

    def fetch_history(self, start: datetime, end: datetime) -> pd.DataFrame:
        """
        拉取历史财报数据（按发布日期范围）

        Args:
            start: 开始日期（发布日期）
            end: 结束日期（发布日期）

        Returns:
            DataFrame包含发布日期在时间范围内所有财报
        """
        # fina_indicator_vip 的 ann_date 参数只支持单日查询
        # 需要遍历日期范围内的每一天
        all_dfs = []
        current = start

        while current <= end:
            df = self.fetch_daily(current)
            time.sleep(0.12)
            if df is not None and not df.empty:
                all_dfs.append(df)
            print("finance fetch:{}".format(current))
            current = datetime(current.year, current.month, current.day) + pd.Timedelta(days=1)

        if not all_dfs:
            return pd.DataFrame()

        result = pd.concat(all_dfs, ignore_index=True)
        return result

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
