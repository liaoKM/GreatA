"""财报数据整合器"""
from pathlib import Path
import pandas as pd
from .base import BaseAggregator


class FinanceAggregator(BaseAggregator):
    """财报数据整合器"""

    DATA_TYPE = 'finance'

    def aggregate_daily_to_monthly(self, year: int, month: int) -> Path:
        """
        将当月所有财报数据合并为月文件

        Args:
            year: 年份
            month: 月份

        Returns:
            生成的monthly文件路径
        """
        daily_files = self._load_daily_files(year, month)
        if not daily_files:
            return None

        # 合并所有daily文件
        dfs = [pd.read_parquet(f) for f in daily_files]
        merged_df = pd.concat(dfs, ignore_index=True)

        # 去重：同一股票同一报告期保留最新公告
        if not merged_df.empty:
            merged_df = self._deduplicate(merged_df)

        # 保存为monthly文件
        filename = f"{year}-{month:02d}.parquet"
        filepath = self.monthly_path / filename
        merged_df.to_parquet(filepath, index=False)

        # 删除daily文件
        self._delete_daily_files(year, month)

        return filepath

    def aggregate_monthly_to_yearly(self, year: int) -> Path:
        """
        将当年所有财报数据合并为年文件

        Args:
            year: 年份

        Returns:
            生成的yearly文件路径
        """
        monthly_files = self._load_monthly_files(year)
        if not monthly_files:
            return None

        # 合并所有monthly文件
        dfs = [pd.read_parquet(f) for f in monthly_files]
        merged_df = pd.concat(dfs, ignore_index=True)

        # 去重
        if not merged_df.empty:
            merged_df = self._deduplicate(merged_df)

        # 保存为yearly文件
        filename = f"{year}.parquet"
        filepath = self.yearly_path / filename
        merged_df.to_parquet(filepath, index=False)

        # 删除monthly文件
        self._delete_monthly_files(year)

        return filepath

    def _deduplicate(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        去重：同一股票同一报告期保留最新公告

        Args:
            df: 原始DataFrame

        Returns:
            去重后的DataFrame
        """
        # TODO: 根据实际字段实现去重逻辑
        # 需要定义 stock_code, report_period, ann_date 等字段
        return df
