"""市场数据整合器"""
from pathlib import Path
import pandas as pd
from .base import BaseAggregator


class MarketAggregator(BaseAggregator):
    """市场数据整合器"""

    DATA_TYPE = 'market'

    def aggregate_daily_to_monthly(self, year: int, month: int) -> Path:
        """
        将当月所有日线数据合并为月文件

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

        # 保存为monthly文件
        filename = f"{year}-{month:02d}.parquet"
        filepath = self.monthly_path / filename
        merged_df.to_parquet(filepath, index=False)

        # 删除daily文件
        self._delete_daily_files(year, month)

        return filepath

    def aggregate_monthly_to_yearly(self, year: int) -> Path:
        """
        将当年所有月线数据合并为年文件

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

        # 保存为yearly文件
        filename = f"{year}.parquet"
        filepath = self.yearly_path / filename
        merged_df.to_parquet(filepath, index=False)

        # 删除monthly文件
        self._delete_monthly_files(year)

        return filepath
