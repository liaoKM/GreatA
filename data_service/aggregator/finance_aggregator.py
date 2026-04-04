"""Finance data aggregation."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from .base import BaseAggregator


class FinanceAggregator(BaseAggregator):
    """Aggregate finance daily parquet files."""

    DATA_TYPE = "finance"

    def aggregate_daily_to_monthly(self, year: int, month: int) -> Path:
        daily_files = self._load_daily_files(year, month)
        merged_df = self._concat_parquet_files(daily_files)
        if not merged_df.empty:
            merged_df = self._deduplicate(merged_df)

        filepath = self.monthly_path / f"{year}-{month:02d}.parquet"
        filepath.parent.mkdir(parents=True, exist_ok=True)
        merged_df.to_parquet(filepath, index=False)
        self._delete_daily_files(year, month)
        return filepath

    def aggregate_monthly_to_yearly(self, year: int) -> Path | None:
        monthly_files = self._load_monthly_files(year)
        if not monthly_files:
            return None

        merged_df = self._concat_parquet_files(monthly_files)
        if not merged_df.empty:
            merged_df = self._deduplicate(merged_df)

        filepath = self.yearly_path / f"{year}.parquet"
        filepath.parent.mkdir(parents=True, exist_ok=True)
        merged_df.to_parquet(filepath, index=False)
        self._delete_monthly_files(year)
        return filepath

    def _deduplicate(self, df: pd.DataFrame) -> pd.DataFrame:
        return self._deduplicate_by_latest(df, ["stock_code", "end_date"])
