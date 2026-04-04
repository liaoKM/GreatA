"""Market data aggregation."""

from __future__ import annotations

from pathlib import Path

from .base import BaseAggregator


class MarketAggregator(BaseAggregator):
    """Aggregate market daily parquet files."""

    DATA_TYPE = "market"

    def aggregate_daily_to_monthly(self, year: int, month: int) -> Path | None:
        daily_files = self._load_daily_files(year, month)
        if not daily_files:
            return None

        merged_df = self._concat_parquet_files(daily_files)
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
        filepath = self.yearly_path / f"{year}.parquet"
        filepath.parent.mkdir(parents=True, exist_ok=True)
        merged_df.to_parquet(filepath, index=False)
        self._delete_monthly_files(year)
        return filepath
