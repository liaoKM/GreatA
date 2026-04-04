"""Base classes for parquet aggregators."""

from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path

import pandas as pd


class BaseAggregator(ABC):
    """Shared helpers for daily/monthly/yearly aggregation."""

    DATA_TYPE: str | None = None

    def __init__(self, data_path: str = "./data"):
        self.data_path = Path(data_path)
        self.daily_path = self.data_path / self.DATA_TYPE / "daily"
        self.monthly_path = self.data_path / self.DATA_TYPE / "monthly"
        self.yearly_path = self.data_path / self.DATA_TYPE / "yearly"

    @abstractmethod
    def aggregate_daily_to_monthly(self, year: int, month: int):
        """Aggregate one month of daily data into one monthly file."""

    @abstractmethod
    def aggregate_monthly_to_yearly(self, year: int):
        """Aggregate one year of monthly data into one yearly file."""

    def _load_daily_files(self, year: int, month: int) -> list[Path]:
        pattern = f"{year}-{month:02d}-*.parquet"
        return sorted(self.daily_path.glob(pattern))

    def _load_monthly_files(self, year: int) -> list[Path]:
        pattern = f"{year}-*.parquet"
        return sorted(self.monthly_path.glob(pattern))

    def monthly_file_exists(self, year: int, month: int) -> bool:
        return (self.monthly_path / f"{year}-{month:02d}.parquet").exists()

    def yearly_file_exists(self, year: int) -> bool:
        return (self.yearly_path / f"{year}.parquet").exists()

    def _delete_daily_files(self, year: int, month: int) -> int:
        files = self._load_daily_files(year, month)
        for file_path in files:
            file_path.unlink()
        return len(files)

    def _delete_monthly_files(self, year: int) -> int:
        files = self._load_monthly_files(year)
        for file_path in files:
            file_path.unlink()
        return len(files)

    @staticmethod
    def _concat_parquet_files(files: list[Path]) -> pd.DataFrame:
        if not files:
            return pd.DataFrame()
        dataframes = [pd.read_parquet(file_path) for file_path in files]
        return pd.concat(dataframes, ignore_index=True) if dataframes else pd.DataFrame()

    @staticmethod
    def _drop_exact_duplicates(df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df
        return df.drop_duplicates().reset_index(drop=True)

    @staticmethod
    def _deduplicate_by_latest(
        df: pd.DataFrame,
        subset: list[str],
        date_column: str = "ann_date",
        flag_column: str = "update_flag",
    ) -> pd.DataFrame:
        if df.empty:
            return df
        if any(column not in df.columns for column in subset):
            return BaseAggregator._drop_exact_duplicates(df)

        working = df.copy()
        helper_columns: list[str] = []
        sort_columns = list(subset)
        ascending = [True] * len(subset)

        if date_column in working.columns:
            working["_sort_date"] = pd.to_datetime(working[date_column], errors="coerce")
            helper_columns.append("_sort_date")
            sort_columns.append("_sort_date")
            ascending.append(False)

        if flag_column in working.columns:
            working["_sort_flag"] = pd.to_numeric(working[flag_column], errors="coerce").fillna(0)
            helper_columns.append("_sort_flag")
            sort_columns.append("_sort_flag")
            ascending.append(False)

        working["_sort_index"] = range(len(working))
        helper_columns.append("_sort_index")
        sort_columns.append("_sort_index")
        ascending.append(False)

        working = working.sort_values(sort_columns, ascending=ascending, kind="mergesort")
        working = working.drop_duplicates(subset=subset, keep="first")
        return working.drop(columns=helper_columns).reset_index(drop=True)
