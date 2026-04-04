"""Announcement data aggregation."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from .base import BaseAggregator
from ..fetcher.announcement_fetcher import ANNOUNCEMENT_TYPES


class AnnouncementAggregator(BaseAggregator):
    """Aggregate announcement parquet files by subtype."""

    DATA_TYPE = "announcement"

    def __init__(self, data_path: str = "./data"):
        self.data_path = Path(data_path)
        self.type_paths: dict[str, dict[str, Path]] = {}
        for ann_type in ANNOUNCEMENT_TYPES:
            type_path = self.data_path / self.DATA_TYPE / ann_type
            self.type_paths[ann_type] = {
                "daily": type_path / "daily",
                "monthly": type_path / "monthly",
                "yearly": type_path / "yearly",
            }
        self.daily_path = self.data_path / self.DATA_TYPE / "_daily"
        self.monthly_path = self.data_path / self.DATA_TYPE / "_monthly"
        self.yearly_path = self.data_path / self.DATA_TYPE / "_yearly"

    def _load_daily_files(self, year: int, month: int) -> list[Path]:
        all_files: list[Path] = []
        for ann_type in ANNOUNCEMENT_TYPES:
            pattern = f"{year}-{month:02d}-*.parquet"
            all_files.extend(self.type_paths[ann_type]["daily"].glob(pattern))
        return sorted(all_files)

    def _load_monthly_files(self, year: int) -> list[Path]:
        all_files: list[Path] = []
        for ann_type in ANNOUNCEMENT_TYPES:
            pattern = f"{year}-*.parquet"
            all_files.extend(self.type_paths[ann_type]["monthly"].glob(pattern))
        return sorted(all_files)

    def monthly_file_exists(self, year: int, month: int) -> bool:
        return all(
            (self.type_paths[ann_type]["monthly"] / f"{year}-{month:02d}.parquet").exists()
            for ann_type in ANNOUNCEMENT_TYPES
        )

    def yearly_file_exists(self, year: int) -> bool:
        return all(
            (self.type_paths[ann_type]["yearly"] / f"{year}.parquet").exists()
            for ann_type in ANNOUNCEMENT_TYPES
        )

    def aggregate_daily_to_monthly(
        self,
        year: int,
        month: int,
        ann_type: str | None = None,
    ) -> dict[str, Path] | Path:
        if ann_type is not None:
            return self._aggregate_daily_to_monthly_by_type(ann_type, year, month)

        result: dict[str, Path] = {}
        for current_type in ANNOUNCEMENT_TYPES:
            result[current_type] = self._aggregate_daily_to_monthly_by_type(current_type, year, month)
        return result

    def _aggregate_daily_to_monthly_by_type(self, ann_type: str, year: int, month: int) -> Path:
        paths = self.type_paths[ann_type]
        pattern = f"{year}-{month:02d}-*.parquet"
        daily_files = sorted(paths["daily"].glob(pattern))

        merged_df = self._concat_parquet_files(daily_files)
        merged_df = self._deduplicate(ann_type, merged_df)

        filepath = paths["monthly"] / f"{year}-{month:02d}.parquet"
        filepath.parent.mkdir(parents=True, exist_ok=True)
        merged_df.to_parquet(filepath, index=False)

        for file_path in daily_files:
            file_path.unlink()

        return filepath

    def aggregate_monthly_to_yearly(
        self,
        year: int,
        ann_type: str | None = None,
    ) -> dict[str, Path] | Path | None:
        if ann_type is not None:
            return self._aggregate_monthly_to_yearly_by_type(ann_type, year)

        result: dict[str, Path] = {}
        for current_type in ANNOUNCEMENT_TYPES:
            yearly_path = self._aggregate_monthly_to_yearly_by_type(current_type, year)
            if yearly_path is not None:
                result[current_type] = yearly_path
        return result

    def _aggregate_monthly_to_yearly_by_type(self, ann_type: str, year: int) -> Path | None:
        paths = self.type_paths[ann_type]
        pattern = f"{year}-*.parquet"
        monthly_files = sorted(paths["monthly"].glob(pattern))
        if not monthly_files:
            return None

        merged_df = self._concat_parquet_files(monthly_files)
        merged_df = self._deduplicate(ann_type, merged_df)

        filepath = paths["yearly"] / f"{year}.parquet"
        filepath.parent.mkdir(parents=True, exist_ok=True)
        merged_df.to_parquet(filepath, index=False)

        for file_path in monthly_files:
            file_path.unlink()

        return filepath

    def _deduplicate(self, ann_type: str, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df
        if ann_type in {"forecast", "express"}:
            return self._deduplicate_by_latest(df, ["stock_code", "end_date"])
        if ann_type in {"repurchase", "holdertrade"}:
            return self._drop_exact_duplicates(df)
        return df
