"""Announcement fetcher abstractions."""

from __future__ import annotations

from abc import abstractmethod
from datetime import datetime
from pathlib import Path

import pandas as pd

from .base import BaseFetcher


ANNOUNCEMENT_TYPES = {
    "forecast": "业绩预告",
    "express": "业绩快报",
    "repurchase": "股票回购",
    "holdertrade": "股东增减持",
}


class AnnouncementFetcher(BaseFetcher):
    """Base class for announcement fetchers."""

    DATA_TYPE = "announcement"

    def __init__(self, data_path: str = "./data"):
        super().__init__(data_path)
        self.type_paths = {
            ann_type: {
                "daily": Path(self.data_path) / self.DATA_TYPE / ann_type / "daily",
                "monthly": Path(self.data_path) / self.DATA_TYPE / ann_type / "monthly",
                "yearly": Path(self.data_path) / self.DATA_TYPE / ann_type / "yearly",
            }
            for ann_type in ANNOUNCEMENT_TYPES
        }

    @abstractmethod
    def fetch_by_type(self, ann_type: str, date: datetime) -> pd.DataFrame:
        """Fetch one announcement subtype for one publication date."""

    @abstractmethod
    def fetch_history_by_type(self, ann_type: str, start: datetime, end: datetime) -> pd.DataFrame:
        """Fetch one announcement subtype for a publication-date range."""

    def fetch_daily(self, date: datetime, ann_type: str | None = None) -> pd.DataFrame:
        """Fetch one natural day of announcement data."""
        if ann_type is not None:
            return self.fetch_by_type(ann_type, date)
        return self._combine_types(date=date)

    def fetch_history(
        self,
        start: datetime,
        end: datetime,
        ann_type: str | None = None,
    ) -> pd.DataFrame:
        """Fetch one announcement range as a single dataframe."""
        if ann_type is not None:
            return self.fetch_history_by_type(ann_type, start, end)
        return self._combine_types(start=start, end=end)

    def _combine_types(
        self,
        date: datetime | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> pd.DataFrame:
        frames: list[pd.DataFrame] = []
        for ann_type in ANNOUNCEMENT_TYPES:
            if date is not None:
                frame = self.fetch_by_type(ann_type, date)
            else:
                frame = self.fetch_history_by_type(ann_type, start, end)

            if frame is None or frame.empty:
                continue

            working = frame.copy()
            working["ann_type"] = ann_type
            frames.append(working)

        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True)
