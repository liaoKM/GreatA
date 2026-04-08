"""Base abstractions for data fetchers."""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path

import pandas as pd


class BaseFetcher(ABC):
    """Base class for fetchers that return one dataframe per requested range."""

    DATA_TYPE: str | None = None

    def __init__(self, data_path: str = "./data"):
        self.data_path = Path(data_path)

    def fetch_daily(self, date: datetime) -> pd.DataFrame:
        """Convenience wrapper around ``fetch_history(date, date)``."""
        return self.fetch_history(date, date)

    @abstractmethod
    def fetch_history(self, start: datetime, end: datetime) -> pd.DataFrame:
        """Fetch one full date range and return it as a dataframe."""
