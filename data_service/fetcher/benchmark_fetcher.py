"""Benchmark fetcher abstraction."""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path

import pandas as pd

from ..benchmark_storage import parse_benchmark_id


class BenchmarkFetcher(ABC):
    """Base class for persisted benchmark fetchers."""

    DATA_TYPE = "benchmark"
    BENCHMARK_IDS: tuple[str, ...] = ()

    def __init__(self, data_path: str = "./data"):
        self.data_path = Path(data_path)

    @abstractmethod
    def fetch_daily(self, date: datetime) -> pd.DataFrame:
        """Fetch one day of benchmark data for all supported benchmark ids."""

    @abstractmethod
    def fetch_history(self, start: datetime, end: datetime) -> pd.DataFrame:
        """Fetch one date range of benchmark data for all supported benchmark ids."""

    @staticmethod
    def raw_id(benchmark_id: str) -> str:
        """Return the raw benchmark code component."""
        return parse_benchmark_id(benchmark_id)[1]
