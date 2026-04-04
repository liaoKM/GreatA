"""Benchmark data fetcher abstraction."""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path

import pandas as pd

from ..benchmark_storage import (
    get_benchmark_partition_path,
    normalize_benchmark_frame,
    parse_benchmark_id,
)


class BenchmarkFetcher(ABC):
    """Base class for persisted benchmark fetchers."""

    DATA_TYPE = "benchmark"
    BENCHMARK_IDS: tuple[str, ...] = ()

    def __init__(self, data_path: str = "./data"):
        self.data_path = Path(data_path)

    @abstractmethod
    def fetch_daily(self, date: datetime) -> pd.DataFrame:
        """Fetch one day of benchmark data for all supported benchmarks."""

    @abstractmethod
    def fetch_history(self, start: datetime, end: datetime) -> pd.DataFrame:
        """Fetch a historical benchmark range for all supported benchmarks."""

    def save_daily(self, df: pd.DataFrame, date: datetime) -> dict[str, Path]:
        """Save one benchmark day into per-id daily partition files."""
        if df is None or df.empty:
            return {}

        paths: dict[str, Path] = {}
        for benchmark_id, group in df.groupby("benchmark_id", sort=True):
            normalized = normalize_benchmark_frame(group, str(benchmark_id))
            path = get_benchmark_partition_path(self.data_path, str(benchmark_id), "daily", date.strftime("%Y-%m-%d"))
            path.parent.mkdir(parents=True, exist_ok=True)
            normalized.to_parquet(path, index=False)
            paths[str(benchmark_id)] = path
        return paths

    def save_monthly(self, df: pd.DataFrame, benchmark_id: str, year: int, month: int) -> Path:
        """Save one benchmark month into the monthly partition."""
        normalized = normalize_benchmark_frame(df, benchmark_id)
        path = get_benchmark_partition_path(self.data_path, benchmark_id, "monthly", f"{year:04d}-{month:02d}")
        path.parent.mkdir(parents=True, exist_ok=True)
        normalized.to_parquet(path, index=False)
        return path

    def save_yearly(self, df: pd.DataFrame, benchmark_id: str, year: int) -> Path:
        """Save one benchmark year into the yearly partition."""
        normalized = normalize_benchmark_frame(df, benchmark_id)
        path = get_benchmark_partition_path(self.data_path, benchmark_id, "yearly", f"{year:04d}")
        path.parent.mkdir(parents=True, exist_ok=True)
        normalized.to_parquet(path, index=False)
        return path

    @staticmethod
    def raw_id(benchmark_id: str) -> str:
        """Return the raw benchmark code component."""
        return parse_benchmark_id(benchmark_id)[1]
