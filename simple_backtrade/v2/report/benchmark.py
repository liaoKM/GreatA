"""Benchmark provider abstractions."""

from __future__ import annotations

from abc import ABC, abstractmethod

import pandas as pd


class BenchmarkProvider(ABC):
    """Abstract benchmark data provider."""

    @abstractmethod
    def get_series(self, sessions: pd.DatetimeIndex) -> pd.Series:
        """Return an aligned benchmark equity or price series."""
