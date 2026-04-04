"""Benchmark provider backed by the custom index engine."""

from __future__ import annotations

from collections.abc import Callable

import pandas as pd

from ..report.benchmark import BenchmarkProvider
from ..report.result import BacktestResult
from ..strategy.base import BaseStrategy
from .config import CustomIndexConfig
from .engine import CustomIndexEngine


class CustomIndexBenchmarkProvider(BenchmarkProvider):
    """Benchmark provider that lazily computes and caches a custom index series."""

    def __init__(
        self,
        config: CustomIndexConfig,
        strategy_factory: Callable[[], BaseStrategy],
    ):
        self.config = config
        self.strategy_factory = strategy_factory
        self._cached_result: BacktestResult | None = None

    def get_series(self, sessions: pd.DatetimeIndex) -> pd.Series:
        """Return an aligned custom benchmark series."""
        result = self.get_result()
        return result.equity_curve.reindex(sessions).dropna()

    def get_result(self) -> BacktestResult:
        """Return the cached custom index result, computing it on first access."""
        if self._cached_result is None:
            strategy = self.strategy_factory()
            self._cached_result = CustomIndexEngine(self.config, strategy).run()
        return self._cached_result
