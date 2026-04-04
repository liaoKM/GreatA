"""Backtest framework v2."""

from .benchmark import (
    CustomIndexBenchmarkProvider,
    CustomIndexConfig,
    CustomIndexEngine,
    FinanceGrowthIndexStrategy,
    run_custom_index,
)
from .config import BacktestConfig
from .engine.runner import BacktestEngine, run_backtest
from .models import (
    BarFrequency,
    ExposureBucket,
    InstrumentSpec,
    InstrumentType,
    PositionSide,
    PriceAdjustment,
    PublishedEvent,
    TargetPortfolio,
    TradeTiming,
)
from .report.result import BacktestResult
from .strategy.base import BaseStrategy
from .strategy.empty import EmptyStrategy

__all__ = [
    "BacktestConfig",
    "BacktestEngine",
    "BacktestResult",
    "BaseStrategy",
    "BarFrequency",
    "CustomIndexBenchmarkProvider",
    "CustomIndexConfig",
    "CustomIndexEngine",
    "FinanceGrowthIndexStrategy",
    "EmptyStrategy",
    "ExposureBucket",
    "InstrumentSpec",
    "InstrumentType",
    "PositionSide",
    "PriceAdjustment",
    "PublishedEvent",
    "TargetPortfolio",
    "TradeTiming",
    "run_custom_index",
    "run_backtest",
]
