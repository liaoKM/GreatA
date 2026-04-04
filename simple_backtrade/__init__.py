"""Backtest framework v2."""

from .benchmark import (
    CustomIndexBenchmarkProvider,
    CustomIndexConfig,
    CustomIndexEngine,
    FinanceGrowthIndexStrategy,
    REGISTERED_CUSTOM_BENCHMARKS,
    build_registered_custom_benchmark,
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
    "REGISTERED_CUSTOM_BENCHMARKS",
    "EmptyStrategy",
    "ExposureBucket",
    "InstrumentSpec",
    "InstrumentType",
    "PositionSide",
    "PriceAdjustment",
    "PublishedEvent",
    "TargetPortfolio",
    "TradeTiming",
    "build_registered_custom_benchmark",
    "run_custom_index",
    "run_backtest",
]
