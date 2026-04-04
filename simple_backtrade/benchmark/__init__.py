"""Custom benchmark framework for v2."""

from .config import CustomIndexConfig
from .engine import CustomIndexEngine, run_custom_index
from .finance_growth import FinanceGrowthIndexStrategy
from .provider import CustomIndexBenchmarkProvider
from .registry import REGISTERED_CUSTOM_BENCHMARKS, build_registered_custom_benchmark

__all__ = [
    "CustomIndexBenchmarkProvider",
    "CustomIndexConfig",
    "CustomIndexEngine",
    "FinanceGrowthIndexStrategy",
    "REGISTERED_CUSTOM_BENCHMARKS",
    "build_registered_custom_benchmark",
    "run_custom_index",
]
