"""Registry for persisted custom benchmark definitions."""

from __future__ import annotations

from collections.abc import Callable

import pandas as pd

from data_service.benchmark_storage import write_partitioned_benchmark_range

from ..report.result import BacktestResult
from ..strategy.base import BaseStrategy
from .config import CustomIndexConfig
from .engine import CustomIndexEngine
from .finance_growth import FinanceGrowthIndexStrategy


REGISTERED_CUSTOM_BENCHMARKS: dict[str, Callable[[], BaseStrategy]] = {
    "custom:finance_growth": FinanceGrowthIndexStrategy,
}


def get_registered_custom_strategy_factory(benchmark_id: str) -> Callable[[], BaseStrategy]:
    """Return the strategy factory registered for one persisted custom benchmark id."""
    try:
        return REGISTERED_CUSTOM_BENCHMARKS[str(benchmark_id)]
    except KeyError as exc:
        raise ValueError(f"Unsupported custom benchmark id: {benchmark_id}") from exc


def build_registered_custom_benchmark(
    benchmark_id: str,
    start: pd.Timestamp | str,
    end: pd.Timestamp | str,
    data_path: str = "./data",
    initial_level: float = 1000.0,
    warmup_bars: int = 60,
) -> tuple[BacktestResult, list[str]]:
    """Compute one registered custom benchmark and persist its index curve."""
    strategy_factory = get_registered_custom_strategy_factory(benchmark_id)
    config = CustomIndexConfig(
        start=start,
        end=end,
        data_path=data_path,
        initial_level=initial_level,
        warmup_bars=warmup_bars,
    )
    result = CustomIndexEngine(config, strategy_factory()).run()
    frame = pd.DataFrame(
        {
            "benchmark_id": str(benchmark_id),
            "trade_date": pd.DatetimeIndex(result.equity_curve.index).normalize(),
            "close": result.equity_curve.astype("float64").values,
        }
    )
    written = write_partitioned_benchmark_range(
        data_path=data_path,
        benchmark_id=str(benchmark_id),
        frame=frame,
        start=config.start,
        end=config.end,
        overwrite=True,
    )
    return result, [str(path) for path in written]
