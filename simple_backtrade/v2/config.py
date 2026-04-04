"""Configuration for the v2 backtest framework."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

import pandas as pd

from .models import BarFrequency, PriceAdjustment, TradeTiming
from .report.benchmark import BenchmarkProvider


def _normalize_date(value: date | datetime | str) -> pd.Timestamp:
    if isinstance(value, pd.Timestamp):
        return value.normalize()
    if isinstance(value, datetime):
        return pd.Timestamp(value.date())
    if isinstance(value, date):
        return pd.Timestamp(value)
    return pd.Timestamp(value).normalize()


@dataclass
class BacktestConfig:
    """Top-level backtest configuration."""

    start: date | datetime | str
    end: date | datetime | str
    data_path: str | Path = "./data"
    initial_cash: float = 1_000_000.0
    bar_frequency: BarFrequency = BarFrequency.DAILY
    trade_timing: TradeTiming = TradeTiming.CLOSE
    price_adjustment: PriceAdjustment = PriceAdjustment.HFQ
    warmup_bars: int = 60
    benchmark: pd.Series | BenchmarkProvider | None = None

    def __post_init__(self) -> None:
        self.start = _normalize_date(self.start)
        self.end = _normalize_date(self.end)
        self.data_path = Path(self.data_path)
        self.initial_cash = float(self.initial_cash)
        self.warmup_bars = int(self.warmup_bars)

        if self.start > self.end:
            raise ValueError("BacktestConfig.start must be on or before end")
        if self.initial_cash <= 0:
            raise ValueError("BacktestConfig.initial_cash must be positive")
        if self.warmup_bars < 0:
            raise ValueError("BacktestConfig.warmup_bars must be non-negative")
        if self.bar_frequency != BarFrequency.DAILY:
            raise NotImplementedError("Only daily bars are supported in v2")
        if self.trade_timing != TradeTiming.CLOSE:
            raise NotImplementedError("Only close execution is supported in v2")
        if self.price_adjustment != PriceAdjustment.HFQ:
            raise NotImplementedError("Only HFQ price adjustment is supported in v2")
