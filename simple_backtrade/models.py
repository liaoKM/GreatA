"""Shared enums and dataclasses for the v2 backtest framework."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from enum import Enum
from typing import Any, Iterable

import pandas as pd


class BarFrequency(str, Enum):
    """Supported bar frequencies."""

    DAILY = "1d"


class TradeTiming(str, Enum):
    """Supported execution timings."""

    CLOSE = "close"


class PriceAdjustment(str, Enum):
    """Supported price adjustment modes."""

    HFQ = "hfq"


class InstrumentType(str, Enum):
    """Supported instrument types."""

    EQUITY = "equity"


class PositionSide(str, Enum):
    """Supported position sides."""

    LONG = "long"
    SHORT = "short"


class ExposureBucket(str, Enum):
    """Reserved exposure buckets for future hedge support."""

    CASH = "cash"
    EQUITY_LONG = "equity_long"
    EQUITY_SHORT = "equity_short"


@dataclass(frozen=True)
class InstrumentSpec:
    """Instrument metadata used by the broker."""

    instrument_type: InstrumentType = InstrumentType.EQUITY
    lot_size: int = 100
    side: PositionSide = PositionSide.LONG


@dataclass(frozen=True)
class PublishedEvent:
    """One finance or announcement event exposed to strategies."""

    source: str
    subtype: str | None
    stock_code: str
    publish_date: date
    payload: dict[str, Any]


@dataclass
class TargetPortfolio:
    """Long-only target portfolio described by target weights."""

    weights: pd.Series = field(default_factory=lambda: pd.Series(dtype="float64"))

    def __post_init__(self) -> None:
        if not isinstance(self.weights, pd.Series):
            self.weights = pd.Series(self.weights, dtype="float64")
        else:
            self.weights = self.weights.astype("float64", copy=False)

        if self.weights.empty:
            self.weights = pd.Series(dtype="float64")
            self.weights.index = pd.Index([], dtype="object", name="stock_code")
            return

        cleaned = self.weights.dropna()
        cleaned.index = cleaned.index.astype(str)
        cleaned = cleaned[cleaned != 0].sort_index()
        if (cleaned < 0).any():
            raise ValueError("TargetPortfolio only supports non-negative weights")
        total_weight = float(cleaned.sum())
        if total_weight > 1.0 + 1e-9:
            raise ValueError("TargetPortfolio weights must sum to 1 or less")
        if cleaned.isin([float("inf"), float("-inf")]).any():
            raise ValueError("TargetPortfolio weights must be finite")
        cleaned.index.name = "stock_code"
        self.weights = cleaned

    @classmethod
    def empty(cls) -> "TargetPortfolio":
        """Create an empty target portfolio."""
        return cls()

    @classmethod
    def equal_weight(
        cls,
        stock_codes: Iterable[str],
        total_weight: float = 1.0,
    ) -> "TargetPortfolio":
        """Create an equal-weight long-only target portfolio."""
        if total_weight < 0 or total_weight > 1:
            raise ValueError("total_weight must be between 0 and 1")
        unique_codes = sorted({str(stock_code) for stock_code in stock_codes})
        if not unique_codes:
            return cls.empty()
        weight = total_weight / len(unique_codes)
        return cls(pd.Series(weight, index=unique_codes, dtype="float64"))
