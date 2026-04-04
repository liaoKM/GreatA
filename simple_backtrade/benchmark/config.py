"""Configuration for custom benchmark calculation."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

import pandas as pd

from data_service.fetcher.announcement_fetcher import ANNOUNCEMENT_TYPES

from ..models import PriceAdjustment


def _normalize_date(value: date | datetime | str) -> pd.Timestamp:
    if isinstance(value, pd.Timestamp):
        return value.normalize()
    if isinstance(value, datetime):
        return pd.Timestamp(value.date())
    if isinstance(value, date):
        return pd.Timestamp(value)
    return pd.Timestamp(value).normalize()


@dataclass
class CustomIndexConfig:
    """Top-level configuration for one custom index run."""

    start: date | datetime | str
    end: date | datetime | str
    data_path: str | Path = "./data"
    initial_level: float = 1000.0
    warmup_bars: int = 60
    price_adjustment: PriceAdjustment = PriceAdjustment.HFQ
    event_sources: tuple[str, ...] = ("finance",)
    announcement_subtypes: tuple[str, ...] = ()
    benchmark: str | pd.Series | None = None

    def __post_init__(self) -> None:
        self.start = _normalize_date(self.start)
        self.end = _normalize_date(self.end)
        self.data_path = Path(self.data_path)
        self.initial_level = float(self.initial_level)
        self.warmup_bars = int(self.warmup_bars)
        self.event_sources = tuple(str(source) for source in self.event_sources)
        self.announcement_subtypes = tuple(str(subtype) for subtype in self.announcement_subtypes)

        if self.start > self.end:
            raise ValueError("CustomIndexConfig.start must be on or before end")
        if self.initial_level <= 0:
            raise ValueError("CustomIndexConfig.initial_level must be positive")
        if self.warmup_bars < 0:
            raise ValueError("CustomIndexConfig.warmup_bars must be non-negative")
        if self.price_adjustment != PriceAdjustment.HFQ:
            raise NotImplementedError("Only HFQ price adjustment is supported in custom index v1")
        invalid_sources = set(self.event_sources) - {"finance", "announcement"}
        if invalid_sources:
            raise ValueError(f"Unsupported event sources: {sorted(invalid_sources)}")
        if "announcement" not in self.event_sources and self.announcement_subtypes:
            raise ValueError("announcement_subtypes requires event_sources to include 'announcement'")
        invalid_subtypes = set(self.announcement_subtypes) - set(ANNOUNCEMENT_TYPES)
        if invalid_subtypes:
            raise ValueError(f"Unsupported announcement subtypes: {sorted(invalid_subtypes)}")
