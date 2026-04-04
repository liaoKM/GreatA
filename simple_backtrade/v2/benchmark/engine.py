"""Event-driven custom index engine."""

from __future__ import annotations

from ..data.portal import DataPortal
from ..engine.broker import IndexPortfolio
from ..engine.runner import BacktestEngine
from ..report.result import BacktestResult
from ..strategy.base import BaseStrategy
from .config import CustomIndexConfig


class CustomIndexEngine(BacktestEngine):
    """Custom benchmark engine using an event-driven fractional-share portfolio."""

    def __init__(
        self,
        config: CustomIndexConfig,
        strategy: BaseStrategy,
        data_portal: DataPortal | None = None,
    ):
        portal = data_portal or DataPortal(config)
        portfolio = IndexPortfolio(portal, initial_level=config.initial_level)
        super().__init__(
            config=config,
            strategy=strategy,
            data_portal=portal,
            portfolio=portfolio,
            rebalance_policy="event_driven",
            event_sources=config.event_sources,
            announcement_subtypes=config.announcement_subtypes or None,
        )


def run_custom_index(config: CustomIndexConfig, strategy: BaseStrategy) -> BacktestResult:
    """Convenience helper for one-off custom index runs."""
    return CustomIndexEngine(config, strategy).run()
