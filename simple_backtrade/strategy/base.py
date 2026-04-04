"""Base strategy interface for the v2 backtest framework."""

from __future__ import annotations

from abc import ABC, abstractmethod

from ..models import PublishedEvent, TargetPortfolio


class BaseStrategy(ABC):
    """Base class for daily close strategies."""

    def on_initialize(self, context) -> None:
        """Called once before the first trading session."""

    def on_events(self, context, events: list[PublishedEvent]) -> None:
        """Called before bar-close logic for each session."""

    @abstractmethod
    def on_bar_close(self, context) -> TargetPortfolio:
        """Return the target portfolio for the current session."""
