"""Empty strategy for smoke testing the v2 framework."""

from __future__ import annotations

from ..models import TargetPortfolio
from .base import BaseStrategy


class EmptyStrategy(BaseStrategy):
    """A do-nothing strategy that stays in cash."""

    def on_bar_close(self, context) -> TargetPortfolio:
        return TargetPortfolio.empty()
