"""Strategy context for the v2 backtest framework."""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from ..config import BacktestConfig
from ..data.portal import DataPortal


@dataclass
class StrategyContext:
    """Runtime context passed to strategies."""

    config: BacktestConfig
    data: DataPortal
    broker: "PortfolioLedger"
    session_date: pd.Timestamp | None = None

    def set_session(self, session_date: pd.Timestamp) -> None:
        self.session_date = pd.Timestamp(session_date).normalize()

    @property
    def cash(self) -> float:
        return self.broker.cash

    @property
    def equity(self) -> float:
        if self.session_date is None:
            return self.broker.cash
        return self.broker.equity(self.session_date)

    @property
    def positions(self) -> pd.Series:
        return self.broker.positions.copy()


from .broker import PortfolioLedger  # noqa: E402  pylint: disable=wrong-import-position
