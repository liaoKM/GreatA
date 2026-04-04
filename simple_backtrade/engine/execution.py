"""Execution models for the v2 backtest framework."""

from __future__ import annotations

from abc import ABC, abstractmethod

import pandas as pd

from ..data.portal import DataPortal


class ExecutionModel(ABC):
    """Abstract execution model reserved for future extensions."""

    @abstractmethod
    def get_fill_prices(
        self,
        portal: DataPortal,
        session_date: pd.Timestamp | str,
        stock_codes: list[str] | pd.Index,
    ) -> pd.Series:
        """Return tradable fill prices for one session."""


class CloseExecutionModel(ExecutionModel):
    """Close-price execution using adjusted close prices."""

    def get_fill_prices(
        self,
        portal: DataPortal,
        session_date: pd.Timestamp | str,
        stock_codes: list[str] | pd.Index,
    ) -> pd.Series:
        return portal.adjusted_close(session_date, stock_codes=stock_codes)
