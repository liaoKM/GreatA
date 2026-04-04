"""Broker and portfolio state for the v2 backtest framework."""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from ..data.portal import DataPortal
from ..models import InstrumentSpec, InstrumentType, PositionSide, TargetPortfolio
from .execution import CloseExecutionModel, ExecutionModel


@dataclass(frozen=True)
class TradeRecord:
    """One executed trade."""

    session_date: pd.Timestamp
    stock_code: str
    side: str
    quantity: int
    price: float
    notional: float
    instrument_type: str = InstrumentType.EQUITY.value


class Broker:
    """Long-only broker using adjusted close prices for valuation and execution."""

    def __init__(
        self,
        portal: DataPortal,
        initial_cash: float,
        execution_model: ExecutionModel | None = None,
        default_spec: InstrumentSpec | None = None,
    ):
        self.portal = portal
        self.cash = float(initial_cash)
        self.positions = pd.Series(dtype="int64")
        self.execution_model = execution_model or CloseExecutionModel()
        self.default_spec = default_spec or InstrumentSpec()
        if self.default_spec.side != PositionSide.LONG:
            raise NotImplementedError("v2 broker only supports long-only instruments")

    def equity(self, session_date: pd.Timestamp | str) -> float:
        """Mark the portfolio to the latest adjusted close."""
        if self.positions.empty:
            return self.cash
        prices = self.portal.adjusted_close_asof(session_date, stock_codes=self.positions.index)
        if prices.empty:
            return self.cash
        aligned_positions = self.positions.reindex(prices.index, fill_value=0)
        return float(self.cash + (aligned_positions * prices).sum())

    def rebalance(
        self,
        session_date: pd.Timestamp | str,
        target: TargetPortfolio | None,
    ) -> list[TradeRecord]:
        """Rebalance the portfolio to the target weights."""
        session_date = pd.Timestamp(session_date).normalize()
        target = target or TargetPortfolio.empty()
        weights = target.weights
        tradable_codes = pd.Index(sorted(set(self.positions.index.astype(str)).union(weights.index.astype(str))))
        if tradable_codes.empty:
            return []

        fill_prices = self.execution_model.get_fill_prices(self.portal, session_date, tradable_codes)
        tradable_codes = fill_prices.index
        if tradable_codes.empty:
            return []

        current_equity = self.equity(session_date)
        desired_shares = pd.Series(0, index=tradable_codes, dtype="int64")
        for stock_code, target_weight in weights.items():
            if stock_code not in fill_prices.index or target_weight <= 0:
                continue
            price = float(fill_prices.loc[stock_code])
            if price <= 0:
                continue
            target_value = current_equity * float(target_weight)
            lot_size = self.default_spec.lot_size
            desired_lots = int(target_value // (price * lot_size))
            desired_shares.loc[stock_code] = desired_lots * lot_size

        current_shares = self.positions.reindex(tradable_codes, fill_value=0).astype("int64")
        deltas = desired_shares - current_shares
        trades: list[TradeRecord] = []

        for stock_code, delta in deltas[deltas < 0].sort_index().items():
            quantity = int(-delta)
            price = float(fill_prices.loc[stock_code])
            notional = quantity * price
            self.cash += notional
            current_shares.loc[stock_code] = int(current_shares.loc[stock_code] - quantity)
            trades.append(TradeRecord(session_date, stock_code, "sell", quantity, price, notional))

        for stock_code, delta in deltas[deltas > 0].sort_index().items():
            quantity = int(delta)
            price = float(fill_prices.loc[stock_code])
            lot_size = self.default_spec.lot_size
            affordable_lots = int(self.cash // (price * lot_size))
            if affordable_lots <= 0:
                continue
            affordable_quantity = min(quantity, affordable_lots * lot_size)
            if affordable_quantity <= 0:
                continue
            notional = affordable_quantity * price
            self.cash -= notional
            current_shares.loc[stock_code] = int(current_shares.loc[stock_code] + affordable_quantity)
            trades.append(TradeRecord(session_date, stock_code, "buy", affordable_quantity, price, notional))

        self.positions = current_shares[current_shares > 0].sort_index()
        return trades
