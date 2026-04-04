"""Main backtest loop for the v2 backtest framework."""

from __future__ import annotations

from dataclasses import asdict

import pandas as pd

from ..config import BacktestConfig
from ..data.portal import DataPortal
from ..models import PublishedEvent, TargetPortfolio
from ..report.result import BacktestResult, build_summary
from ..strategy.base import BaseStrategy
from .broker import Broker, PortfolioLedger, TradeRecord
from .context import StrategyContext


class BacktestEngine:
    """Main entry point for one backtest run."""

    def __init__(
        self,
        config: BacktestConfig,
        strategy: BaseStrategy,
        data_portal: DataPortal | None = None,
        portfolio: PortfolioLedger | None = None,
        event_sources: tuple[str, ...] | None = None,
        announcement_subtypes: tuple[str, ...] | None = None,
    ):
        self.config = config
        self.strategy = strategy
        self.data_portal = data_portal or DataPortal(config)
        self.broker = portfolio or Broker(self.data_portal, initial_cash=config.initial_cash)
        self.context = StrategyContext(config=config, data=self.data_portal, broker=self.broker)
        self.event_sources = event_sources
        self.announcement_subtypes = announcement_subtypes

    def run(self) -> BacktestResult:
        """Execute the backtest and return structured results."""
        self.strategy.on_initialize(self.context)

        equity_points: list[tuple[pd.Timestamp, float]] = []
        position_rows: list[dict[str, object]] = []
        trade_rows: list[dict[str, object]] = []
        event_rows: list[dict[str, object]] = []

        previous_session_date: pd.Timestamp | None = None
        for session_date in self.data_portal.sessions:
            self.context.set_session(session_date)
            events = self._get_session_events(session_date, previous_session_date)
            trades, consumed_events = self._process_session(session_date, events)
            equity = self.broker.equity(session_date)
            equity_points.append((session_date, equity))

            for stock_code, quantity in self.broker.positions.items():
                position_rows.append(
                    {
                        "session_date": session_date,
                        "stock_code": str(stock_code),
                        "quantity": float(quantity),
                    }
                )

            for trade in trades:
                trade_rows.append(asdict(trade))

            for event in consumed_events:
                event_rows.append(self._event_to_record(event, session_date))

            previous_session_date = session_date

        equity_curve = pd.Series(
            [value for _, value in equity_points],
            index=pd.DatetimeIndex([index for index, _ in equity_points]),
            name="equity",
            dtype="float64",
        )
        positions = pd.DataFrame(position_rows)
        trades = pd.DataFrame(trade_rows)
        event_log = pd.DataFrame(event_rows)
        benchmark = self._resolve_benchmark(equity_curve.index)
        summary = build_summary(equity_curve, benchmark=benchmark)

        return BacktestResult(
            equity_curve=equity_curve,
            positions=positions,
            trades=trades,
            event_log=event_log,
            summary=summary,
            benchmark=benchmark,
        )

    def _get_session_events(
        self,
        session_date: pd.Timestamp,
        previous_session_date: pd.Timestamp | None,
    ) -> list[PublishedEvent]:
        return self.data_portal.events.for_session(
            session_date,
            previous_session_date=previous_session_date,
            sources=self.event_sources,
            announcement_subtypes=self.announcement_subtypes,
        )

    def _process_session(
        self,
        session_date: pd.Timestamp,
        events: list[PublishedEvent],
    ) -> tuple[list[TradeRecord], list[PublishedEvent]]:
        self.strategy.on_events(self.context, events)
        target = self.strategy.on_bar_close(self.context)
        if target is None:
            return [], events
        return self.broker.rebalance(session_date, target), events

    def _resolve_benchmark(self, sessions: pd.DatetimeIndex) -> pd.Series | None:
        benchmark = getattr(self.config, "benchmark", None)
        if benchmark is None:
            return None
        if hasattr(benchmark, "get_series"):
            return benchmark.get_series(sessions)
        series = pd.Series(benchmark).copy()
        series.index = pd.to_datetime(series.index).normalize()
        return series.reindex(sessions).dropna()

    @staticmethod
    def _event_to_record(event: PublishedEvent, session_date: pd.Timestamp) -> dict[str, object]:
        return {
            "source": event.source,
            "subtype": event.subtype,
            "stock_code": event.stock_code,
            "publish_date": pd.Timestamp(event.publish_date),
            "session_date": pd.Timestamp(session_date),
            "payload": event.payload,
        }


def run_backtest(config: BacktestConfig, strategy: BaseStrategy) -> BacktestResult:
    """Convenience helper for one-off backtest runs."""
    return BacktestEngine(config, strategy).run()
