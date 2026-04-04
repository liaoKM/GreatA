"""Structured backtest results and summary metrics."""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd


@dataclass
class BacktestResult:
    """Structured output for one backtest run."""

    equity_curve: pd.Series
    positions: pd.DataFrame
    trades: pd.DataFrame
    event_log: pd.DataFrame
    summary: dict[str, float]
    benchmark: pd.Series | None = None


def build_summary(
    equity_curve: pd.Series,
    benchmark: pd.Series | None = None,
) -> dict[str, float]:
    """Build high-level performance summary statistics."""
    if equity_curve.empty:
        return {}

    equity_curve = equity_curve.astype("float64")
    daily_returns = equity_curve.pct_change().fillna(0.0)
    total_return = float(equity_curve.iloc[-1] / equity_curve.iloc[0] - 1.0)
    periods = max(len(daily_returns), 1)
    annualized_return = float((1.0 + total_return) ** (252 / periods) - 1.0)
    volatility = float(daily_returns.std(ddof=0) * np.sqrt(252))
    sharpe = 0.0 if volatility == 0 else float(daily_returns.mean() / daily_returns.std(ddof=0) * np.sqrt(252))

    running_max = equity_curve.cummax()
    drawdown = equity_curve / running_max - 1.0
    max_drawdown = float(drawdown.min())

    summary: dict[str, float] = {
        "start_equity": float(equity_curve.iloc[0]),
        "end_equity": float(equity_curve.iloc[-1]),
        "total_return": total_return,
        "annualized_return": annualized_return,
        "volatility": volatility,
        "sharpe": sharpe,
        "max_drawdown": max_drawdown,
    }

    if benchmark is None or benchmark.empty:
        return summary

    benchmark = benchmark.astype("float64").reindex(equity_curve.index).dropna()
    aligned = pd.concat([daily_returns, benchmark.pct_change().fillna(0.0)], axis=1, join="inner")
    aligned.columns = ["strategy", "benchmark"]
    if aligned.empty:
        return summary

    benchmark_returns = aligned["benchmark"]
    benchmark_total_return = float(benchmark.iloc[-1] / benchmark.iloc[0] - 1.0)
    summary["benchmark_total_return"] = benchmark_total_return
    summary["relative_total_return"] = total_return - benchmark_total_return

    benchmark_variance = float(benchmark_returns.var(ddof=0))
    if benchmark_variance > 0:
        covariance = float(np.cov(aligned["strategy"], benchmark_returns, ddof=0)[0][1])
        beta = covariance / benchmark_variance
    else:
        beta = 0.0
    benchmark_annualized = float((1.0 + benchmark_total_return) ** (252 / max(len(benchmark_returns), 1)) - 1.0)
    alpha = annualized_return - beta * benchmark_annualized
    summary["beta"] = float(beta)
    summary["alpha"] = float(alpha)
    return summary
