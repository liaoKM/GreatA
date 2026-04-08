"""Structured backtest results and summary metrics."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

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

    @classmethod
    def merge(cls, results: Iterable["BacktestResult"]) -> "BacktestResult":
        """Merge multiple backtest results into one continuous result.

        Equity and benchmark curves are chained by returns, so independently
        computed segments can still be combined into one continuous series.
        Tabular outputs are concatenated and sorted by their timestamp columns.
        """
        result_list = [result for result in results if result is not None]
        if not result_list:
            raise ValueError("BacktestResult.merge() requires at least one result")

        ordered_results = sorted(
            result_list,
            key=lambda result: pd.Timestamp(result.equity_curve.index[0]) if not result.equity_curve.empty else pd.Timestamp.max,
        )

        merged_equity = _chain_series([result.equity_curve for result in ordered_results], name="equity")
        benchmark_segments = [result.benchmark for result in ordered_results if result.benchmark is not None]
        merged_benchmark = None
        if benchmark_segments:
            merged_benchmark = _chain_series(benchmark_segments, name="benchmark")

        positions = _concat_frames(
            [result.positions for result in ordered_results],
            sort_columns=["session_date", "stock_code"],
        )
        trades = _concat_frames(
            [result.trades for result in ordered_results],
            sort_columns=["session_date", "stock_code", "side"],
        )
        event_log = _concat_frames(
            [result.event_log for result in ordered_results],
            sort_columns=["session_date", "publish_date", "stock_code", "source", "subtype"],
        )
        summary = build_summary(merged_equity, benchmark=merged_benchmark)

        return cls(
            equity_curve=merged_equity,
            positions=positions,
            trades=trades,
            event_log=event_log,
            summary=summary,
            benchmark=merged_benchmark,
        )

    def plot(
        self,
        save_path: str | Path | None = None,
        show: bool = False,
        include_benchmark: bool = True,
        title: str | None = None,
        figsize: tuple[float, float] = (12.0, 8.0),
    ):
        """Plot the result equity curve, benchmark spread, and drawdown.

        Returning ``matplotlib`` objects keeps the method useful in notebooks
        and scripts, while ``save_path`` allows direct image export.
        """
        try:
            import matplotlib.pyplot as plt
        except ImportError as exc:  # pragma: no cover - depends on local env
            raise RuntimeError("matplotlib is required for BacktestResult.plot()") from exc

        if self.equity_curve.empty:
            raise ValueError("Cannot plot an empty BacktestResult.equity_curve")

        equity = self.equity_curve.astype("float64").dropna()
        running_max = equity.cummax()
        drawdown = equity / running_max - 1.0

        figure, (equity_axis, spread_axis, drawdown_axis) = plt.subplots(
            3,
            1,
            figsize=figsize,
            sharex=True,
            gridspec_kw={"height_ratios": [3, 1.2, 1]},
        )

        normalized_equity = equity / float(equity.iloc[0])
        equity_axis.plot(normalized_equity.index, normalized_equity.values, label="Strategy", linewidth=1.8)
        spread_axis.axhline(0.0, color="tab:gray", linewidth=1.0, linestyle="--", alpha=0.7)

        if include_benchmark and self.benchmark is not None and not self.benchmark.empty:
            benchmark = self.benchmark.astype("float64").reindex(equity.index).dropna()
            if not benchmark.empty:
                benchmark = benchmark / float(benchmark.iloc[0])
                equity_axis.plot(benchmark.index, benchmark.values, label="Benchmark", linewidth=1.5, alpha=0.85)
                aligned = pd.concat([normalized_equity, benchmark], axis=1, join="inner").dropna()
                if not aligned.empty:
                    aligned.columns = ["strategy", "benchmark"]
                    spread = aligned["strategy"] - aligned["benchmark"]
                    spread_axis.fill_between(spread.index, spread.values, 0.0, color="tab:green", alpha=0.2)
                    spread_axis.plot(spread.index, spread.values, color="tab:green", linewidth=1.2)
        else:
            spread_axis.text(
                0.5,
                0.5,
                "No benchmark",
                transform=spread_axis.transAxes,
                ha="center",
                va="center",
                color="tab:gray",
            )

        equity_axis.set_ylabel("Normalized Equity")
        equity_axis.set_title(title or "Backtest Result")
        equity_axis.grid(True, alpha=0.25)
        if len(equity_axis.lines) > 1:
            equity_axis.legend()

        spread_axis.set_ylabel("Return Spread")
        spread_axis.grid(True, alpha=0.25)

        drawdown_axis.fill_between(drawdown.index, drawdown.values, 0.0, color="tab:red", alpha=0.25)
        drawdown_axis.plot(drawdown.index, drawdown.values, color="tab:red", linewidth=1.0)
        drawdown_axis.set_ylabel("Drawdown")
        drawdown_axis.set_xlabel("Date")
        drawdown_axis.grid(True, alpha=0.25)

        figure.tight_layout()

        if save_path is not None:
            output_path = Path(save_path)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            figure.savefig(output_path, dpi=150, bbox_inches="tight")

        if show:
            plt.show()

        return figure, (equity_axis, spread_axis, drawdown_axis)


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


def _chain_series(segments: Iterable[pd.Series | None], name: str) -> pd.Series:
    chained_parts: list[pd.Series] = []
    last_value: float | None = None

    for segment in segments:
        if segment is None:
            continue
        current = pd.Series(segment).copy()
        if current.empty:
            continue
        current.index = pd.to_datetime(current.index).normalize()
        current = current.astype("float64").sort_index()
        current = current[~current.index.duplicated(keep="last")]

        if last_value is not None:
            start_value = float(current.iloc[0])
            if start_value == 0:
                raise ValueError("Cannot merge result segment with zero starting value")
            current = current * (last_value / start_value)

        chained_parts.append(current)
        last_value = float(current.iloc[-1])

    if not chained_parts:
        return pd.Series(dtype="float64", name=name)

    merged = pd.concat(chained_parts).sort_index()
    merged = merged[~merged.index.duplicated(keep="last")]
    merged.name = name
    return merged


def _concat_frames(
    frames: Iterable[pd.DataFrame],
    sort_columns: list[str],
) -> pd.DataFrame:
    non_empty_frames = [frame.copy() for frame in frames if frame is not None and not frame.empty]
    if not non_empty_frames:
        return pd.DataFrame()

    merged = pd.concat(non_empty_frames, ignore_index=True)
    available_sort_columns = [column for column in sort_columns if column in merged.columns]
    if available_sort_columns:
        merged = merged.sort_values(available_sort_columns).reset_index(drop=True)
    return merged
