"""Unit tests for the v2 custom benchmark framework."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import pandas as pd

from simple_backtrade import BacktestConfig, EmptyStrategy, TargetPortfolio
from simple_backtrade.benchmark import (
    CustomIndexBenchmarkProvider,
    CustomIndexConfig,
    CustomIndexEngine,
    FinanceGrowthIndexStrategy,
)
from simple_backtrade.engine.runner import BacktestEngine
from simple_backtrade.models import PublishedEvent
from simple_backtrade.strategy.base import BaseStrategy


def _write_market_frame(root: Path, level: str, name: str, rows: list[dict[str, object]]) -> None:
    path = root / "market" / level / f"{name}.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_parquet(path, index=False)


def _write_finance_frame(root: Path, level: str, name: str, rows: list[dict[str, object]]) -> None:
    path = root / "finance" / level / f"{name}.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_parquet(path, index=False)


def _write_announcement_frame(
    root: Path,
    subtype: str,
    level: str,
    name: str,
    rows: list[dict[str, object]],
) -> None:
    path = root / "announcement" / subtype / level / f"{name}.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_parquet(path, index=False)


class FinanceOnlyRecordingStrategy(BaseStrategy):
    """Record event-driven callback order for the custom index engine."""

    def __init__(self):
        self.call_log: list[tuple[str, str, int]] = []

    def on_initialize(self, context) -> None:
        self.call_log.append(("initialize", "none", 0))

    def on_events(self, context, events: list[PublishedEvent]) -> None:
        self.call_log.append(("events", context.session_date.strftime("%Y-%m-%d"), len(events)))

    def on_bar_close(self, context):
        self.call_log.append(("bar", context.session_date.strftime("%Y-%m-%d"), 0))
        return None


class MultiStepIndexStrategy(BaseStrategy):
    """Strategy used to test rebalance semantics in index mode."""

    def __init__(self):
        self.step = 0

    def on_events(self, context, events: list[PublishedEvent]) -> None:
        self.step += 1

    def on_bar_close(self, context):
        if self.step == 1:
            return TargetPortfolio(pd.Series({"AAA.SZ": 0.3, "BBB.SZ": 0.3}, dtype="float64"))
        if self.step == 2:
            return None
        if self.step == 3:
            return TargetPortfolio.empty()
        return None


class SingleMemberIndexStrategy(BaseStrategy):
    """A simple strategy for provider caching tests."""

    def on_bar_close(self, context):
        return TargetPortfolio.equal_weight(["AAA.SZ"])


class CustomBenchmarkTests(unittest.TestCase):
    """End-to-end tests for custom benchmark support."""

    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.data_root = Path(self.tempdir.name)

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def _write_market_days(self) -> None:
        market_rows = {
            "2026-01-02": [
                {"stock_code": "AAA.SZ", "trade_date": "2026-01-02", "open": 10.0, "high": 10.0, "low": 10.0, "close": 10.0, "adj_factor": 1.0},
                {"stock_code": "BBB.SZ", "trade_date": "2026-01-02", "open": 10.0, "high": 10.0, "low": 10.0, "close": 10.0, "adj_factor": 1.0},
            ],
            "2026-01-05": [
                {"stock_code": "AAA.SZ", "trade_date": "2026-01-05", "open": 30.0, "high": 30.0, "low": 30.0, "close": 30.0, "adj_factor": 1.0},
                {"stock_code": "BBB.SZ", "trade_date": "2026-01-05", "open": 10.0, "high": 10.0, "low": 10.0, "close": 10.0, "adj_factor": 1.0},
            ],
            "2026-01-06": [
                {"stock_code": "AAA.SZ", "trade_date": "2026-01-06", "open": 36.0, "high": 36.0, "low": 36.0, "close": 36.0, "adj_factor": 1.0},
                {"stock_code": "BBB.SZ", "trade_date": "2026-01-06", "open": 12.0, "high": 12.0, "low": 12.0, "close": 12.0, "adj_factor": 1.0},
            ],
            "2026-01-07": [
                {"stock_code": "AAA.SZ", "trade_date": "2026-01-07", "open": 36.0, "high": 36.0, "low": 36.0, "close": 36.0, "adj_factor": 1.0},
                {"stock_code": "BBB.SZ", "trade_date": "2026-01-07", "open": 12.0, "high": 12.0, "low": 12.0, "close": 12.0, "adj_factor": 1.0},
            ],
        }
        for name, rows in market_rows.items():
            _write_market_frame(self.data_root, "daily", name, rows)

    def test_custom_index_filters_to_finance_events_only(self) -> None:
        self._write_market_days()
        _write_finance_frame(
            self.data_root,
            "daily",
            "2026-01-02",
            [{"stock_code": "AAA.SZ", "ann_date": "2026-01-02", "end_date": "2025-12-31", "eps": 1.0}],
        )
        _write_announcement_frame(
            self.data_root,
            "forecast",
            "daily",
            "2026-01-03",
            [{"stock_code": "BBB.SZ", "ann_date": "2026-01-03", "end_date": "2025-12-31", "type": "预增"}],
        )

        strategy = FinanceOnlyRecordingStrategy()
        engine = CustomIndexEngine(
            CustomIndexConfig(
                start="2026-01-02",
                end="2026-01-06",
                data_path=self.data_root,
                warmup_bars=0,
                event_sources=("finance",),
            ),
            strategy,
        )
        result = engine.run()

        self.assertEqual(len(result.event_log), 1)
        self.assertEqual(result.event_log.iloc[0]["source"], "finance")
        self.assertEqual(
            strategy.call_log,
            [
                ("initialize", "none", 0),
                ("events", "2026-01-02", 1),
                ("bar", "2026-01-02", 0),
                ("events", "2026-01-05", 0),
                ("bar", "2026-01-05", 0),
                ("events", "2026-01-06", 0),
                ("bar", "2026-01-06", 0),
            ],
        )

    def test_index_portfolio_supports_fractional_shares_and_event_driven_rebalance(self) -> None:
        self._write_market_days()
        _write_finance_frame(
            self.data_root,
            "daily",
            "2026-01-02",
            [{"stock_code": "AAA.SZ", "ann_date": "2026-01-02", "end_date": "2025-12-31", "eps": 1.0}],
        )
        _write_finance_frame(
            self.data_root,
            "daily",
            "2026-01-05",
            [{"stock_code": "BBB.SZ", "ann_date": "2026-01-05", "end_date": "2025-12-31", "eps": 1.1}],
        )
        _write_finance_frame(
            self.data_root,
            "daily",
            "2026-01-06",
            [{"stock_code": "AAA.SZ", "ann_date": "2026-01-06", "end_date": "2025-12-31", "eps": 1.2}],
        )

        engine = CustomIndexEngine(
            CustomIndexConfig(
                start="2026-01-02",
                end="2026-01-07",
                data_path=self.data_root,
                warmup_bars=0,
                initial_level=1000.0,
            ),
            MultiStepIndexStrategy(),
        )
        result = engine.run()

        self.assertEqual(len(result.trades), 4)

        positions_on_first_rebalance = result.positions[
            result.positions["session_date"] == pd.Timestamp("2026-01-02")
        ].sort_values("stock_code")
        self.assertAlmostEqual(float(positions_on_first_rebalance.iloc[0]["quantity"]), 50.0, places=6)
        self.assertAlmostEqual(float(positions_on_first_rebalance.iloc[1]["quantity"]), 50.0, places=6)

        positions_on_second_rebalance = result.positions[
            result.positions["session_date"] == pd.Timestamp("2026-01-05")
        ].sort_values("stock_code")
        self.assertAlmostEqual(
            float(positions_on_second_rebalance.iloc[0]["quantity"]),
            float(positions_on_first_rebalance.iloc[0]["quantity"]),
            places=6,
        )
        self.assertAlmostEqual(float(result.equity_curve.loc[pd.Timestamp("2026-01-05")]), 2000.0, places=6)
        self.assertTrue(result.positions[result.positions["session_date"] == pd.Timestamp("2026-01-06")].empty)
        self.assertAlmostEqual(float(result.equity_curve.loc[pd.Timestamp("2026-01-07")]), 2400.0, places=6)
        self.assertNotIn("benchmark_total_return", result.summary)

    def test_provider_caches_result_and_can_feed_regular_backtest(self) -> None:
        self._write_market_days()
        _write_finance_frame(
            self.data_root,
            "daily",
            "2026-01-02",
            [{"stock_code": "AAA.SZ", "ann_date": "2026-01-02", "end_date": "2025-12-31", "eps": 1.0}],
        )

        build_count = {"count": 0}

        def strategy_factory():
            build_count["count"] += 1
            return SingleMemberIndexStrategy()

        provider = CustomIndexBenchmarkProvider(
            CustomIndexConfig(
                start="2026-01-02",
                end="2026-01-07",
                data_path=self.data_root,
                warmup_bars=0,
            ),
            strategy_factory=strategy_factory,
        )

        sessions = pd.to_datetime(["2026-01-02", "2026-01-05", "2026-01-06"])
        first = provider.get_series(sessions)
        second = provider.get_series(sessions)
        self.assertEqual(build_count["count"], 1)
        pd.testing.assert_series_equal(first, second)

        engine = BacktestEngine(
            BacktestConfig(
                start="2026-01-02",
                end="2026-01-06",
                data_path=self.data_root,
                initial_cash=1000.0,
                warmup_bars=0,
                benchmark=provider,
            ),
            EmptyStrategy(),
        )
        result = engine.run()
        self.assertIn("benchmark_total_return", result.summary)

    def test_finance_growth_strategy_selects_three_strong_annuals_and_positive_latest_report(self) -> None:
        _write_market_frame(
            self.data_root,
            "daily",
            "2026-04-29",
            [
                {"stock_code": "AAA.SZ", "trade_date": "2026-04-29", "open": 10.0, "high": 10.0, "low": 10.0, "close": 10.0, "adj_factor": 1.0},
                {"stock_code": "BBB.SZ", "trade_date": "2026-04-29", "open": 10.0, "high": 10.0, "low": 10.0, "close": 10.0, "adj_factor": 1.0},
                {"stock_code": "CCC.SZ", "trade_date": "2026-04-29", "open": 10.0, "high": 10.0, "low": 10.0, "close": 10.0, "adj_factor": 1.0},
                {"stock_code": "DDD.SZ", "trade_date": "2026-04-29", "open": 10.0, "high": 10.0, "low": 10.0, "close": 10.0, "adj_factor": 1.0},
            ],
        )
        _write_finance_frame(
            self.data_root,
            "daily",
            "2026-04-29",
            [
                {"stock_code": "AAA.SZ", "ann_date": "2023-03-20", "end_date": "2022-12-31", "dt_netprofit_yoy": 15.0, "netprofit_yoy": 15.0},
                {"stock_code": "AAA.SZ", "ann_date": "2024-03-20", "end_date": "2023-12-31", "dt_netprofit_yoy": 12.0, "netprofit_yoy": 12.0},
                {"stock_code": "AAA.SZ", "ann_date": "2025-03-20", "end_date": "2024-12-31", "dt_netprofit_yoy": None, "netprofit_yoy": 11.0},
                {"stock_code": "AAA.SZ", "ann_date": "2026-04-29", "end_date": "2026-03-31", "dt_netprofit_yoy": None, "netprofit_yoy": 5.0},
                {"stock_code": "BBB.SZ", "ann_date": "2023-03-20", "end_date": "2022-12-31", "dt_netprofit_yoy": 15.0, "netprofit_yoy": 15.0},
                {"stock_code": "BBB.SZ", "ann_date": "2024-03-20", "end_date": "2023-12-31", "dt_netprofit_yoy": 9.0, "netprofit_yoy": 9.0},
                {"stock_code": "BBB.SZ", "ann_date": "2025-03-20", "end_date": "2024-12-31", "dt_netprofit_yoy": 13.0, "netprofit_yoy": 13.0},
                {"stock_code": "BBB.SZ", "ann_date": "2026-04-29", "end_date": "2026-03-31", "dt_netprofit_yoy": 6.0, "netprofit_yoy": 6.0},
                {"stock_code": "CCC.SZ", "ann_date": "2023-03-20", "end_date": "2022-12-31", "dt_netprofit_yoy": 14.0, "netprofit_yoy": 14.0},
                {"stock_code": "CCC.SZ", "ann_date": "2024-03-20", "end_date": "2023-12-31", "dt_netprofit_yoy": 16.0, "netprofit_yoy": 16.0},
                {"stock_code": "CCC.SZ", "ann_date": "2025-03-20", "end_date": "2024-12-31", "dt_netprofit_yoy": 18.0, "netprofit_yoy": 18.0},
                {"stock_code": "CCC.SZ", "ann_date": "2026-04-29", "end_date": "2026-03-31", "dt_netprofit_yoy": -1.0, "netprofit_yoy": -1.0},
                {"stock_code": "DDD.SZ", "ann_date": "2024-03-20", "end_date": "2023-12-31", "dt_netprofit_yoy": 20.0, "netprofit_yoy": 20.0},
                {"stock_code": "DDD.SZ", "ann_date": "2025-03-20", "end_date": "2024-12-31", "dt_netprofit_yoy": 22.0, "netprofit_yoy": 22.0},
                {"stock_code": "DDD.SZ", "ann_date": "2026-04-29", "end_date": "2026-03-31", "dt_netprofit_yoy": 7.0, "netprofit_yoy": 7.0},
            ],
        )

        result = CustomIndexEngine(
            CustomIndexConfig(
                start="2026-04-29",
                end="2026-04-29",
                data_path=self.data_root,
                warmup_bars=0,
            ),
            FinanceGrowthIndexStrategy(),
        ).run()

        positions = result.positions[result.positions["session_date"] == pd.Timestamp("2026-04-29")]
        self.assertEqual(list(positions["stock_code"]), ["AAA.SZ"])
        self.assertAlmostEqual(float(positions.iloc[0]["quantity"]), 100.0, places=6)
        self.assertAlmostEqual(float(result.equity_curve.iloc[0]), 1000.0, places=6)

    def test_finance_growth_strategy_initializes_from_previous_natural_day_and_updates_on_event(self) -> None:
        _write_market_frame(
            self.data_root,
            "daily",
            "2026-04-29",
            [
                {"stock_code": "AAA.SZ", "trade_date": "2026-04-29", "open": 10.0, "high": 10.0, "low": 10.0, "close": 10.0, "adj_factor": 1.0},
                {"stock_code": "BBB.SZ", "trade_date": "2026-04-29", "open": 20.0, "high": 20.0, "low": 20.0, "close": 20.0, "adj_factor": 1.0},
            ],
        )
        _write_market_frame(
            self.data_root,
            "daily",
            "2026-04-30",
            [
                {"stock_code": "AAA.SZ", "trade_date": "2026-04-30", "open": 10.0, "high": 10.0, "low": 10.0, "close": 10.0, "adj_factor": 1.0},
                {"stock_code": "BBB.SZ", "trade_date": "2026-04-30", "open": 20.0, "high": 20.0, "low": 20.0, "close": 20.0, "adj_factor": 1.0},
            ],
        )
        _write_finance_frame(
            self.data_root,
            "daily",
            "2026-04-28",
            [
                {"stock_code": "AAA.SZ", "ann_date": "2023-03-20", "end_date": "2022-12-31", "dt_netprofit_yoy": 15.0, "netprofit_yoy": 15.0},
                {"stock_code": "AAA.SZ", "ann_date": "2024-03-20", "end_date": "2023-12-31", "dt_netprofit_yoy": 12.0, "netprofit_yoy": 12.0},
                {"stock_code": "AAA.SZ", "ann_date": "2025-03-20", "end_date": "2024-12-31", "dt_netprofit_yoy": 11.0, "netprofit_yoy": 11.0},
                {"stock_code": "AAA.SZ", "ann_date": "2026-04-28", "end_date": "2026-03-31", "dt_netprofit_yoy": 6.0, "netprofit_yoy": 6.0},
                {"stock_code": "BBB.SZ", "ann_date": "2023-03-20", "end_date": "2022-12-31", "dt_netprofit_yoy": 13.0, "netprofit_yoy": 13.0},
                {"stock_code": "BBB.SZ", "ann_date": "2024-03-20", "end_date": "2023-12-31", "dt_netprofit_yoy": 14.0, "netprofit_yoy": 14.0},
                {"stock_code": "BBB.SZ", "ann_date": "2025-03-20", "end_date": "2024-12-31", "dt_netprofit_yoy": 16.0, "netprofit_yoy": 16.0},
            ],
        )
        _write_finance_frame(
            self.data_root,
            "daily",
            "2026-04-29",
            [
                {"stock_code": "BBB.SZ", "ann_date": "2026-04-29", "end_date": "2026-03-31", "dt_netprofit_yoy": 5.0, "netprofit_yoy": 5.0},
            ],
        )

        result = CustomIndexEngine(
            CustomIndexConfig(
                start="2026-04-29",
                end="2026-04-30",
                data_path=self.data_root,
                warmup_bars=0,
            ),
            FinanceGrowthIndexStrategy(),
        ).run()

        first_day = result.positions[result.positions["session_date"] == pd.Timestamp("2026-04-29")].sort_values("stock_code")
        second_day = result.positions[result.positions["session_date"] == pd.Timestamp("2026-04-30")].sort_values("stock_code")
        self.assertEqual(first_day["stock_code"].tolist(), ["AAA.SZ", "BBB.SZ"])
        self.assertEqual(second_day["stock_code"].tolist(), ["AAA.SZ", "BBB.SZ"])
        self.assertEqual(result.event_log["session_date"].dt.strftime("%Y-%m-%d").tolist(), ["2026-04-29"])


if __name__ == "__main__":
    unittest.main()
