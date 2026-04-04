"""Unit tests for the v2 backtest framework."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import pandas as pd

from simple_backtrade import BacktestConfig, EmptyStrategy, TargetPortfolio
from simple_backtrade.data.portal import DataPortal
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


class RecordingStrategy(BaseStrategy):
    """Strategy used for callback-order testing."""

    def __init__(self):
        self.call_log: list[tuple[str, str, int]] = []

    def on_initialize(self, context) -> None:
        self.call_log.append(("initialize", "none", 0))

    def on_events(self, context, events: list[PublishedEvent]) -> None:
        self.call_log.append(("events", context.session_date.strftime("%Y-%m-%d"), len(events)))

    def on_bar_close(self, context) -> TargetPortfolio:
        self.call_log.append(("bar", context.session_date.strftime("%Y-%m-%d"), 0))
        return TargetPortfolio.empty()


class BuyThenSellStrategy(BaseStrategy):
    """Strategy used for rebalance testing."""

    def on_bar_close(self, context) -> TargetPortfolio:
        if context.session_date == pd.Timestamp("2026-01-02"):
            return TargetPortfolio.equal_weight(["AAA.SZ"])
        return TargetPortfolio.empty()


class FrameworkTests(unittest.TestCase):
    """End-to-end tests for the v2 framework."""

    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.data_root = Path(self.tempdir.name)

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def _base_market_rows(self) -> list[dict[str, object]]:
        return [
            {
                "stock_code": "AAA.SZ",
                "trade_date": "2026-01-02",
                "open": 10.0,
                "high": 10.0,
                "low": 10.0,
                "close": 10.0,
                "adj_factor": 1.0,
            },
            {
                "stock_code": "AAA.SZ",
                "trade_date": "2026-01-05",
                "open": 11.0,
                "high": 11.0,
                "low": 11.0,
                "close": 11.0,
                "adj_factor": 1.0,
            },
            {
                "stock_code": "AAA.SZ",
                "trade_date": "2026-01-06",
                "open": 12.0,
                "high": 12.0,
                "low": 12.0,
                "close": 12.0,
                "adj_factor": 1.0,
            },
        ]

    def test_partition_loader_deduplicates_mixed_partitions(self) -> None:
        _write_market_frame(
            self.data_root,
            "yearly",
            "2026",
            [
                {
                    "stock_code": "AAA.SZ",
                    "trade_date": "2026-01-02",
                    "open": 10.0,
                    "high": 10.0,
                    "low": 10.0,
                    "close": 10.0,
                    "adj_factor": 1.0,
                }
            ],
        )
        _write_market_frame(
            self.data_root,
            "monthly",
            "2026-01",
            [
                {
                    "stock_code": "AAA.SZ",
                    "trade_date": "2026-01-02",
                    "open": 99.0,
                    "high": 99.0,
                    "low": 99.0,
                    "close": 99.0,
                    "adj_factor": 1.0,
                },
                {
                    "stock_code": "AAA.SZ",
                    "trade_date": "2026-01-05",
                    "open": 11.0,
                    "high": 11.0,
                    "low": 11.0,
                    "close": 11.0,
                    "adj_factor": 1.0,
                },
            ],
        )
        _write_market_frame(
            self.data_root,
            "daily",
            "2026-01-05",
            [
                {
                    "stock_code": "AAA.SZ",
                    "trade_date": "2026-01-05",
                    "open": 88.0,
                    "high": 88.0,
                    "low": 88.0,
                    "close": 88.0,
                    "adj_factor": 1.0,
                }
            ],
        )
        _write_market_frame(
            self.data_root,
            "daily",
            "2026-01-06",
            [
                {
                    "stock_code": "AAA.SZ",
                    "trade_date": "2026-01-06",
                    "open": 12.0,
                    "high": 12.0,
                    "low": 12.0,
                    "close": 12.0,
                    "adj_factor": 1.0,
                }
            ],
        )

        portal = DataPortal(
            BacktestConfig(
                start="2026-01-02",
                end="2026-01-06",
                data_path=self.data_root,
                warmup_bars=0,
            )
        )

        current = portal.market.current("2026-01-02")
        self.assertEqual(float(current.loc["AAA.SZ", "close"]), 10.0)

        history = portal.market.history(["AAA.SZ"], "2026-01-06", bars=10)
        self.assertEqual(len(history), 3)
        self.assertListEqual(
            history.index.get_level_values("trade_date").strftime("%Y-%m-%d").tolist(),
            ["2026-01-02", "2026-01-05", "2026-01-06"],
        )

    def test_hfq_uses_earliest_local_anchor(self) -> None:
        _write_market_frame(
            self.data_root,
            "daily",
            "2025-12-31",
            [
                {
                    "stock_code": "AAA.SZ",
                    "trade_date": "2025-12-31",
                    "open": 10.0,
                    "high": 10.0,
                    "low": 10.0,
                    "close": 10.0,
                    "adj_factor": 1.0,
                }
            ],
        )
        _write_market_frame(
            self.data_root,
            "monthly",
            "2026-01",
            [
                {
                    "stock_code": "AAA.SZ",
                    "trade_date": "2026-01-02",
                    "open": 20.0,
                    "high": 20.0,
                    "low": 20.0,
                    "close": 20.0,
                    "adj_factor": 2.0,
                },
                {
                    "stock_code": "AAA.SZ",
                    "trade_date": "2026-01-05",
                    "open": 30.0,
                    "high": 30.0,
                    "low": 30.0,
                    "close": 30.0,
                    "adj_factor": 3.0,
                },
            ],
        )

        portal = DataPortal(
            BacktestConfig(
                start="2026-01-02",
                end="2026-01-05",
                data_path=self.data_root,
                warmup_bars=0,
            )
        )

        adjusted = portal.adjusted_close("2026-01-05", stock_codes=["AAA.SZ"])
        self.assertEqual(float(adjusted.loc["AAA.SZ"]), 90.0)

    def test_event_schedule_uses_session_window_and_precedes_bar(self) -> None:
        _write_market_frame(self.data_root, "daily", "2026-01-02", [self._base_market_rows()[0]])
        _write_market_frame(self.data_root, "daily", "2026-01-05", [self._base_market_rows()[1]])
        _write_market_frame(self.data_root, "daily", "2026-01-06", [self._base_market_rows()[2]])
        _write_finance_frame(
            self.data_root,
            "daily",
            "2026-01-02",
            [
                {
                    "stock_code": "AAA.SZ",
                    "ann_date": "2026-01-02",
                    "end_date": "2025-12-31",
                    "eps": 1.2,
                }
            ],
        )
        _write_announcement_frame(
            self.data_root,
            "forecast",
            "daily",
            "2026-01-03",
            [
                {
                    "stock_code": "AAA.SZ",
                    "ann_date": "2026-01-03",
                    "end_date": "2025-12-31",
                    "type": "预增",
                }
            ],
        )

        strategy = RecordingStrategy()
        engine = BacktestEngine(
            BacktestConfig(
                start="2026-01-02",
                end="2026-01-06",
                data_path=self.data_root,
                warmup_bars=0,
            ),
            strategy,
        )
        result = engine.run()

        self.assertEqual(len(result.event_log), 2)
        self.assertEqual(
            result.event_log["publish_date"].dt.strftime("%Y-%m-%d").tolist(),
            ["2026-01-02", "2026-01-03"],
        )
        self.assertEqual(
            result.event_log["session_date"].dt.strftime("%Y-%m-%d").tolist(),
            ["2026-01-02", "2026-01-05"],
        )
        expected_calls = [
            ("initialize", "none", 0),
            ("events", "2026-01-02", 1),
            ("bar", "2026-01-02", 0),
            ("events", "2026-01-05", 1),
            ("bar", "2026-01-05", 0),
            ("events", "2026-01-06", 0),
            ("bar", "2026-01-06", 0),
        ]
        self.assertEqual(strategy.call_log, expected_calls)

    def test_rebalance_rounds_to_lots_and_result_contains_required_outputs(self) -> None:
        _write_market_frame(self.data_root, "daily", "2026-01-02", [self._base_market_rows()[0]])
        _write_market_frame(self.data_root, "daily", "2026-01-05", [self._base_market_rows()[1]])
        benchmark = pd.Series(
            [100.0, 101.0],
            index=pd.to_datetime(["2026-01-02", "2026-01-05"]),
            dtype="float64",
        )

        engine = BacktestEngine(
            BacktestConfig(
                start="2026-01-02",
                end="2026-01-05",
                data_path=self.data_root,
                initial_cash=1050,
                warmup_bars=0,
                benchmark=benchmark,
            ),
            BuyThenSellStrategy(),
        )
        result = engine.run()

        self.assertEqual(len(result.trades), 2)
        self.assertEqual(int(result.trades.iloc[0]["quantity"]), 100)
        self.assertEqual(int(result.trades.iloc[1]["quantity"]), 100)
        self.assertAlmostEqual(float(result.equity_curve.iloc[-1]), 1150.0)
        self.assertIn("benchmark_total_return", result.summary)
        self.assertIn("equity_curve", result.__dict__)
        self.assertIn("positions", result.__dict__)
        self.assertIn("trades", result.__dict__)
        self.assertIn("event_log", result.__dict__)
        self.assertIn("summary", result.__dict__)

    def test_empty_strategy_stays_in_cash(self) -> None:
        _write_market_frame(self.data_root, "daily", "2026-01-02", [self._base_market_rows()[0]])
        _write_market_frame(self.data_root, "daily", "2026-01-05", [self._base_market_rows()[1]])

        engine = BacktestEngine(
            BacktestConfig(
                start="2026-01-02",
                end="2026-01-05",
                data_path=self.data_root,
                initial_cash=2000,
                warmup_bars=0,
            ),
            EmptyStrategy(),
        )
        result = engine.run()

        self.assertTrue(result.trades.empty)
        self.assertTrue(result.positions.empty)
        self.assertEqual(float(result.equity_curve.iloc[-1]), 2000.0)


if __name__ == "__main__":
    unittest.main()
