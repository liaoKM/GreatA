"""Unit tests for persisted benchmark data support."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import pandas as pd

from data_service.benchmark_storage import (
    OFFICIAL_TUSHARE_BENCHMARK_IDS,
    get_benchmark_partition_path,
)
from data_service.fetcher.tushare.benchmark_fetcher import TushareBenchmarkFetcher
from simple_backtrade import BacktestConfig, EmptyStrategy, build_registered_custom_benchmark
from simple_backtrade.data.portal import DataPortal
from simple_backtrade.engine.runner import BacktestEngine


def _write_market_frame(root: Path, level: str, name: str, rows: list[dict[str, object]]) -> None:
    path = root / "market" / level / f"{name}.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_parquet(path, index=False)


def _write_finance_frame(root: Path, level: str, name: str, rows: list[dict[str, object]]) -> None:
    path = root / "finance" / level / f"{name}.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_parquet(path, index=False)


def _write_benchmark_frame(
    root: Path,
    benchmark_id: str,
    level: str,
    name: str,
    rows: list[dict[str, object]],
) -> None:
    path = get_benchmark_partition_path(root, benchmark_id, level, name)
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_parquet(path, index=False)


class PersistedBenchmarkTests(unittest.TestCase):
    """Coverage for local benchmark persistence and loading."""

    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.data_root = Path(self.tempdir.name)

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def _write_market_days(self) -> None:
        _write_market_frame(
            self.data_root,
            "daily",
            "2026-01-02",
            [{"stock_code": "AAA.SZ", "trade_date": "2026-01-02", "open": 10.0, "high": 10.0, "low": 10.0, "close": 10.0, "adj_factor": 1.0}],
        )
        _write_market_frame(
            self.data_root,
            "daily",
            "2026-01-05",
            [{"stock_code": "AAA.SZ", "trade_date": "2026-01-05", "open": 11.0, "high": 11.0, "low": 11.0, "close": 11.0, "adj_factor": 1.0}],
        )
        _write_market_frame(
            self.data_root,
            "daily",
            "2026-01-06",
            [{"stock_code": "AAA.SZ", "trade_date": "2026-01-06", "open": 12.0, "high": 12.0, "low": 12.0, "close": 12.0, "adj_factor": 1.0}],
        )

    def test_tushare_benchmark_fetcher_supports_only_fixed_ids_and_normalizes_output(self) -> None:
        self.assertEqual(TushareBenchmarkFetcher.BENCHMARK_IDS, OFFICIAL_TUSHARE_BENCHMARK_IDS)

        raw = pd.DataFrame(
            [
                {
                    "ts_code": "000300.SH",
                    "trade_date": "20260105",
                    "close": 100.0,
                    "open": 99.0,
                    "high": 101.0,
                    "low": 98.0,
                    "pre_close": 98.5,
                    "change": 1.5,
                    "pct_chg": 1.52,
                    "vol": 1234.0,
                    "amount": 5678.0,
                }
            ]
        )
        normalized = TushareBenchmarkFetcher._normalize_frame(raw, "tushare:000300.SH")
        self.assertEqual(normalized.iloc[0]["benchmark_id"], "tushare:000300.SH")
        self.assertEqual(normalized.iloc[0]["benchmark_code"], "000300.SH")
        self.assertEqual(pd.Timestamp(normalized.iloc[0]["trade_date"]), pd.Timestamp("2026-01-05"))

    def test_benchmark_view_loads_partitioned_series_without_fill(self) -> None:
        self._write_market_days()
        _write_benchmark_frame(
            self.data_root,
            "tushare:000300.SH",
            "monthly",
            "2026-01",
            [
                {"benchmark_id": "tushare:000300.SH", "trade_date": "2026-01-02", "close": 100.0},
                {"benchmark_id": "tushare:000300.SH", "trade_date": "2026-01-05", "close": 101.0},
            ],
        )
        _write_benchmark_frame(
            self.data_root,
            "tushare:000300.SH",
            "daily",
            "2026-01-05",
            [{"benchmark_id": "tushare:000300.SH", "trade_date": "2026-01-05", "close": 999.0}],
        )
        _write_benchmark_frame(
            self.data_root,
            "tushare:000300.SH",
            "daily",
            "2026-01-06",
            [{"benchmark_id": "tushare:000300.SH", "trade_date": "2026-01-06", "close": 102.0}],
        )

        portal = DataPortal(
            BacktestConfig(start="2026-01-02", end="2026-01-06", data_path=self.data_root, warmup_bars=0)
        )
        series = portal.benchmark.get_series("tushare:000300.SH", portal.sessions)
        self.assertEqual(series.index.strftime("%Y-%m-%d").tolist(), ["2026-01-02", "2026-01-05", "2026-01-06"])
        self.assertEqual(series.tolist(), [100.0, 101.0, 102.0])

    def test_backtest_reads_local_benchmark_id_and_missing_benchmark_raises(self) -> None:
        self._write_market_days()
        _write_benchmark_frame(
            self.data_root,
            "tushare:000300.SH",
            "daily",
            "2026-01-02",
            [{"benchmark_id": "tushare:000300.SH", "trade_date": "2026-01-02", "close": 100.0}],
        )
        _write_benchmark_frame(
            self.data_root,
            "tushare:000300.SH",
            "daily",
            "2026-01-05",
            [{"benchmark_id": "tushare:000300.SH", "trade_date": "2026-01-05", "close": 105.0}],
        )

        result = BacktestEngine(
            BacktestConfig(
                start="2026-01-02",
                end="2026-01-05",
                data_path=self.data_root,
                initial_cash=1000.0,
                warmup_bars=0,
                benchmark="tushare:000300.SH",
            ),
            EmptyStrategy(),
        ).run()
        self.assertIn("benchmark_total_return", result.summary)

        with self.assertRaises(FileNotFoundError):
            BacktestEngine(
                BacktestConfig(
                    start="2026-01-02",
                    end="2026-01-05",
                    data_path=self.data_root,
                    initial_cash=1000.0,
                    warmup_bars=0,
                    benchmark="custom:finance_growth",
                ),
                EmptyStrategy(),
            ).run()

    def test_build_registered_custom_benchmark_persists_curve_for_portal(self) -> None:
        _write_market_frame(
            self.data_root,
            "daily",
            "2026-04-29",
            [{"stock_code": "AAA.SZ", "trade_date": "2026-04-29", "open": 10.0, "high": 10.0, "low": 10.0, "close": 10.0, "adj_factor": 1.0}],
        )
        _write_finance_frame(
            self.data_root,
            "daily",
            "2026-04-29",
            [
                {"stock_code": "AAA.SZ", "ann_date": "2023-03-20", "end_date": "2022-12-31", "dt_netprofit_yoy": 15.0, "netprofit_yoy": 15.0},
                {"stock_code": "AAA.SZ", "ann_date": "2024-03-20", "end_date": "2023-12-31", "dt_netprofit_yoy": 12.0, "netprofit_yoy": 12.0},
                {"stock_code": "AAA.SZ", "ann_date": "2025-03-20", "end_date": "2024-12-31", "dt_netprofit_yoy": 11.0, "netprofit_yoy": 11.0},
                {"stock_code": "AAA.SZ", "ann_date": "2026-04-29", "end_date": "2026-03-31", "dt_netprofit_yoy": 5.0, "netprofit_yoy": 5.0},
            ],
        )

        result, written_paths = build_registered_custom_benchmark(
            benchmark_id="custom:finance_growth",
            start="2026-04-29",
            end="2026-04-29",
            data_path=self.data_root,
            warmup_bars=0,
        )
        self.assertTrue(written_paths)
        self.assertAlmostEqual(float(result.equity_curve.iloc[0]), 1000.0, places=6)

        portal = DataPortal(
            BacktestConfig(start="2026-04-29", end="2026-04-29", data_path=self.data_root, warmup_bars=0)
        )
        series = portal.benchmark.get_series("custom:finance_growth", portal.sessions)
        self.assertEqual(series.index.strftime("%Y-%m-%d").tolist(), ["2026-04-29"])
        self.assertEqual(series.tolist(), [1000.0])


if __name__ == "__main__":
    unittest.main()
