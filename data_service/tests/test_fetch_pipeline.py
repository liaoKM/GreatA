"""Tests for the unified fetch pipeline."""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from data_service.benchmark_storage import OFFICIAL_TUSHARE_BENCHMARK_IDS, get_benchmark_partition_path
from data_service.consolidation import consolidate_data_type
from data_service.fetch_meta import bootstrap_fetch_meta, get_last_update_path
from data_service.fetch_pipeline import run_fetch
from data_service.fetcher.announcement_fetcher import ANNOUNCEMENT_TYPES, AnnouncementFetcher
from data_service.partition_writer import write_partitioned_data


def _write_frame(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_parquet(path, index=False)


class _EmptyFinanceFetcher:
    def __init__(self, data_path: str = "./data"):
        self.data_path = Path(data_path)

    def fetch_history(self, start, end) -> pd.DataFrame:
        del start, end
        return pd.DataFrame(columns=["stock_code", "ann_date", "end_date"])


class _DummyAnnouncementFetcher(AnnouncementFetcher):
    def fetch_by_type(self, ann_type: str, date) -> pd.DataFrame:
        return self.fetch_history_by_type(ann_type, date, date)

    def fetch_history_by_type(self, ann_type: str, start, end) -> pd.DataFrame:
        if ann_type == "forecast":
            return pd.DataFrame(
                [
                    {"stock_code": "AAA.SZ", "ann_date": "2026-03-01", "end_date": "2025-12-31"},
                ]
            )
        if ann_type == "express":
            return pd.DataFrame(
                [
                    {"stock_code": "BBB.SZ", "ann_date": "2026-03-02", "end_date": "2025-12-31"},
                ]
            )
        return pd.DataFrame()


class UnifiedFetchPipelineTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.data_root = Path(self.tempdir.name)

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def test_bootstrap_fetch_meta_infers_shared_cutoffs(self) -> None:
        _write_frame(
            self.data_root / "market" / "yearly" / "2025.parquet",
            [{"stock_code": "AAA.SZ", "trade_date": "2025-12-31", "close": 1.0}],
        )
        _write_frame(
            self.data_root / "finance" / "monthly" / "2026-02.parquet",
            [{"stock_code": "AAA.SZ", "ann_date": "2026-02-15", "end_date": "2025-12-31"}],
        )
        _write_frame(
            self.data_root / "finance" / "daily" / "2026-03-05.parquet",
            [{"stock_code": "AAA.SZ", "ann_date": "2026-03-05", "end_date": "2025-12-31"}],
        )

        announcement_dates = {
            "forecast": "2026-03-05",
            "express": "2026-03-04",
            "repurchase": "2026-03-03",
            "holdertrade": "2026-03-02",
        }
        for ann_type, current_date in announcement_dates.items():
            _write_frame(
                self.data_root / "announcement" / ann_type / "daily" / f"{current_date}.parquet",
                [{"stock_code": "AAA.SZ", "ann_date": current_date}],
            )

        benchmark_dates = {
            "tushare:000001.SH": "2026-03-03",
            "tushare:000300.SH": "2026-03-31",
            "tushare:399001.SZ": "2026-03-02",
        }
        for benchmark_id, current_date in benchmark_dates.items():
            level = "monthly" if current_date.endswith("-31") else "daily"
            key = current_date[:7] if level == "monthly" else current_date
            _write_frame(
                get_benchmark_partition_path(self.data_root, benchmark_id, level, key),
                [{"benchmark_id": benchmark_id, "trade_date": current_date, "close": 100.0}],
            )

        meta = bootstrap_fetch_meta(self.data_root)
        self.assertEqual(meta["market"], pd.Timestamp("2025-12-31"))
        self.assertEqual(meta["finance"], pd.Timestamp("2026-03-05"))
        self.assertEqual(meta["announcement"], pd.Timestamp("2026-03-02"))
        self.assertEqual(meta["benchmark"], pd.Timestamp("2026-03-02"))

        payload = json.loads(get_last_update_path(self.data_root).read_text(encoding="utf-8"))
        self.assertEqual(payload["benchmark"], "2026-03-02")

    def test_announcement_fetcher_combines_subtypes_into_one_frame(self) -> None:
        fetcher = _DummyAnnouncementFetcher(data_path=str(self.data_root))
        frame = fetcher.fetch_history(pd.Timestamp("2026-03-01"), pd.Timestamp("2026-03-02"))
        self.assertEqual(sorted(frame["ann_type"].unique().tolist()), ["express", "forecast"])
        self.assertEqual(len(frame), 2)

    def test_write_partitioned_data_routes_announcement_and_benchmark_frames(self) -> None:
        announcement_frame = pd.DataFrame(
            [
                {"stock_code": "AAA.SZ", "ann_type": "forecast", "ann_date": "2026-02-03", "end_date": "2025-12-31"},
                {"stock_code": "AAA.SZ", "ann_type": "forecast", "ann_date": "2026-03-01", "end_date": "2025-12-31"},
                {"stock_code": "BBB.SZ", "ann_type": "express", "ann_date": "2026-02-05", "end_date": "2025-12-31"},
            ]
        )
        announcement_paths = write_partitioned_data(
            data_path=self.data_root,
            data_type="announcement",
            frame=announcement_frame,
            start="2026-02-01",
            end="2026-03-02",
            update_meta=True,
        )
        self.assertTrue(self.data_root.joinpath("announcement", "forecast", "monthly", "2026-02.parquet").exists())
        self.assertTrue(self.data_root.joinpath("announcement", "forecast", "daily", "2026-03-01.parquet").exists())
        self.assertTrue(self.data_root.joinpath("announcement", "express", "monthly", "2026-02.parquet").exists())
        self.assertGreaterEqual(len(announcement_paths), 3)

        benchmark_frame = pd.DataFrame(
            [
                {"benchmark_id": OFFICIAL_TUSHARE_BENCHMARK_IDS[0], "trade_date": "2026-02-03", "close": 100.0},
                {"benchmark_id": OFFICIAL_TUSHARE_BENCHMARK_IDS[1], "trade_date": "2026-02-03", "close": 200.0},
                {"benchmark_id": OFFICIAL_TUSHARE_BENCHMARK_IDS[2], "trade_date": "2026-02-03", "close": 300.0},
            ]
        )
        write_partitioned_data(
            data_path=self.data_root,
            data_type="benchmark",
            frame=benchmark_frame,
            start="2026-02-01",
            end="2026-02-28",
            update_meta=True,
        )
        for benchmark_id in OFFICIAL_TUSHARE_BENCHMARK_IDS:
            self.assertTrue(get_benchmark_partition_path(self.data_root, benchmark_id, "monthly", "2026-02").exists())

    def test_run_fetch_advances_cutoff_on_empty_frame_and_then_noops(self) -> None:
        with patch.dict("data_service.fetch_pipeline.FETCHER_FACTORIES", {"finance": _EmptyFinanceFetcher}, clear=False):
            first = run_fetch(until="2000-01-03", data_type="finance", data_path=self.data_root)["finance"]
            self.assertEqual(first.status, "success")
            self.assertEqual(first.row_count, 0)

            meta_payload = json.loads(get_last_update_path(self.data_root).read_text(encoding="utf-8"))
            self.assertEqual(meta_payload["finance"], "2000-01-03")

            second = run_fetch(until="2000-01-03", data_type="finance", data_path=self.data_root)["finance"]
            self.assertEqual(second.status, "noop")

    def test_finance_consolidation_promotes_monthly_and_yearly_without_status_files(self) -> None:
        _write_frame(
            self.data_root / "finance" / "daily" / "2026-01-05.parquet",
            [{"stock_code": "AAA.SZ", "ann_date": "2026-01-05", "end_date": "2025-12-31", "update_flag": 0}],
        )
        _write_frame(
            self.data_root / "finance" / "daily" / "2026-01-06.parquet",
            [{"stock_code": "AAA.SZ", "ann_date": "2026-01-06", "end_date": "2025-12-31", "update_flag": 1}],
        )
        _write_frame(
            self.data_root / "finance" / "monthly" / "2026-02.parquet",
            [{"stock_code": "BBB.SZ", "ann_date": "2026-02-10", "end_date": "2025-12-31", "update_flag": 0}],
        )

        actions = consolidate_data_type("finance", cutoff="2026-12-31", data_path=self.data_root)

        self.assertTrue(self.data_root.joinpath("finance", "yearly", "2026.parquet").exists())
        self.assertFalse(self.data_root.joinpath("finance", "daily", "2026-01-05.parquet").exists())
        self.assertFalse(self.data_root.joinpath("finance", "monthly", "2026-01.parquet").exists())
        self.assertFalse(self.data_root.joinpath("finance", "monthly", "2026-02.parquet").exists())
        self.assertIn("finance", actions)


if __name__ == "__main__":
    unittest.main()
