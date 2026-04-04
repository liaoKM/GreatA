"""Tushare benchmark data fetcher."""

from __future__ import annotations

from datetime import datetime

import pandas as pd
import tushare as ts

from config.settings import get_token

from ...benchmark_storage import OFFICIAL_TUSHARE_BENCHMARK_IDS
from ..benchmark_fetcher import BenchmarkFetcher


class TushareBenchmarkFetcher(BenchmarkFetcher):
    """Fetch supported benchmark indices from Tushare index_daily."""

    BENCHMARK_IDS = OFFICIAL_TUSHARE_BENCHMARK_IDS

    def __init__(self, token: str = None, data_path: str = "./data"):
        super().__init__(data_path=data_path)
        if token is None:
            token = get_token("tushare")
        self.pro = ts.pro_api(token)

    def fetch_daily(self, date: datetime) -> pd.DataFrame:
        """Fetch one trading day of benchmark data for all supported ids."""
        frames = [
            self.fetch_daily_by_id(benchmark_id, date)
            for benchmark_id in self.BENCHMARK_IDS
        ]
        frames = [frame for frame in frames if frame is not None and not frame.empty]
        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True)

    def fetch_history(self, start: datetime, end: datetime) -> pd.DataFrame:
        """Fetch one historical range for all supported ids."""
        frames = [
            self.fetch_history_by_id(benchmark_id, start, end)
            for benchmark_id in self.BENCHMARK_IDS
        ]
        frames = [frame for frame in frames if frame is not None and not frame.empty]
        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True)

    def fetch_daily_by_id(self, benchmark_id: str, date: datetime) -> pd.DataFrame:
        """Fetch one trading day for one supported benchmark id."""
        trade_date = pd.Timestamp(date).strftime("%Y%m%d")
        raw_id = self.raw_id(benchmark_id)
        frame = self.pro.index_daily(ts_code=raw_id, start_date=trade_date, end_date=trade_date)
        return self._normalize_frame(frame, benchmark_id)

    def fetch_history_by_id(self, benchmark_id: str, start: datetime, end: datetime) -> pd.DataFrame:
        """Fetch one historical range for one supported benchmark id."""
        raw_id = self.raw_id(benchmark_id)
        frame = self.pro.index_daily(
            ts_code=raw_id,
            start_date=pd.Timestamp(start).strftime("%Y%m%d"),
            end_date=pd.Timestamp(end).strftime("%Y%m%d"),
        )
        return self._normalize_frame(frame, benchmark_id)

    def fetch_trade_calendar(self, start: datetime, end: datetime) -> pd.DataFrame:
        """Return the exchange trade calendar for one natural-date range."""
        return self.pro.trade_cal(
            start_date=pd.Timestamp(start).strftime("%Y%m%d"),
            end_date=pd.Timestamp(end).strftime("%Y%m%d"),
            is_open="1",
        )

    @staticmethod
    def _normalize_frame(frame: pd.DataFrame | None, benchmark_id: str) -> pd.DataFrame:
        """Normalize Tushare index_daily output into the local benchmark schema."""
        if frame is None or frame.empty:
            return pd.DataFrame()

        normalized = frame.copy()
        normalized["trade_date"] = pd.to_datetime(normalized["trade_date"]).dt.normalize()
        normalized["benchmark_id"] = str(benchmark_id)
        if "ts_code" in normalized.columns:
            normalized = normalized.rename(columns={"ts_code": "benchmark_code"})
        ordered_columns = [
            "benchmark_id",
            "benchmark_code",
            "trade_date",
            "close",
            "open",
            "high",
            "low",
            "pre_close",
            "change",
            "pct_chg",
            "vol",
            "amount",
        ]
        available_columns = [column for column in ordered_columns if column in normalized.columns]
        remainder = [column for column in normalized.columns if column not in available_columns]
        normalized = normalized.loc[:, available_columns + remainder]
        normalized = normalized.sort_values(["trade_date"]).drop_duplicates(subset=["trade_date"], keep="last")
        return normalized.reset_index(drop=True)
