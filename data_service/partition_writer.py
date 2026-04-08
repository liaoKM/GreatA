"""Unified partition writer for fetched and derived data."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

import pandas as pd

from .fetch_meta import advance_fetch_meta
from .fetcher.announcement_fetcher import ANNOUNCEMENT_TYPES


DATE_COLUMN_BY_TYPE: dict[str, str] = {
    "market": "trade_date",
    "finance": "ann_date",
    "announcement": "ann_date",
    "benchmark": "trade_date",
}


@dataclass(frozen=True)
class DatePartition:
    """One natural-date partition span plus its storage key."""

    level: str
    start: pd.Timestamp
    end: pd.Timestamp
    key: str


def iter_date_partitions(
    start: pd.Timestamp | date | datetime | str,
    end: pd.Timestamp | date | datetime | str,
) -> list[DatePartition]:
    """Split one natural-date range into yearly, monthly, and daily partitions."""
    current = pd.Timestamp(start).normalize()
    limit = pd.Timestamp(end).normalize()
    partitions: list[DatePartition] = []

    while current <= limit:
        year_end = pd.Timestamp(year=current.year, month=12, day=31)
        month_end = current + pd.offsets.MonthEnd(0)

        if current.month == 1 and current.day == 1 and limit >= year_end:
            partitions.append(DatePartition("yearly", current, year_end, f"{current.year:04d}"))
            current = year_end + pd.Timedelta(days=1)
            continue

        if current.day == 1 and limit >= month_end:
            partitions.append(DatePartition("monthly", current, month_end, f"{current.year:04d}-{current.month:02d}"))
            current = month_end + pd.Timedelta(days=1)
            continue

        partitions.append(DatePartition("daily", current, current, current.strftime("%Y-%m-%d")))
        current += pd.Timedelta(days=1)

    return partitions


def write_partitioned_data(
    data_path: str | Path,
    data_type: str,
    frame: pd.DataFrame | None,
    start: pd.Timestamp | date | datetime | str,
    end: pd.Timestamp | date | datetime | str,
    update_meta: bool = True,
) -> list[Path]:
    """Persist one fetched dataframe into yearly/monthly/daily partitions."""
    normalized_start = pd.Timestamp(start).normalize()
    normalized_end = pd.Timestamp(end).normalize()
    normalized = normalize_frame_by_type(data_type, frame)

    written: list[Path] = []
    if data_type == "announcement":
        if not normalized.empty:
            for ann_type, group in normalized.groupby("ann_type", sort=True):
                written.extend(
                    _write_root_partitions(
                        _announcement_root(data_path, str(ann_type)),
                        DATE_COLUMN_BY_TYPE[data_type],
                        group,
                        normalized_start,
                        normalized_end,
                    )
                )
    elif data_type == "benchmark":
        if not normalized.empty:
            for benchmark_id, group in normalized.groupby("benchmark_id", sort=True):
                written.extend(
                    _write_root_partitions(
                        _benchmark_root(data_path, str(benchmark_id)),
                        DATE_COLUMN_BY_TYPE[data_type],
                        group,
                        normalized_start,
                        normalized_end,
                    )
                )
    else:
        written.extend(
            _write_root_partitions(
                Path(data_path) / data_type,
                DATE_COLUMN_BY_TYPE[data_type],
                normalized,
                normalized_start,
                normalized_end,
            )
        )

    if update_meta:
        advance_fetch_meta(data_path, data_type, normalized_end)
    return written


def normalize_frame_by_type(data_type: str, frame: pd.DataFrame | None) -> pd.DataFrame:
    """Normalize dataframe columns needed by the unified writer."""
    if frame is None or frame.empty:
        return pd.DataFrame()

    normalized = frame.copy()
    if data_type not in DATE_COLUMN_BY_TYPE:
        raise ValueError(f"Unsupported data type: {data_type}")

    date_column = DATE_COLUMN_BY_TYPE[data_type]
    if date_column not in normalized.columns:
        raise ValueError(f"{data_type} frame is missing required date column {date_column}")

    normalized[date_column] = pd.to_datetime(normalized[date_column]).dt.normalize()

    if data_type == "announcement":
        if "ann_type" not in normalized.columns:
            raise ValueError("announcement frame is missing required column ann_type")
        normalized["ann_type"] = normalized["ann_type"].astype(str)
        valid_types = set(ANNOUNCEMENT_TYPES)
        invalid_types = sorted(set(normalized["ann_type"]) - valid_types)
        if invalid_types:
            raise ValueError(f"Unsupported announcement subtypes: {invalid_types}")
        normalized = normalized.sort_values(["ann_type", date_column]).reset_index(drop=True)
        return normalized

    if data_type == "benchmark":
        if "benchmark_id" not in normalized.columns:
            raise ValueError("benchmark frame is missing required column benchmark_id")
        normalized["benchmark_id"] = normalized["benchmark_id"].astype(str)
        normalized = normalized.sort_values(["benchmark_id", date_column]).reset_index(drop=True)
        return normalized

    normalized = normalized.sort_values([date_column]).reset_index(drop=True)
    return normalized


def _announcement_root(data_path: str | Path, ann_type: str) -> Path:
    return Path(data_path) / "announcement" / ann_type


def _benchmark_root(data_path: str | Path, benchmark_id: str) -> Path:
    source, separator, raw_id = str(benchmark_id).partition(":")
    if not separator or not source or not raw_id:
        raise ValueError(f"Invalid benchmark id: {benchmark_id}")
    return Path(data_path) / "benchmark" / source / raw_id


def _write_root_partitions(
    root: Path,
    date_column: str,
    frame: pd.DataFrame,
    start: pd.Timestamp,
    end: pd.Timestamp,
) -> list[Path]:
    if frame.empty:
        return []

    written: list[Path] = []
    for partition in iter_date_partitions(start, end):
        partition_frame = frame[
            (frame[date_column] >= partition.start)
            & (frame[date_column] <= partition.end)
        ].copy()
        if partition_frame.empty:
            continue

        path = root / partition.level / f"{partition.key}.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)
        partition_frame.to_parquet(path, index=False)
        written.append(path)

    return written
