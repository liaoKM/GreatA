"""Helpers for persisted benchmark data."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

import pandas as pd


OFFICIAL_TUSHARE_BENCHMARK_IDS: tuple[str, ...] = (
    "tushare:000001.SH",
    "tushare:000300.SH",
    "tushare:399001.SZ",
)


@dataclass(frozen=True)
class BenchmarkPartition:
    """One benchmark partition span plus its destination key."""

    level: str
    start: pd.Timestamp
    end: pd.Timestamp
    key: str


def _as_timestamp(value: pd.Timestamp | date | datetime | str) -> pd.Timestamp:
    return pd.Timestamp(value).normalize()


def parse_benchmark_id(benchmark_id: str) -> tuple[str, str]:
    """Split a benchmark id into source and raw id."""
    source, separator, raw_id = str(benchmark_id).partition(":")
    if not separator or not source or not raw_id:
        raise ValueError(f"Invalid benchmark id: {benchmark_id}")
    return source, raw_id


def get_benchmark_root(data_path: str | Path, benchmark_id: str) -> Path:
    """Return the filesystem root for one benchmark id."""
    source, raw_id = parse_benchmark_id(benchmark_id)
    return Path(data_path) / "benchmark" / source / raw_id


def get_benchmark_partition_path(
    data_path: str | Path,
    benchmark_id: str,
    level: str,
    key: str,
) -> Path:
    """Return the filesystem path for one benchmark partition file."""
    if level not in {"daily", "monthly", "yearly"}:
        raise ValueError(f"Unsupported benchmark partition level: {level}")
    return get_benchmark_root(data_path, benchmark_id) / level / f"{key}.parquet"


def normalize_benchmark_frame(frame: pd.DataFrame, benchmark_id: str) -> pd.DataFrame:
    """Normalize one persisted benchmark frame."""
    if frame.empty:
        return pd.DataFrame(columns=["benchmark_id", "trade_date", "close"])
    normalized = frame.copy()
    if "trade_date" not in normalized.columns:
        raise ValueError("Benchmark frame is missing required column trade_date")
    normalized["trade_date"] = pd.to_datetime(normalized["trade_date"]).dt.normalize()
    normalized["benchmark_id"] = str(benchmark_id)
    normalized = normalized.sort_values(["trade_date"]).drop_duplicates(subset=["trade_date"], keep="last")
    return normalized.reset_index(drop=True)


def iter_benchmark_partitions(
    start: pd.Timestamp | date | datetime | str,
    end: pd.Timestamp | date | datetime | str,
) -> list[BenchmarkPartition]:
    """Split one natural-date range into yearly, monthly, and daily partitions."""
    current = _as_timestamp(start)
    end = _as_timestamp(end)
    partitions: list[BenchmarkPartition] = []

    while current <= end:
        year_end = pd.Timestamp(year=current.year, month=12, day=31)
        month_end = current + pd.offsets.MonthEnd(0)

        if current.month == 1 and current.day == 1 and end >= year_end:
            partitions.append(BenchmarkPartition("yearly", current, year_end, f"{current.year:04d}"))
            current = year_end + pd.Timedelta(days=1)
            continue

        if current.day == 1 and end >= month_end:
            partitions.append(BenchmarkPartition("monthly", current, month_end, f"{current.year:04d}-{current.month:02d}"))
            current = month_end + pd.Timedelta(days=1)
            continue

        partitions.append(BenchmarkPartition("daily", current, current, current.strftime("%Y-%m-%d")))
        current += pd.Timedelta(days=1)

    return partitions


def write_benchmark_partition(
    data_path: str | Path,
    benchmark_id: str,
    partition: BenchmarkPartition,
    frame: pd.DataFrame,
    overwrite: bool = True,
) -> Path | None:
    """Write one benchmark partition if it contains data."""
    partition_frame = frame[
        (frame["trade_date"] >= partition.start)
        & (frame["trade_date"] <= partition.end)
    ].copy()
    if partition_frame.empty:
        return None

    path = get_benchmark_partition_path(data_path, benchmark_id, partition.level, partition.key)
    if path.exists() and not overwrite:
        return path
    path.parent.mkdir(parents=True, exist_ok=True)
    partition_frame.to_parquet(path, index=False)
    return path


def write_partitioned_benchmark_range(
    data_path: str | Path,
    benchmark_id: str,
    frame: pd.DataFrame,
    start: pd.Timestamp | date | datetime | str,
    end: pd.Timestamp | date | datetime | str,
    overwrite: bool = True,
) -> list[Path]:
    """Write one benchmark frame into yearly/monthly/daily partitions."""
    normalized = normalize_benchmark_frame(frame, benchmark_id)
    written: list[Path] = []
    for partition in iter_benchmark_partitions(start, end):
        path = write_benchmark_partition(
            data_path=data_path,
            benchmark_id=benchmark_id,
            partition=partition,
            frame=normalized,
            overwrite=overwrite,
        )
        if path is not None:
            written.append(path)
    return written


def get_existing_benchmark_periods(data_path: str | Path, benchmark_id: str) -> dict[str, set[str | int]]:
    """Return the existing partition keys for one persisted benchmark."""
    root = get_benchmark_root(data_path, benchmark_id)
    existing: dict[str, set[str | int]] = {
        "yearly": set(),
        "monthly": set(),
        "daily": set(),
    }
    if not root.exists():
        return existing

    for path in (root / "yearly").glob("*.parquet"):
        try:
            existing["yearly"].add(int(path.stem))
        except ValueError:
            continue

    for path in (root / "monthly").glob("*.parquet"):
        try:
            pd.Timestamp(path.stem)
        except ValueError:
            continue
        existing["monthly"].add(path.stem)

    for path in (root / "daily").glob("*.parquet"):
        try:
            pd.Timestamp(path.stem)
        except ValueError:
            continue
        existing["daily"].add(path.stem)

    return existing
