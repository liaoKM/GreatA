"""Helpers for persisted benchmark data paths and compatibility wrappers."""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

import pandas as pd


OFFICIAL_TUSHARE_BENCHMARK_IDS: tuple[str, ...] = (
    "tushare:000001.SH",
    "tushare:000300.SH",
    "tushare:399001.SZ",
)


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
    """Normalize one benchmark dataframe into the local schema."""
    if frame is None or frame.empty:
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
) -> list:
    """Compatibility wrapper for benchmark partition planning."""
    from .partition_writer import iter_date_partitions

    return iter_date_partitions(start, end)


def write_partitioned_benchmark_range(
    data_path: str | Path,
    benchmark_id: str,
    frame: pd.DataFrame,
    start: pd.Timestamp | date | datetime | str,
    end: pd.Timestamp | date | datetime | str,
    overwrite: bool = True,
) -> list[Path]:
    """Persist one benchmark range through the unified partition writer."""
    del overwrite
    from .partition_writer import write_partitioned_data

    normalized = normalize_benchmark_frame(frame, benchmark_id)
    return write_partitioned_data(
        data_path=data_path,
        data_type="benchmark",
        frame=normalized,
        start=start,
        end=end,
        update_meta=False,
    )


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
