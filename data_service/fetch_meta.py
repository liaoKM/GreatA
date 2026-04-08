"""Cutoff-only metadata helpers for the unified fetch pipeline."""

from __future__ import annotations

import json
from calendar import monthrange
from datetime import date, datetime
from pathlib import Path

import pandas as pd

from .benchmark_storage import OFFICIAL_TUSHARE_BENCHMARK_IDS, get_benchmark_root
from .fetcher.announcement_fetcher import ANNOUNCEMENT_TYPES


SUPPORTED_FETCH_TYPES: tuple[str, ...] = ("market", "benchmark", "finance", "announcement")
DEFAULT_FETCH_START = pd.Timestamp("2000-01-01")


def get_last_update_path(data_path: str | Path = "./data") -> Path:
    """Return the cutoff metadata path."""
    return Path(data_path) / "meta" / "last_update.json"


def _normalize_timestamp(value: pd.Timestamp | date | datetime | str | None) -> pd.Timestamp | None:
    if value in {None, ""}:
        return None
    return pd.Timestamp(value).normalize()


def _serialize_meta(meta: dict[str, pd.Timestamp | None]) -> dict[str, str | None]:
    return {
        data_type: cutoff.strftime("%Y-%m-%d") if cutoff is not None else None
        for data_type, cutoff in meta.items()
    }


def load_fetch_meta(data_path: str | Path = "./data") -> dict[str, pd.Timestamp | None]:
    """Load cutoff metadata, filling any missing keys with ``None``."""
    path = get_last_update_path(data_path)
    if not path.exists():
        return {data_type: None for data_type in SUPPORTED_FETCH_TYPES}

    raw = json.loads(path.read_text(encoding="utf-8"))
    meta = {data_type: _normalize_timestamp(raw.get(data_type)) for data_type in SUPPORTED_FETCH_TYPES}
    return meta


def save_fetch_meta(
    data_path: str | Path,
    meta: dict[str, pd.Timestamp | date | datetime | str | None],
) -> Path:
    """Persist cutoff metadata as ``YYYY-MM-DD`` strings."""
    normalized = {data_type: _normalize_timestamp(meta.get(data_type)) for data_type in SUPPORTED_FETCH_TYPES}
    path = get_last_update_path(data_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(_serialize_meta(normalized), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return path


def get_fetch_start(
    meta: dict[str, pd.Timestamp | None],
    data_type: str,
) -> pd.Timestamp:
    """Return the next incremental fetch start date for one data type."""
    cutoff = meta.get(str(data_type))
    if cutoff is None:
        return DEFAULT_FETCH_START
    return cutoff + pd.Timedelta(days=1)


def advance_fetch_meta(
    data_path: str | Path,
    data_type: str,
    cutoff: pd.Timestamp | date | datetime | str,
) -> dict[str, pd.Timestamp | None]:
    """Advance one data type cutoff and persist the metadata file."""
    meta = load_fetch_meta(data_path)
    meta[str(data_type)] = _normalize_timestamp(cutoff)
    save_fetch_meta(data_path, meta)
    return meta


def bootstrap_fetch_meta(data_path: str | Path = "./data") -> dict[str, pd.Timestamp | None]:
    """Initialize missing cutoffs by inferring them from the current data layout."""
    path = get_last_update_path(data_path)
    raw: dict[str, object] = {}
    if path.exists():
        raw = json.loads(path.read_text(encoding="utf-8"))

    meta = load_fetch_meta(data_path)
    changed = (not path.exists()) or any(data_type not in raw for data_type in SUPPORTED_FETCH_TYPES)

    for data_type in SUPPORTED_FETCH_TYPES:
        if meta[data_type] is not None:
            continue
        inferred = infer_fetch_cutoff(data_path, data_type)
        if inferred is not None:
            meta[data_type] = inferred
            changed = True

    if changed:
        save_fetch_meta(data_path, meta)
    return meta


def infer_fetch_cutoff(data_path: str | Path, data_type: str) -> pd.Timestamp | None:
    """Infer the highest fully covered date from persisted files."""
    root = Path(data_path)
    if data_type == "market":
        return _infer_root_cutoff(root / "market")
    if data_type == "finance":
        return _infer_root_cutoff(root / "finance")
    if data_type == "announcement":
        announcement_roots = [root / "announcement" / ann_type for ann_type in ANNOUNCEMENT_TYPES]
        return _infer_shared_cutoff(announcement_roots)
    if data_type == "benchmark":
        benchmark_roots = [get_benchmark_root(root, benchmark_id) for benchmark_id in OFFICIAL_TUSHARE_BENCHMARK_IDS]
        return _infer_shared_cutoff(benchmark_roots)
    raise ValueError(f"Unsupported data type for cutoff inference: {data_type}")


def _infer_shared_cutoff(roots: list[Path]) -> pd.Timestamp | None:
    cutoffs: list[pd.Timestamp] = []
    for root in roots:
        cutoff = _infer_root_cutoff(root)
        if cutoff is None:
            return None
        cutoffs.append(cutoff)
    if not cutoffs:
        return None
    return min(cutoffs)


def _infer_root_cutoff(root: Path) -> pd.Timestamp | None:
    candidates: list[pd.Timestamp] = []
    candidates.extend(_infer_yearly_dates(root / "yearly"))
    candidates.extend(_infer_monthly_dates(root / "monthly"))
    candidates.extend(_infer_daily_dates(root / "daily"))
    if not candidates:
        return None
    return max(candidates)


def _infer_yearly_dates(directory: Path) -> list[pd.Timestamp]:
    if not directory.exists():
        return []
    dates: list[pd.Timestamp] = []
    for path in directory.glob("*.parquet"):
        try:
            year = int(path.stem)
        except ValueError:
            continue
        dates.append(pd.Timestamp(year=year, month=12, day=31))
    return dates


def _infer_monthly_dates(directory: Path) -> list[pd.Timestamp]:
    if not directory.exists():
        return []
    dates: list[pd.Timestamp] = []
    for path in directory.glob("*.parquet"):
        try:
            timestamp = pd.Timestamp(path.stem)
        except ValueError:
            continue
        dates.append(pd.Timestamp(year=timestamp.year, month=timestamp.month, day=monthrange(timestamp.year, timestamp.month)[1]))
    return dates


def _infer_daily_dates(directory: Path) -> list[pd.Timestamp]:
    if not directory.exists():
        return []
    dates: list[pd.Timestamp] = []
    for path in directory.glob("*.parquet"):
        try:
            dates.append(pd.Timestamp(path.stem).normalize())
        except ValueError:
            continue
    return dates
