"""File-only consolidation for the unified fetch pipeline."""

from __future__ import annotations

from calendar import monthrange
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

import pandas as pd

from .benchmark_storage import OFFICIAL_TUSHARE_BENCHMARK_IDS, get_benchmark_root
from .fetcher.announcement_fetcher import ANNOUNCEMENT_TYPES


DeduplicateFn = Callable[[pd.DataFrame], pd.DataFrame]


@dataclass(frozen=True)
class RootSpec:
    """One physical storage root plus the rules needed to consolidate it."""

    root: Path
    date_column: str
    deduplicate: DeduplicateFn
    require_all_months_for_year: bool


def consolidate_data_type(
    data_type: str,
    cutoff: pd.Timestamp | str,
    data_path: str | Path = "./data",
) -> dict[str, list[str]]:
    """Consolidate one data type up to the provided cutoff."""
    normalized_cutoff = pd.Timestamp(cutoff).normalize()
    results: dict[str, list[str]] = {}
    for key, spec in _get_root_specs(data_type, data_path).items():
        actions = _consolidate_root(spec, normalized_cutoff)
        if actions:
            results[key] = actions
    return results


def _get_root_specs(data_type: str, data_path: str | Path) -> dict[str, RootSpec]:
    root = Path(data_path)
    if data_type == "market":
        return {
            "market": RootSpec(
                root=root / "market",
                date_column="trade_date",
                deduplicate=_identity,
                require_all_months_for_year=True,
            )
        }
    if data_type == "finance":
        return {
            "finance": RootSpec(
                root=root / "finance",
                date_column="ann_date",
                deduplicate=_deduplicate_finance_like,
                require_all_months_for_year=False,
            )
        }
    if data_type == "announcement":
        return {
            ann_type: RootSpec(
                root=root / "announcement" / ann_type,
                date_column="ann_date",
                deduplicate=_announcement_deduplicator(ann_type),
                require_all_months_for_year=False,
            )
            for ann_type in ANNOUNCEMENT_TYPES
        }
    if data_type == "benchmark":
        return {
            benchmark_id: RootSpec(
                root=get_benchmark_root(root, benchmark_id),
                date_column="trade_date",
                deduplicate=_deduplicate_benchmark,
                require_all_months_for_year=True,
            )
            for benchmark_id in OFFICIAL_TUSHARE_BENCHMARK_IDS
        }
    raise ValueError(f"Unsupported data type for consolidation: {data_type}")


def _consolidate_root(spec: RootSpec, cutoff: pd.Timestamp) -> list[str]:
    if not spec.root.exists():
        return []

    actions: list[str] = []
    for year in _collect_candidate_years(spec.root):
        if _cleanup_year_if_covered(spec.root, year):
            actions.append(f"cleanup-year:{year}")
            continue

        for month in _collect_month_candidates(spec.root, year):
            if _cleanup_month_if_covered(spec.root, year, month):
                actions.append(f"cleanup-month:{year:04d}-{month:02d}")
                continue
            if _aggregate_month(spec, year, month, cutoff):
                actions.append(f"aggregate-month:{year:04d}-{month:02d}")

        if _aggregate_year(spec, year, cutoff):
            actions.append(f"aggregate-year:{year:04d}")
            _cleanup_year_if_covered(spec.root, year)

    return actions


def _collect_candidate_years(root: Path) -> list[int]:
    years: set[int] = set()

    for path in (root / "yearly").glob("*.parquet"):
        try:
            years.add(int(path.stem))
        except ValueError:
            continue

    for path in (root / "monthly").glob("*.parquet"):
        try:
            timestamp = pd.Timestamp(path.stem)
        except ValueError:
            continue
        years.add(timestamp.year)

    for path in (root / "daily").glob("*.parquet"):
        try:
            timestamp = pd.Timestamp(path.stem)
        except ValueError:
            continue
        years.add(timestamp.year)

    return sorted(years)


def _collect_month_candidates(root: Path, year: int) -> list[int]:
    months: set[int] = set()
    for path in (root / "monthly").glob(f"{year:04d}-*.parquet"):
        try:
            months.add(pd.Timestamp(path.stem).month)
        except ValueError:
            continue

    for path in (root / "daily").glob(f"{year:04d}-*.parquet"):
        try:
            months.add(pd.Timestamp(path.stem).month)
        except ValueError:
            continue

    return sorted(months)


def _cleanup_month_if_covered(root: Path, year: int, month: int) -> bool:
    monthly_path = root / "monthly" / f"{year:04d}-{month:02d}.parquet"
    if not monthly_path.exists():
        return False

    deleted = 0
    for path in (root / "daily").glob(f"{year:04d}-{month:02d}-*.parquet"):
        path.unlink()
        deleted += 1
    return deleted > 0


def _cleanup_year_if_covered(root: Path, year: int) -> bool:
    yearly_path = root / "yearly" / f"{year:04d}.parquet"
    if not yearly_path.exists():
        return False

    deleted = 0
    for path in (root / "monthly").glob(f"{year:04d}-*.parquet"):
        path.unlink()
        deleted += 1
    for path in (root / "daily").glob(f"{year:04d}-*.parquet"):
        path.unlink()
        deleted += 1
    return deleted > 0


def _aggregate_month(spec: RootSpec, year: int, month: int, cutoff: pd.Timestamp) -> bool:
    monthly_path = spec.root / "monthly" / f"{year:04d}-{month:02d}.parquet"
    if monthly_path.exists():
        return False
    if pd.Timestamp(year=year, month=month, day=monthrange(year, month)[1]) > cutoff:
        return False

    daily_files = sorted((spec.root / "daily").glob(f"{year:04d}-{month:02d}-*.parquet"))
    if not daily_files:
        return False

    frame = _concat_parquet_files(daily_files, spec.date_column)
    frame = spec.deduplicate(frame)
    monthly_path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(monthly_path, index=False)
    for path in daily_files:
        path.unlink()
    return True


def _aggregate_year(spec: RootSpec, year: int, cutoff: pd.Timestamp) -> bool:
    yearly_path = spec.root / "yearly" / f"{year:04d}.parquet"
    if yearly_path.exists():
        return False
    if pd.Timestamp(year=year, month=12, day=31) > cutoff:
        return False

    monthly_files = sorted((spec.root / "monthly").glob(f"{year:04d}-*.parquet"))
    if spec.require_all_months_for_year and len(monthly_files) < 12:
        return False
    if not monthly_files:
        return False

    frame = _concat_parquet_files(monthly_files, spec.date_column)
    frame = spec.deduplicate(frame)
    yearly_path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(yearly_path, index=False)
    for path in monthly_files:
        path.unlink()
    return True


def _concat_parquet_files(files: list[Path], date_column: str) -> pd.DataFrame:
    if not files:
        return pd.DataFrame()

    frames = [pd.read_parquet(path) for path in files]
    frame = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    if frame.empty or date_column not in frame.columns:
        return frame
    frame = frame.copy()
    frame[date_column] = pd.to_datetime(frame[date_column]).dt.normalize()
    return frame


def _identity(frame: pd.DataFrame) -> pd.DataFrame:
    return frame


def _deduplicate_finance_like(frame: pd.DataFrame) -> pd.DataFrame:
    return _deduplicate_by_latest(frame, ["stock_code", "end_date"])


def _announcement_deduplicator(ann_type: str) -> DeduplicateFn:
    if ann_type in {"forecast", "express"}:
        return _deduplicate_finance_like
    if ann_type in {"repurchase", "holdertrade"}:
        return _drop_exact_duplicates
    return _identity


def _deduplicate_benchmark(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame
    if "trade_date" not in frame.columns:
        return frame.drop_duplicates().reset_index(drop=True)
    working = frame.copy()
    working["trade_date"] = pd.to_datetime(working["trade_date"]).dt.normalize()
    working = working.sort_values(["trade_date"]).drop_duplicates(subset=["trade_date"], keep="last")
    return working.reset_index(drop=True)


def _drop_exact_duplicates(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame
    return frame.drop_duplicates().reset_index(drop=True)


def _deduplicate_by_latest(
    frame: pd.DataFrame,
    subset: list[str],
    date_column: str = "ann_date",
    flag_column: str = "update_flag",
) -> pd.DataFrame:
    if frame.empty or any(column not in frame.columns for column in subset):
        return _drop_exact_duplicates(frame)

    working = frame.copy()
    helper_columns: list[str] = []
    sort_columns = list(subset)
    ascending = [True] * len(subset)

    if date_column in working.columns:
        working["_sort_date"] = pd.to_datetime(working[date_column], errors="coerce")
        helper_columns.append("_sort_date")
        sort_columns.append("_sort_date")
        ascending.append(False)

    if flag_column in working.columns:
        working["_sort_flag"] = pd.to_numeric(working[flag_column], errors="coerce").fillna(0)
        helper_columns.append("_sort_flag")
        sort_columns.append("_sort_flag")
        ascending.append(False)

    working["_sort_index"] = range(len(working))
    helper_columns.append("_sort_index")
    sort_columns.append("_sort_index")
    ascending.append(False)

    working = working.sort_values(sort_columns, ascending=ascending, kind="mergesort")
    working = working.drop_duplicates(subset=subset, keep="first")
    return working.drop(columns=helper_columns).reset_index(drop=True)
