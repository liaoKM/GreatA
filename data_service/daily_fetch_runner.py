"""Shared daily fetch execution helpers."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

import pandas as pd

from .fetch_status import write_fetch_status
from .fetcher.announcement_fetcher import ANNOUNCEMENT_TYPES
from .fetcher.tushare import (
    TushareAnnouncementFetcher,
    TushareFinanceFetcher,
    TushareMarketFetcher,
)


def _normalized_date(target_date: date | datetime) -> date:
    if isinstance(target_date, datetime):
        return target_date.date()
    return target_date


def _timestamp() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _df_row_count(df: pd.DataFrame | None) -> int:
    if df is None:
        return 0
    return int(len(df))


def _success_payload(
    data_type: str,
    target_date: date,
    row_count: int,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "data_type": data_type,
        "target_date": target_date.isoformat(),
        "fetched_at": _timestamp(),
        "status": "success" if row_count > 0 else "empty",
        "row_count": row_count,
    }
    if extra:
        payload.update(extra)
    return payload


def _error_payload(
    data_type: str,
    target_date: date,
    error: Exception | str,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "data_type": data_type,
        "target_date": target_date.isoformat(),
        "fetched_at": _timestamp(),
        "status": "error",
        "row_count": 0,
        "error": str(error),
    }
    if extra:
        payload.update(extra)
    return payload


def fetch_market_day(target_date: date | datetime, data_path: str = "./data") -> dict[str, Any]:
    """Fetch, save, and record status for one market day."""
    normalized_date = _normalized_date(target_date)
    fetcher = TushareMarketFetcher(data_path=data_path)

    try:
        df = fetcher.fetch_daily(normalized_date)
        row_count = _df_row_count(df)
        path = fetcher.save_daily(df, normalized_date) if row_count > 0 else None
        payload = _success_payload(
            "market",
            normalized_date,
            row_count,
            {"path": str(path)} if path else None,
        )
        write_fetch_status(data_path, "market", normalized_date, payload)
        return payload
    except Exception as exc:  # pragma: no cover - depends on remote API
        payload = _error_payload("market", normalized_date, exc)
        write_fetch_status(data_path, "market", normalized_date, payload)
        raise


def fetch_finance_day(target_date: date | datetime, data_path: str = "./data") -> dict[str, Any]:
    """Fetch, save, and record status for one finance day."""
    normalized_date = _normalized_date(target_date)
    fetcher = TushareFinanceFetcher(data_path=data_path)

    try:
        df = fetcher.fetch_daily(normalized_date)
        row_count = _df_row_count(df)
        path = fetcher.save_daily(df, normalized_date) if row_count > 0 else None
        payload = _success_payload(
            "finance",
            normalized_date,
            row_count,
            {"path": str(path)} if path else None,
        )
        write_fetch_status(data_path, "finance", normalized_date, payload)
        return payload
    except Exception as exc:  # pragma: no cover - depends on remote API
        payload = _error_payload("finance", normalized_date, exc)
        write_fetch_status(data_path, "finance", normalized_date, payload)
        raise


def fetch_announcement_day(
    target_date: date | datetime,
    data_path: str = "./data",
) -> dict[str, Any]:
    """Fetch, save, and record status for one announcement day."""
    normalized_date = _normalized_date(target_date)
    fetcher = TushareAnnouncementFetcher(data_path=data_path)

    type_results: dict[str, Any] = {}
    type_errors: dict[str, Exception] = {}
    total_rows = 0

    for ann_type in ANNOUNCEMENT_TYPES:
        try:
            df = fetcher.fetch_by_type(ann_type, normalized_date)
            row_count = _df_row_count(df)
            total_rows += row_count
            path = (
                fetcher.save_daily_by_type(df, ann_type, normalized_date)
                if row_count > 0
                else None
            )
            type_results[ann_type] = {
                "status": "success" if row_count > 0 else "empty",
                "row_count": row_count,
            }
            if path:
                type_results[ann_type]["path"] = str(path)
        except Exception as exc:  # pragma: no cover - depends on remote API
            type_results[ann_type] = {
                "status": "error",
                "row_count": 0,
                "error": str(exc),
            }
            type_errors[ann_type] = exc

    status = "error" if type_errors else ("success" if total_rows > 0 else "empty")
    payload: dict[str, Any] = {
        "data_type": "announcement",
        "target_date": normalized_date.isoformat(),
        "fetched_at": _timestamp(),
        "status": status,
        "row_count": total_rows,
        "types": type_results,
    }
    write_fetch_status(data_path, "announcement", normalized_date, payload)

    if type_errors:
        error_message = ", ".join(f"{ann_type}: {error}" for ann_type, error in type_errors.items())
        raise RuntimeError(f"Announcement fetch failed for {normalized_date}: {error_message}")

    return payload


def fetch_daily_data(
    target_date: date | datetime,
    data_type: str = "all",
    data_path: str = "./data",
) -> dict[str, dict[str, Any]]:
    """Fetch one day of data for one or more data types."""
    normalized_date = _normalized_date(target_date)
    results: dict[str, dict[str, Any]] = {}

    if data_type in ("market", "all"):
        results["market"] = fetch_market_day(normalized_date, data_path=data_path)
    if data_type in ("finance", "all"):
        results["finance"] = fetch_finance_day(normalized_date, data_path=data_path)
    if data_type in ("announcement", "all"):
        results["announcement"] = fetch_announcement_day(normalized_date, data_path=data_path)

    return results
