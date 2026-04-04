"""Utilities for daily fetch status files."""

from __future__ import annotations

import json
from calendar import monthrange
from datetime import date, datetime
from pathlib import Path
from typing import Any


VALID_STATUSES = {"success", "empty", "error"}


def _normalize_date(value: date | datetime) -> date:
    if isinstance(value, datetime):
        return value.date()
    return value


def iter_month_dates(year: int, month: int) -> list[date]:
    """Return every natural day in the target month."""
    return [
        date(year, month, day)
        for day in range(1, monthrange(year, month)[1] + 1)
    ]


def get_fetch_status_root(data_path: str | Path = "./data") -> Path:
    """Return the root directory for fetch status files."""
    return Path(data_path) / "meta" / "fetch_status"


def get_fetch_status_path(
    data_path: str | Path,
    data_type: str,
    target_date: date | datetime,
) -> Path:
    """Return the status file path for one day of one data type."""
    normalized_date = _normalize_date(target_date)
    return get_fetch_status_root(data_path) / data_type / f"{normalized_date:%Y-%m-%d}.json"


def _to_jsonable(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dict):
        return {str(key): _to_jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_to_jsonable(item) for item in value]
    return value


def write_fetch_status(
    data_path: str | Path,
    data_type: str,
    target_date: date | datetime,
    payload: dict[str, Any],
) -> Path:
    """Persist a daily fetch status payload."""
    status_path = get_fetch_status_path(data_path, data_type, target_date)
    status_path.parent.mkdir(parents=True, exist_ok=True)
    status_path.write_text(
        json.dumps(_to_jsonable(payload), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return status_path


def read_fetch_status(
    data_path: str | Path,
    data_type: str,
    target_date: date | datetime,
) -> dict[str, Any] | None:
    """Read a daily fetch status payload if it exists."""
    status_path = get_fetch_status_path(data_path, data_type, target_date)
    if not status_path.exists():
        return None
    return json.loads(status_path.read_text(encoding="utf-8"))


def delete_fetch_status(
    data_path: str | Path,
    data_type: str,
    target_date: date | datetime,
) -> bool:
    """Delete a daily fetch status file if it exists."""
    status_path = get_fetch_status_path(data_path, data_type, target_date)
    if not status_path.exists():
        return False
    status_path.unlink()
    return True


def summarize_month_status(
    data_path: str | Path,
    data_type: str,
    year: int,
    month: int,
    announcement_types: list[str] | None = None,
) -> dict[str, Any]:
    """Check whether one month has complete, non-error daily fetch status files."""
    summary: dict[str, Any] = {
        "complete": True,
        "missing_dates": [],
        "error_dates": [],
        "missing_types": {},
        "error_types": {},
    }

    for current_date in iter_month_dates(year, month):
        payload = read_fetch_status(data_path, data_type, current_date)
        if payload is None:
            summary["missing_dates"].append(current_date)
            continue

        status = payload.get("status")
        if status not in VALID_STATUSES or status == "error":
            summary["error_dates"].append(current_date)

        if data_type != "announcement" or not announcement_types:
            continue

        type_payloads = payload.get("types")
        if not isinstance(type_payloads, dict):
            summary["missing_types"][current_date.isoformat()] = list(announcement_types)
            continue

        missing_types: list[str] = []
        error_types: list[str] = []
        for ann_type in announcement_types:
            ann_payload = type_payloads.get(ann_type)
            if not isinstance(ann_payload, dict):
                missing_types.append(ann_type)
                continue

            ann_status = ann_payload.get("status")
            if ann_status == "error":
                error_types.append(ann_type)
            elif ann_status not in {"success", "empty"}:
                missing_types.append(ann_type)

        if missing_types:
            summary["missing_types"][current_date.isoformat()] = missing_types
        if error_types:
            summary["error_types"][current_date.isoformat()] = error_types

    has_issues = any(
        (
            summary["missing_dates"],
            summary["error_dates"],
            summary["missing_types"],
            summary["error_types"],
        )
    )
    summary["complete"] = not has_issues
    return summary
