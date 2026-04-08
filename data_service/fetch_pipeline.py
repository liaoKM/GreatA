"""Unified fetch orchestration for all persisted data types."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

import pandas as pd

from .consolidation import consolidate_data_type
from .fetch_meta import bootstrap_fetch_meta, get_fetch_start
from .fetcher.tushare import (
    TushareAnnouncementFetcher,
    TushareBenchmarkFetcher,
    TushareFinanceFetcher,
    TushareMarketFetcher,
)
from .partition_writer import write_partitioned_data


FETCH_ORDER: tuple[str, ...] = ("market", "benchmark", "finance", "announcement")
FETCHER_FACTORIES = {
    "market": TushareMarketFetcher,
    "benchmark": TushareBenchmarkFetcher,
    "finance": TushareFinanceFetcher,
    "announcement": TushareAnnouncementFetcher,
}


@dataclass(frozen=True)
class FetchRunResult:
    """Structured output for one unified fetch task."""

    data_type: str
    start: pd.Timestamp
    end: pd.Timestamp
    status: str
    row_count: int
    written_paths: list[str]
    consolidation_actions: list[str]

    def to_dict(self) -> dict[str, object]:
        return {
            "data_type": self.data_type,
            "start": self.start.strftime("%Y-%m-%d"),
            "end": self.end.strftime("%Y-%m-%d"),
            "status": self.status,
            "row_count": self.row_count,
            "written_paths": self.written_paths,
            "consolidation_actions": self.consolidation_actions,
        }


def run_fetch(
    until: date | datetime | str,
    data_type: str = "all",
    data_path: str | Path = "./data",
) -> dict[str, FetchRunResult]:
    """Run the unified incremental fetch workflow."""
    normalized_until = pd.Timestamp(until).normalize()
    meta = bootstrap_fetch_meta(data_path)

    target_types = FETCH_ORDER if data_type == "all" else (str(data_type),)
    results: dict[str, FetchRunResult] = {}

    for current_type in target_types:
        start = get_fetch_start(meta, current_type)
        if start > normalized_until:
            results[current_type] = FetchRunResult(
                data_type=current_type,
                start=start,
                end=normalized_until,
                status="noop",
                row_count=0,
                written_paths=[],
                consolidation_actions=[],
            )
            continue

        fetcher = FETCHER_FACTORIES[current_type](data_path=str(data_path))
        frame = fetcher.fetch_history(start.to_pydatetime(), normalized_until.to_pydatetime())
        written_paths = [
            str(path)
            for path in write_partitioned_data(
                data_path=data_path,
                data_type=current_type,
                frame=frame,
                start=start,
                end=normalized_until,
                update_meta=True,
            )
        ]
        consolidation = consolidate_data_type(
            current_type,
            cutoff=normalized_until,
            data_path=data_path,
        )
        actions = [
            f"{scope}:{action}"
            for scope, scope_actions in consolidation.items()
            for action in scope_actions
        ]

        results[current_type] = FetchRunResult(
            data_type=current_type,
            start=start,
            end=normalized_until,
            status="success",
            row_count=0 if frame is None else int(len(frame)),
            written_paths=written_paths,
            consolidation_actions=actions,
        )
        meta[current_type] = normalized_until

    return results
