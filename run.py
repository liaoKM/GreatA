import argparse
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass
from os import cpu_count

import pandas as pd

from simple_backtrade.benchmark import CustomIndexConfig, CustomIndexEngine, FinanceGrowthIndexStrategy
from simple_backtrade.report.result import BacktestResult


@dataclass(frozen=True)
class YearlyTask:
    start: str
    end: str
    data_path: str
    initial_level: float
    warmup_bars: int
    benchmark: str | None


def _build_yearly_tasks(
    start: str,
    end: str,
    data_path: str,
    initial_level: float,
    warmup_bars: int,
    benchmark: str | None,
) -> list[YearlyTask]:
    start_ts = pd.Timestamp(start).normalize()
    end_ts = pd.Timestamp(end).normalize()
    if start_ts > end_ts:
        raise ValueError("start must be on or before end")

    tasks: list[YearlyTask] = []
    current_start = start_ts
    while current_start <= end_ts:
        year_end = min(pd.Timestamp(year=current_start.year, month=12, day=31), end_ts)
        tasks.append(
            YearlyTask(
                start=current_start.strftime("%Y-%m-%d"),
                end=year_end.strftime("%Y-%m-%d"),
                data_path=data_path,
                initial_level=initial_level,
                warmup_bars=warmup_bars,
                benchmark=benchmark,
            )
        )
        current_start = year_end + pd.Timedelta(days=1)
    return tasks


def _run_yearly_task(task: YearlyTask) -> BacktestResult:
    return CustomIndexEngine(
        CustomIndexConfig(
            start=task.start,
            end=task.end,
            data_path=task.data_path,
            initial_level=task.initial_level,
            warmup_bars=task.warmup_bars,
            benchmark=task.benchmark,
        ),
        FinanceGrowthIndexStrategy(),
    ).run()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the local custom index benchmark in parallel by year")
    parser.add_argument("--start", required=True, help="Backtest start date, for example 2020-01-01")
    parser.add_argument("--end", required=True, help="Backtest end date, for example 2026-12-31")
    parser.add_argument("--data-path", default="./data", help="Root data directory")
    parser.add_argument("--initial-level", type=float, default=1_000.0, help="Initial custom index level")
    parser.add_argument("--warmup-bars", type=int, default=60, help="Warmup bars loaded before each segment")
    parser.add_argument("--benchmark", default="tushare:000300.SH", help="Local benchmark id used for comparison")
    parser.add_argument(
        "--workers",
        type=int,
        default=cpu_count()/4,
        help="Number of worker processes used for yearly tasks",
    )
    args = parser.parse_args()

    if args.workers==1:
        result=CustomIndexEngine(
            CustomIndexConfig(
                start=args.start,
                end=args.end,
                data_path=args.data_path,
                initial_level=args.initial_level,
                warmup_bars=args.warmup_bars,
                benchmark=args.benchmark,
            ),
            FinanceGrowthIndexStrategy(),
        ).run()
        result.plot(show=True)
    else:
        tasks = _build_yearly_tasks(
            start=args.start,
            end=args.end,
            data_path=args.data_path,
            initial_level=args.initial_level,
            warmup_bars=args.warmup_bars,
            benchmark=args.benchmark,
        )

        max_workers = max(1, min(int(args.workers), len(tasks)))
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            results = list(executor.map(_run_yearly_task, tasks))

        merged_result = BacktestResult.merge(results)
        merged_result.plot(show=True)

    return


if __name__ == "__main__":
    main()
