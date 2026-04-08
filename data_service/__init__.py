from .aggregator.announcement_aggregator import AnnouncementAggregator
from .aggregator.base import BaseAggregator
from .aggregator.finance_aggregator import FinanceAggregator
from .aggregator.market_aggregator import MarketAggregator
from .consolidation import consolidate_data_type
from .fetch_meta import (
    DEFAULT_FETCH_START,
    SUPPORTED_FETCH_TYPES,
    advance_fetch_meta,
    bootstrap_fetch_meta,
    get_fetch_start,
    get_last_update_path,
    infer_fetch_cutoff,
    load_fetch_meta,
    save_fetch_meta,
)
from .fetch_pipeline import FetchRunResult, run_fetch
from .fetcher.announcement_fetcher import AnnouncementFetcher
from .fetcher.base import BaseFetcher
from .fetcher.benchmark_fetcher import BenchmarkFetcher
from .fetcher.finance_fetcher import FinanceFetcher
from .fetcher.market_fetcher import MarketFetcher
from .partition_writer import DatePartition, iter_date_partitions, normalize_frame_by_type, write_partitioned_data

__all__ = [
    "BaseFetcher",
    "BenchmarkFetcher",
    "MarketFetcher",
    "FinanceFetcher",
    "AnnouncementFetcher",
    "BaseAggregator",
    "MarketAggregator",
    "FinanceAggregator",
    "AnnouncementAggregator",
    "DEFAULT_FETCH_START",
    "SUPPORTED_FETCH_TYPES",
    "DatePartition",
    "FetchRunResult",
    "advance_fetch_meta",
    "bootstrap_fetch_meta",
    "consolidate_data_type",
    "get_fetch_start",
    "get_last_update_path",
    "infer_fetch_cutoff",
    "iter_date_partitions",
    "load_fetch_meta",
    "normalize_frame_by_type",
    "run_fetch",
    "save_fetch_meta",
    "write_partitioned_data",
]

try:
    from .fetcher.tushare import (
        TushareAnnouncementFetcher,
        TushareBenchmarkFetcher,
        TushareFinanceFetcher,
        TushareMarketFetcher,
    )

    __all__.extend(
        [
            "TushareMarketFetcher",
            "TushareFinanceFetcher",
            "TushareAnnouncementFetcher",
            "TushareBenchmarkFetcher",
        ]
    )
except ImportError:
    pass
