from .aggregator.announcement_aggregator import AnnouncementAggregator
from .aggregator.base import BaseAggregator
from .aggregator.finance_aggregator import FinanceAggregator
from .aggregator.market_aggregator import MarketAggregator
from .fetcher.announcement_fetcher import AnnouncementFetcher
from .fetcher.base import BaseFetcher
from .fetcher.benchmark_fetcher import BenchmarkFetcher
from .fetcher.finance_fetcher import FinanceFetcher
from .fetcher.market_fetcher import MarketFetcher

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
