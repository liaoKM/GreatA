from .announcement_fetcher import AnnouncementFetcher
from .base import BaseFetcher
from .benchmark_fetcher import BenchmarkFetcher
from .finance_fetcher import FinanceFetcher
from .market_fetcher import MarketFetcher

__all__ = [
    "BaseFetcher",
    "BenchmarkFetcher",
    "MarketFetcher",
    "FinanceFetcher",
    "AnnouncementFetcher",
]

try:
    from .tushare import (
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
