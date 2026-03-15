from .base import BaseFetcher
from .market_fetcher import MarketFetcher
from .finance_fetcher import FinanceFetcher
from .announcement_fetcher import AnnouncementFetcher

__all__ = [
    'BaseFetcher',
    'MarketFetcher', 'FinanceFetcher', 'AnnouncementFetcher',
]

# Tushare 实现需要安装 tushare 包
try:
    from .tushare import TushareMarketFetcher, TushareFinanceFetcher, TushareAnnouncementFetcher
    __all__.extend(['TushareMarketFetcher', 'TushareFinanceFetcher', 'TushareAnnouncementFetcher'])
except ImportError:
    pass  # tushare 未安装，跳过
