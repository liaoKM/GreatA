# 数据服务模块 - 负责数据拉取和整合
# 此模块为在线模块，依赖外部数据源（tushare等）

from .fetcher.base import BaseFetcher
from .fetcher.market_fetcher import MarketFetcher
from .fetcher.finance_fetcher import FinanceFetcher
from .fetcher.announcement_fetcher import AnnouncementFetcher

from .aggregator.base import BaseAggregator
from .aggregator.market_aggregator import MarketAggregator
from .aggregator.finance_aggregator import FinanceAggregator
from .aggregator.announcement_aggregator import AnnouncementAggregator

__all__ = [
    # 基类
    'BaseFetcher', 'MarketFetcher', 'FinanceFetcher', 'AnnouncementFetcher',
    'BaseAggregator', 'MarketAggregator', 'FinanceAggregator', 'AnnouncementAggregator',
]

# Tushare 实现需要安装 tushare 包
try:
    from .fetcher.tushare import TushareMarketFetcher, TushareFinanceFetcher, TushareAnnouncementFetcher
    __all__.extend(['TushareMarketFetcher', 'TushareFinanceFetcher', 'TushareAnnouncementFetcher'])
except ImportError:
    pass  # tushare 未安装，跳过
