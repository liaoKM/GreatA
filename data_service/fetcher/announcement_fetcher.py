"""公告数据拉取器基类"""
from abc import abstractmethod
from datetime import datetime
from pathlib import Path
import pandas as pd
from .base import BaseFetcher


# 公告子类型
ANNOUNCEMENT_TYPES = {
    'forecast': '业绩预告',
    'express': '业绩快报',
    'repurchase': '股票回购',
    'holdertrade': '股东增减持',
    #'broker_forecast': '券商预测',
}


class AnnouncementFetcher(BaseFetcher):
    """公告数据拉取器基类

    公告数据按类型分目录存储:
    data/announcement/
    ├── forecast/daily/, monthly/, yearly/
    ├── express/daily/, monthly/, yearly/
    ├── repurchase/daily/, monthly/, yearly/
    └── holdertrade/daily/, monthly/, yearly/
    """

    DATA_TYPE = 'announcement'

    def __init__(self, data_path: str = './data'):
        super().__init__(data_path)
        # 为每种公告类型创建子路径
        self.type_paths = {}
        for ann_type in ANNOUNCEMENT_TYPES:
            type_path = self.data_path / self.DATA_TYPE / ann_type
            self.type_paths[ann_type] = {
                'daily': type_path / 'daily',
                'monthly': type_path / 'monthly',
                'yearly': type_path / 'yearly',
            }

    def _ensure_type_paths(self, ann_type: str):
        """确保指定类型的目录存在"""
        for path in self.type_paths[ann_type].values():
            path.mkdir(parents=True, exist_ok=True)

    @abstractmethod
    def fetch_by_type(self, ann_type: str, date: datetime) -> pd.DataFrame:
        """
        拉取指定类型的单日公告数据

        Args:
            ann_type: 公告类型 (forecast/express/repurchase/holdertrade)
            date: 公告日期

        Returns:
            DataFrame包含当日该类型的所有公告
        """
        pass

    @abstractmethod
    def fetch_history_by_type(self, ann_type: str, start: datetime, end: datetime) -> pd.DataFrame:
        """
        拉取指定类型的历史公告数据

        Args:
            ann_type: 公告类型
            start: 开始日期
            end: 结束日期

        Returns:
            DataFrame包含时间范围内该类型的所有公告
        """
        pass

    def save_daily_by_type(self, df: pd.DataFrame, ann_type: str, date: datetime) -> Path:
        """
        保存指定类型的日增量数据

        Args:
            df: 数据
            ann_type: 公告类型
            date: 日期

        Returns:
            保存的文件路径
        """

        self._ensure_type_paths(ann_type)
        filename = f"{date.strftime('%Y-%m-%d')}.parquet"
        filepath = self.type_paths[ann_type]['daily'] / filename
        df.to_parquet(filepath, index=False)
        return filepath

    def save_monthly_by_type(self, df: pd.DataFrame, ann_type: str, year: int, month: int) -> Path:
        """
        保存指定类型的月度数据

        Args:
            df: 数据
            ann_type: 公告类型
            year: 年份
            month: 月份

        Returns:
            保存的文件路径
        """

        self._ensure_type_paths(ann_type)
        filename = f"{year}-{month:02d}.parquet"
        filepath = self.type_paths[ann_type]['monthly'] / filename
        df.to_parquet(filepath, index=False)
        return filepath

    def fetch_daily(self, date: datetime, ann_type: str = None) -> dict:
        """
        拉取单日公告数据

        Args:
            date: 公告日期
            ann_type: 公告类型，None 表示拉取所有类型

        Returns:
            如果指定 ann_type，返回 DataFrame
            如果 ann_type 为 None，返回 dict {ann_type: DataFrame}
        """
        if ann_type is not None:
            return self.fetch_by_type(ann_type, date)

        # 拉取所有类型
        result = {}
        for ann_type in ANNOUNCEMENT_TYPES:
            df = self.fetch_by_type(ann_type, date)
            if df is not None and not df.empty:
                result[ann_type] = df
        return result

    def fetch_history(self, start: datetime, end: datetime, ann_type: str = None) -> dict:
        """
        拉取历史公告数据

        Args:
            start: 开始日期
            end: 结束日期
            ann_type: 公告类型，None 表示拉取所有类型

        Returns:
            如果指定 ann_type，返回 DataFrame
            如果 ann_type 为 None，返回 dict {ann_type: DataFrame}
        """
        if ann_type is not None:
            return self.fetch_history_by_type(ann_type, start, end)

        # 拉取所有类型
        result = {}
        for ann_type in ANNOUNCEMENT_TYPES:
            df = self.fetch_history_by_type(ann_type, start, end)
            if df is not None and not df.empty:
                result[ann_type] = df
        return result
